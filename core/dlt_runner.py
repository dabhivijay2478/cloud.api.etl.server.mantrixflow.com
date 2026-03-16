"""dlt-based execution engine for preview and sync jobs."""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import dlt
import httpx
from dlt.destinations import mssql, postgres, sqlalchemy
from dlt.sources.sql_database import sql_table

from core.connection_utils import (
    DEFAULT_SQL_SCHEMA,
    build_sqlalchemy_url,
    normalize_connector_type,
    parse_stream_name,
)
from core.transform_utils import (
    TransformCounters,
    apply_record_transform,
    compile_transform_fn,
    count_data_item_rows,
    has_record_level_transform,
)
from pg_replication import init_replication, replication_resource

logger = logging.getLogger("etl.runner")

TMPFS_ROOT = Path(tempfile.gettempdir())
TMPFS_PREFIX = "mxf_"
DEFAULT_DATASET_NAME = "mantrixflow"


def _checkpoint_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


class DLTRunResult:
    """Result of one dlt sync execution."""

    def __init__(self) -> None:
        self.status = "completed"
        self.rows_read = 0
        self.rows_upserted = 0
        self.rows_dropped = 0
        self.rows_deleted = 0
        self.error: str | None = None
        self.duration_seconds = 0.0
        self.checkpoint: dict[str, Any] | None = None
        self.lsn_end: int | None = None


def cleanup_stale_tmpfs() -> None:
    for entry in TMPFS_ROOT.iterdir():
        if entry.is_dir() and entry.name.startswith(TMPFS_PREFIX):
            shutil.rmtree(entry, ignore_errors=True)


async def fetch_pipeline_state(
    nestjs_state_url: str,
    pipeline_id: str,
) -> dict[str, Any] | None:
    url = f"{nestjs_state_url.rstrip('/')}/{pipeline_id}"
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            response = await client.get(url)
            if response.status_code != 200:
                return None
            data = response.json()
            return data.get("checkpoint")
    except Exception as exc:
        logger.warning("Failed to fetch checkpoint for %s: %s", pipeline_id, exc)
        return None


async def post_callback(callback_url: str, payload: dict[str, Any]) -> None:
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            response = await client.post(callback_url, json=payload)
            if response.status_code >= 400:
                logger.error(
                    "Callback POST failed (HTTP %d): %s",
                    response.status_code,
                    response.text[:500],
                )
    except Exception as exc:
        logger.error("Callback POST exception: %s", exc)


def _write_disposition(emit_method: str | None) -> str:
    normalized = (emit_method or "append").strip().lower()
    if normalized in {"merge", "upsert"}:
        return "merge"
    if normalized in {"overwrite", "replace"}:
        return "replace"
    return "append"


def _pipeline_name(job_id: str) -> str:
    return f"mxf_{job_id.replace('-', '_')}"


def _source_tool_name(source_type: str, replication_method: str) -> str:
    normalized = normalize_connector_type(source_type)
    if normalized == "postgres" and replication_method == "LOG_BASED":
        return "dlt_sql_postgres_cdc"
    return f"dlt_sql_{normalized}"


def _dest_tool_name(dest_type: str) -> str:
    normalized = normalize_connector_type(dest_type, role="dest")
    return f"dlt_dest_{normalized}"


def _extract_incremental_start(checkpoint: dict[str, Any] | None) -> Any:
    if not checkpoint:
        return None
    if checkpoint.get("engine") != "dlt":
        return None
    if "last_value" in checkpoint:
        return checkpoint.get("last_value")
    if "lastSyncValue" in checkpoint:
        return checkpoint.get("lastSyncValue")
    return None


def _extract_lsn_start(checkpoint: dict[str, Any] | None) -> int | None:
    if not checkpoint:
        return None
    if checkpoint.get("engine") != "dlt":
        return None
    value = checkpoint.get("last_commit_lsn", checkpoint.get("lsnEnd"))
    return int(value) if value not in (None, "") else None


def _extract_state_value(
    pipeline_state: dict[str, Any],
    pipeline_name: str,
    resource_name: str,
    key_path: list[str],
) -> Any:
    cursor: Any = pipeline_state.get("sources", {}).get(pipeline_name, {}).get("resources", {}).get(resource_name, {})
    for key in key_path:
        if not isinstance(cursor, dict):
            return None
        cursor = cursor.get(key)
    return cursor


def _publication_name(slot_name: str) -> str:
    base = f"{slot_name}_pub"
    return base[:63]


def _build_destination(dest_type: str, config: dict[str, Any]):
    normalized = normalize_connector_type(dest_type, role="dest")

    if normalized == "postgres":
        return postgres(credentials=build_sqlalchemy_url(normalized, config, role="dest"))
    if normalized == "mssql":
        return mssql(credentials=build_sqlalchemy_url(normalized, config, role="dest"))
    # Generic SQLAlchemy destination covers MySQL, MariaDB, SQLite, Oracle, and CockroachDB.
    return sqlalchemy(credentials=build_sqlalchemy_url(normalized, config, role="dest"))


def _counting_map(counters: TransformCounters):
    def _map(item: Any) -> Any:
        rows = count_data_item_rows(item)
        counters.rows_read += rows
        counters.rows_written += rows
        return item

    return _map


def _record_transform_map(
    counters: TransformCounters,
    *,
    column_map: dict[str, str] | None,
    drop_columns: list[str] | None,
    transform_script: str | None,
    on_transform_error: str,
):
    transform_fn = compile_transform_fn(transform_script)
    drop_column_set = set(drop_columns or [])

    def _map(item: Any) -> Any:
        if isinstance(item, list):
            transformed_items: list[dict[str, Any]] = []
            for row in item:
                if not isinstance(row, dict):
                    continue
                counters.rows_read += 1
                try:
                    transformed = apply_record_transform(
                        row,
                        column_map=column_map,
                        drop_columns=drop_column_set,
                        transform_fn=transform_fn,
                        on_error=on_transform_error,
                    )
                except Exception:
                    counters.transform_errors += 1
                    raise
                if transformed is None:
                    counters.rows_dropped += 1
                    continue
                counters.rows_written += 1
                transformed_items.append(transformed)
            return transformed_items

        if not isinstance(item, dict):
            raise ValueError("Unsupported data item type for record-level transform")

        counters.rows_read += 1
        try:
            transformed = apply_record_transform(
                item,
                column_map=column_map,
                drop_columns=drop_column_set,
                transform_fn=transform_fn,
                on_error=on_transform_error,
            )
        except Exception:
            counters.transform_errors += 1
            raise

        if transformed is None:
            counters.rows_dropped += 1
            return None

        counters.rows_written += 1
        return transformed

    return _map


def _build_sql_resource(
    *,
    source_connection_config: dict[str, Any],
    source_type: str,
    source_stream: str,
    replication_method: str,
    replication_key: str | None,
    initial_value: Any,
    emit_method: str,
    upsert_key: list[str] | None,
    dest_table: str | None,
    preview_limit: int | None = None,
    backend: str = "sqlalchemy",
):
    default_schema = str(source_connection_config.get("schema") or DEFAULT_SQL_SCHEMA)
    stream = parse_stream_name(source_stream, default_namespace=default_schema)
    query_adapter_callback = None

    if preview_limit is not None:
        def _limit_query(query, _table, *_args):
            return query.limit(preview_limit)

        query_adapter_callback = _limit_query

    incremental = None
    if replication_method == "INCREMENTAL":
        if not replication_key:
            raise ValueError("replication_key is required when replication_method is INCREMENTAL")
        incremental = dlt.sources.incremental(replication_key, initial_value=initial_value)

    resource = sql_table(
        credentials=build_sqlalchemy_url(source_type, source_connection_config),
        table=stream.name,
        schema=stream.namespace,
        incremental=incremental,
        backend=backend,
        query_adapter_callback=query_adapter_callback,
        write_disposition=_write_disposition(emit_method),
        primary_key=upsert_key or None,
    )

    if dest_table and dest_table != stream.name:
        resource.apply_hints(table_name=dest_table)

    return resource, stream.name, stream


def _build_pg_replication_resource(
    *,
    source_connection_config: dict[str, Any],
    source_stream: str,
    slot_name: str,
    dest_table: str | None,
    initial_lsn: int | None,
):
    stream = parse_stream_name(
        source_stream,
        default_namespace=str(source_connection_config.get("schema") or DEFAULT_SQL_SCHEMA),
    )
    publication_name = str(source_connection_config.get("publication_name") or _publication_name(slot_name))
    credentials = build_sqlalchemy_url("postgres", source_connection_config)

    init_replication(
        slot_name=slot_name,
        pub_name=publication_name,
        schema_name=stream.namespace or DEFAULT_SQL_SCHEMA,
        table_names=stream.name,
        credentials=credentials,
        reset=False,
    )

    table_name_map = {stream.name: dest_table} if dest_table and dest_table != stream.name else None
    resource = replication_resource(
        slot_name=slot_name,
        pub_name=publication_name,
        credentials=credentials,
        initial_last_commit_lsn=initial_lsn,
        table_name_map=table_name_map,
    )
    return resource, slot_name, publication_name


async def run_preview(
    source_connection_config: dict[str, Any],
    source_stream: str,
    *,
    limit: int = 50,
    column_map: dict[str, str] | None = None,
    drop_columns: list[str] | None = None,
    transform_script: str | None = None,
    source_type: str = "postgres",
    on_transform_error: str = "fail",
) -> dict[str, Any]:
    counters = TransformCounters()
    normalized_source_type = normalize_connector_type(source_type)

    resource, _, _ = _build_sql_resource(
        source_connection_config=source_connection_config,
        source_type=normalized_source_type,
        source_stream=source_stream,
        replication_method="FULL_TABLE",
        replication_key=None,
        initial_value=None,
        emit_method="append",
        upsert_key=None,
        dest_table=None,
        preview_limit=limit,
        backend="sqlalchemy",
    )

    mapper = _record_transform_map(
        counters,
        column_map=column_map,
        drop_columns=drop_columns,
        transform_script=transform_script,
        on_transform_error=on_transform_error,
    )
    resource.add_map(mapper)
    resource.add_filter(lambda item: item is not None)

    records: list[dict[str, Any]] = []
    columns: set[str] = set()
    for item in resource:
        if not isinstance(item, dict):
            continue
        records.append(item)
        columns.update(item.keys())
        if len(records) >= limit:
            break

    return {
        "records": records,
        "columns": sorted(columns),
        "total": len(records),
        "stream": source_stream,
    }


async def run_sync(
    *,
    job_id: str,
    pipeline_id: str,
    organization_id: str,
    source_connection_config: dict[str, Any],
    dest_connection_config: dict[str, Any],
    replication_method: str,
    source_stream: str,
    dest_table: str,
    dest_schema: str = DEFAULT_SQL_SCHEMA,
    replication_slot_name: str | None = None,
    replication_key: str | None = None,
    source_type: str = "postgres",
    dest_type: str = "postgres",
    column_map: dict[str, str] | None = None,
    drop_columns: list[str] | None = None,
    transform_script: str | None = None,
    emit_method: str = "append",
    upsert_key: list[str] | None = None,
    nestjs_callback_url: str,
    nestjs_state_url: str,
    on_transform_error: str = "fail",
    dlt_backend: str = "sqlalchemy",
    **_: Any,
) -> DLTRunResult:
    result = DLTRunResult()
    start = time.monotonic()
    work_dir = TMPFS_ROOT / f"{TMPFS_PREFIX}{job_id}"
    normalized_source_type = normalize_connector_type(source_type)
    normalized_dest_type = normalize_connector_type(dest_type, role="dest")
    counters = TransformCounters()
    checkpoint = await fetch_pipeline_state(nestjs_state_url, pipeline_id)

    try:
        work_dir.mkdir(parents=True, exist_ok=True)
        destination = _build_destination(normalized_dest_type, dest_connection_config)
        record_level_transform = has_record_level_transform(
            column_map=column_map,
            drop_columns=drop_columns,
            transform_script=transform_script,
        )
        backend = "sqlalchemy" if record_level_transform else (dlt_backend or "sqlalchemy")

        if normalized_source_type == "postgres" and replication_method == "LOG_BASED":
            if not replication_slot_name:
                raise ValueError("replication_slot_name is required for LOG_BASED PostgreSQL syncs")
            resource, resource_name, publication_name = _build_pg_replication_resource(
                source_connection_config=source_connection_config,
                source_stream=source_stream,
                slot_name=replication_slot_name,
                dest_table=dest_table,
                initial_lsn=_extract_lsn_start(checkpoint),
            )
        else:
            resource, resource_name, _ = _build_sql_resource(
                source_connection_config=source_connection_config,
                source_type=normalized_source_type,
                source_stream=source_stream,
                replication_method=replication_method,
                replication_key=replication_key,
                initial_value=_extract_incremental_start(checkpoint),
                emit_method=emit_method,
                upsert_key=upsert_key,
                dest_table=dest_table,
                backend=backend,
            )

        if record_level_transform:
            resource.add_map(
                _record_transform_map(
                    counters,
                    column_map=column_map,
                    drop_columns=drop_columns,
                    transform_script=transform_script,
                    on_transform_error=on_transform_error,
                )
            )
            resource.add_filter(lambda item: item is not None)
        else:
            resource.add_map(_counting_map(counters))

        pipeline_name = _pipeline_name(job_id)
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            pipelines_dir=str(work_dir),
            destination=destination,
            dataset_name=dest_schema or DEFAULT_DATASET_NAME,
            progress="log",
        )
        pipeline.run(resource)

        result.rows_read = counters.rows_read
        result.rows_upserted = counters.rows_written
        result.rows_dropped = counters.rows_dropped

        if replication_method == "INCREMENTAL" and replication_key:
            last_value = _extract_state_value(
                pipeline.state,
                pipeline_name,
                resource_name,
                ["incremental", replication_key, "last_value"],
            )
            result.checkpoint = {
                "engine": "dlt",
                "version": 1,
                "replication_method": "INCREMENTAL",
                "resource": resource_name,
                "cursor_path": replication_key,
                "last_value": last_value,
                "watermarkField": replication_key,
                "lastSyncValue": last_value,
                "lastSyncAt": _checkpoint_timestamp(),
                "rowsProcessed": result.rows_upserted,
            }
        elif replication_method == "LOG_BASED" and normalized_source_type == "postgres":
            last_commit_lsn = _extract_state_value(
                pipeline.state,
                pipeline_name,
                resource_name,
                ["last_commit_lsn"],
            )
            result.lsn_end = int(last_commit_lsn) if last_commit_lsn not in (None, "") else None
            result.checkpoint = {
                "engine": "dlt",
                "version": 1,
                "replication_method": "LOG_BASED",
                "resource": resource_name,
                "slot_name": replication_slot_name,
                "publication_name": publication_name,
                "last_commit_lsn": result.lsn_end,
                "slotName": replication_slot_name,
                "publicationName": publication_name,
                "lsn": result.lsn_end,
                "lsnEnd": result.lsn_end,
                "lastSyncValue": result.lsn_end,
                "lastSyncAt": _checkpoint_timestamp(),
                "rowsProcessed": result.rows_upserted,
            }
        else:
            result.checkpoint = {
                "engine": "dlt",
                "version": 1,
                "replication_method": "FULL_TABLE",
                "resource": resource_name,
                "lastSyncAt": _checkpoint_timestamp(),
                "rowsProcessed": result.rows_upserted,
            }

        result.status = "completed"
    except asyncio.CancelledError:
        result.status = "interrupted"
        result.error = "Run was cancelled"
    except Exception as exc:
        result.status = "failed"
        result.error = str(exc)
        logger.exception("dlt run failed for job %s", job_id)
        result.rows_upserted = 0
    finally:
        result.duration_seconds = round(time.monotonic() - start, 2)
        await post_callback(
            nestjs_callback_url,
            {
                "job_id": job_id,
                "pipeline_id": pipeline_id,
                "organization_id": organization_id,
                "status": result.status,
                "rows_read": result.rows_read,
                "rows_upserted": result.rows_upserted,
                "rows_dropped": result.rows_dropped,
                "rows_deleted": result.rows_deleted,
                "lsn_end": result.lsn_end,
                "checkpoint": result.checkpoint,
                "error": result.error,
                "duration_seconds": result.duration_seconds,
                "source_tool": _source_tool_name(normalized_source_type, replication_method),
                "dest_tool": _dest_tool_name(normalized_dest_type),
                "replication_method_used": replication_method,
            },
        )
        shutil.rmtree(work_dir, ignore_errors=True)

    return result


_active_tasks: set[asyncio.Task[Any]] = set()
_shutting_down = False


def register_task(task: asyncio.Task[Any]) -> None:
    _active_tasks.add(task)
    task.add_done_callback(_active_tasks.discard)


def is_shutting_down() -> bool:
    return _shutting_down


async def drain_running_pipelines(timeout: float = 300) -> None:
    global _shutting_down
    _shutting_down = True
    if not _active_tasks:
        return
    done, pending = await asyncio.wait(_active_tasks, timeout=timeout)
    if pending:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
