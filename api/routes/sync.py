"""Sync — POST /sync."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from core.concurrency import run_semaphore, source_limiter
from core.connector_support import normalize_dest_type, normalize_source_type
from core.dlt_runner import run_sync, register_task, is_shutting_down
from core.source_mutation_policy import (
    SOURCE_DB_MUTATION_POLICY_MESSAGE,
    are_source_db_mutations_allowed,
)

logger = logging.getLogger("etl.sync")
router = APIRouter()

DEFAULT_PORTS = {
    "postgres": 5432,
    "mysql": 3306,
    "mariadb": 3306,
    "mssql": 1433,
    "oracle": 1521,
    "cockroachdb": 26257,
    "sqlite": 0,
}


class SyncRequest(BaseModel):
    job_id: str
    pipeline_id: str
    organization_id: str
    source_connection_config: dict[str, Any]
    dest_connection_config: dict[str, Any]
    source_type: str | None = None
    dest_type: str | None = None
    replication_method: str  # FULL_TABLE, LOG_BASED, or INCREMENTAL
    source_stream: str
    dest_table: str
    dest_schema: str = "public"
    replication_slot_name: str | None = None
    replication_key: str | None = None  # Required when replication_method is INCREMENTAL
    column_map: dict[str, str] | None = None
    drop_columns: list[str] | None = None
    transform_script: str | None = None
    output_column_sql_types: dict[str, str] | None = None
    transform_type: str | None = None
    emit_method: str = "append"
    upsert_key: list[str] | None = None
    hard_delete: bool = False
    nestjs_callback_url: str
    nestjs_state_url: str
    discovered_catalog: dict[str, Any] | None = None
    on_transform_error: str = "fail"
    dlt_backend: str = "sqlalchemy"


@router.post("/sync")
async def sync(body: SyncRequest):
    try:
        source_type = normalize_source_type(body.source_type)
        dest_type = normalize_dest_type(body.dest_type)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    replication_method_upper = body.replication_method.upper()
    if replication_method_upper not in ("FULL_TABLE", "LOG_BASED", "INCREMENTAL"):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid replication_method: {body.replication_method}. Must be FULL_TABLE, LOG_BASED, or INCREMENTAL.",
        )

    if replication_method_upper == "INCREMENTAL" and not body.replication_key:
        raise HTTPException(
            status_code=400,
            detail="replication_key is required when replication_method is INCREMENTAL.",
        )
    if replication_method_upper == "LOG_BASED" and not are_source_db_mutations_allowed():
        raise HTTPException(status_code=400, detail=SOURCE_DB_MUTATION_POLICY_MESSAGE)

    if is_shutting_down():
        return JSONResponse(
            status_code=503,
            content={"error": "Pod is shutting down, not accepting new syncs"},
        )

    # Check global concurrency
    acquired = await run_semaphore.acquire()
    if not acquired:
        return JSONResponse(
            status_code=503,
            content={
                "error": "Pod at capacity",
                "active_runs": run_semaphore.active,
                "max_runs": run_semaphore.max_runs,
            },
        )

    # Check per-source rate limit
    source_host = (
        body.source_connection_config.get("host")
        or body.source_connection_config.get("database")
        or body.source_connection_config.get("path")
        or "unknown"
    )
    source_port = int(
        body.source_connection_config.get("port")
        or DEFAULT_PORTS.get(source_type, 5432)
    )
    source_acquired = await source_limiter.acquire(source_host, source_port)
    if not source_acquired:
        await run_semaphore.release()
        return JSONResponse(
            status_code=503,
            content={
                "error": f"Source {source_host}:{source_port} at tap limit",
            },
        )

    logger.info(
        "Sync accepted: job=%s pipeline=%s stream=%s method=%s dest=%s.%s",
        body.job_id, body.pipeline_id, body.source_stream,
        body.replication_method, body.dest_schema, body.dest_table,
    )

    task = asyncio.create_task(
        _run_and_release(body, source_host, source_port, source_type, dest_type)
    )
    register_task(task)

    return {"job_id": body.job_id, "status": "accepted"}


async def _run_and_release(
    body: SyncRequest, source_host: str, source_port: int,
    source_type: str = "postgres", dest_type: str = "postgres",
) -> None:
    """Run the sync then release semaphore and source limiter."""
    try:
        await run_sync(
            job_id=body.job_id,
            pipeline_id=body.pipeline_id,
            organization_id=body.organization_id,
            source_connection_config=body.source_connection_config,
            dest_connection_config=body.dest_connection_config,
            source_type=source_type,
            dest_type=dest_type,
            replication_method=body.replication_method.upper(),
            source_stream=body.source_stream,
            dest_table=body.dest_table,
            dest_schema=body.dest_schema,
            replication_slot_name=body.replication_slot_name,
            replication_key=body.replication_key,
            column_map=body.column_map,
            drop_columns=body.drop_columns,
            transform_script=body.transform_script,
            output_column_sql_types=body.output_column_sql_types,
            emit_method=body.emit_method,
            upsert_key=body.upsert_key,
            hard_delete=body.hard_delete,
            nestjs_callback_url=body.nestjs_callback_url,
            nestjs_state_url=body.nestjs_state_url,
            on_transform_error=body.on_transform_error,
            dlt_backend=body.dlt_backend,
        )
    except Exception:
        logger.exception("Sync task failed for job %s", body.job_id)
    finally:
        await source_limiter.release(source_host, source_port)
        await run_semaphore.release()
