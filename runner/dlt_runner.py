"""Main orchestration for dlt pipeline execution."""

from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Any

import dlt

from models.callback_payload import CallbackPayload
from models.run_config import RunConfig
from runner.connection_builder import build_connection_string
from runner.destination_builder import build_postgres_destination
from runner.metrics_extractor import extract_metrics, extract_state
from runner.source_builder import (
    build_cdc_changes_source,
    build_cdc_snapshot_source,
    build_full_table_source,
    build_incremental_source,
)
from runner.transform_handler import compile_transform, make_add_map_fn
from core.connector_support import normalize_source_type

logger = logging.getLogger("etl.runner")

WRITE_DISPOSITION_MAP = {
    "merge": "merge",
    "append": "append",
    "overwrite": "replace",
}

active_runs: set[str] = set()


def _scrub_credentials(msg: str, creds: dict | None) -> str:
    """Remove credential values from error messages."""
    if not creds:
        return msg
    scrubbed = msg
    for key in ["password", "user", "host"]:
        if key in creds and creds.get(key):
            scrubbed = scrubbed.replace(str(creds[key]), "***")
    return re.sub(r"(://[^:/@\s]+:)([^@/\s]+)(@)", r"\1***\3", scrubbed)


def _apply_stream_configs(source: Any, stream_configs: dict) -> None:
    """Apply included_columns and dest_table per stream using apply_hints."""
    if not hasattr(source, "resources"):
        return
    for stream_name, cfg in stream_configs.items():
        if stream_name not in source.resources:
            continue
        resource = source.resources[stream_name]
        if cfg.get("included_columns"):
            resource.apply_hints(columns={c: {} for c in cfg["included_columns"]})
        if cfg.get("dest_table"):
            resource.apply_hints(table_name=cfg["dest_table"])


def _apply_transform(
    source: Any,
    transform_fn: Any,
    on_error: str,
    drop_counter: list,
) -> None:
    """Apply transform function to all resources in the source."""
    add_map_fn = make_add_map_fn(transform_fn, on_error, drop_counter)
    if hasattr(source, "resources"):
        for resource in source.resources.values():
            resource.add_map(add_map_fn)
            resource.add_filter(lambda item: item is not None)
    elif hasattr(source, "add_map"):
        source.add_map(add_map_fn)
        source.add_filter(lambda item: item is not None)


async def run(config: RunConfig) -> CallbackPayload:
    """Execute pipeline run and return CallbackPayload."""
    started_at = time.time()
    drop_counter = [0]
    source_creds: dict = {}

    try:
        if config.replication_method == "LOG_BASED" and config.connector_type != "postgres":
            raise ValueError(
                f"LOG_BASED CDC is only available for PostgreSQL. "
                f"Connector type '{config.connector_type}' does not support CDC."
            )

        source_creds = {
            "host": config.source_host,
            "port": config.source_port,
            "user": config.source_user,
            "password": config.source_password,
            "database": config.source_database,
            "ssl_mode": config.source_ssl_mode,
        }
        dest_creds = {
            "host": config.dest_host,
            "port": config.dest_port,
            "user": config.dest_user,
            "password": config.dest_password,
            "database": config.dest_database,
        }
        source_conn_str = build_connection_string(config.connector_type, source_creds, role="source")

        transform_fn = compile_transform(config.transform_script)

        destination = build_postgres_destination(dest_creds)

        pipeline = dlt.pipeline(
            pipeline_name=f"mxf_{config.pipeline_id[:8]}",
            destination=destination,
            dataset_name=config.dest_schema,
            dev_mode=False,
        )

        write_disposition = WRITE_DISPOSITION_MAP.get(config.emit_method, "merge")
        connector_normalized = normalize_source_type(config.connector_type)

        if config.replication_method == "FULL_TABLE":
            source = build_full_table_source(
                source_conn_str,
                config.selected_streams,
                config.source_schema,
                config.dlt_backend,
                config.chunk_size,
            )
            _apply_stream_configs(source, config.stream_configs)
            if transform_fn:
                _apply_transform(source, transform_fn, config.on_transform_error, drop_counter)
            pipeline.run(source, write_disposition="replace")

        elif config.replication_method == "INCREMENTAL":
            resources = []
            for table_name in config.selected_streams:
                stream_cfg = config.stream_configs.get(table_name, {})
                rep_key = stream_cfg.get("replication_key") or config.replication_key
                if not rep_key:
                    raise ValueError(
                        f"replication_key is required for INCREMENTAL sync. "
                        f"Set it for table '{table_name}' or as pipeline default."
                    )
                last_val = (config.last_state or {}).get(table_name) if config.last_state else None
                resource = build_incremental_source(
                    source_conn_str,
                    table_name,
                    rep_key,
                    last_val,
                    config.source_schema,
                    config.dlt_backend,
                    config.chunk_size,
                )
                if config.stream_configs.get(table_name, {}).get("included_columns"):
                    resource.apply_hints(
                        columns={c: {} for c in config.stream_configs[table_name]["included_columns"]}
                    )
                if transform_fn:
                    add_map_fn = make_add_map_fn(transform_fn, config.on_transform_error, drop_counter)
                    resource.add_map(add_map_fn)
                    resource.add_filter(lambda item: item is not None)
                resources.append(resource)
            pipeline.run(resources, write_disposition="merge")

        elif config.replication_method == "LOG_BASED":
            slot = config.replication_slot_name or f"mxf_{config.pipeline_id[:8]}"
            pub = config.replication_pub_name or f"mxf_pub_{config.pipeline_id[:8]}"
            initial_lsn = None
            if config.last_state and isinstance(config.last_state, dict):
                initial_lsn = config.last_state.get("last_commit_lsn")

            if config.last_state is None:
                snapshot = build_cdc_snapshot_source(
                    source_conn_str,
                    slot,
                    pub,
                    config.source_schema,
                    config.selected_streams,
                )
                if snapshot:
                    snap_list = snapshot if isinstance(snapshot, list) else [snapshot]
                    for snap in snap_list:
                        if transform_fn:
                            add_map_fn = make_add_map_fn(
                                transform_fn, config.on_transform_error, drop_counter
                            )
                            snap.add_map(add_map_fn)
                            snap.add_filter(lambda item: item is not None)
                        pipeline.run(snap, write_disposition="replace")

            changes = build_cdc_changes_source(source_conn_str, slot, pub, initial_lsn)
            if transform_fn:
                add_map_fn = make_add_map_fn(transform_fn, config.on_transform_error, drop_counter)
                changes.add_map(add_map_fn)
                changes.add_filter(lambda item: item is not None)
            pipeline.run(changes, write_disposition="merge")

        metrics = extract_metrics(pipeline, started_at, drop_counter)
        state = extract_state(pipeline, config.selected_streams)

        status = "partial_success" if drop_counter[0] > 0 else "success"
        return CallbackPayload(
            run_id=config.run_id,
            status=status,
            source_tool=f"dlt_sql_{connector_normalized}",
            dest_tool="dlt_dest_postgres",
            last_cursor_value=state,
            **metrics,
        )

    except SyntaxError as exc:
        return CallbackPayload(
            run_id=config.run_id,
            status="failed",
            source_tool=f"dlt_sql_{config.connector_type}",
            dest_tool="dlt_dest_postgres",
            duration_seconds=time.time() - started_at,
            error_message=str(exc),
        )
    except Exception as exc:
        err_msg = _scrub_credentials(str(exc), source_creds)
        return CallbackPayload(
            run_id=config.run_id,
            status="failed",
            source_tool=f"dlt_sql_{config.connector_type}",
            dest_tool="dlt_dest_postgres",
            duration_seconds=time.time() - started_at,
            error_message=err_msg,
        )
