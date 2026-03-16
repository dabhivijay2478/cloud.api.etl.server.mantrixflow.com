"""Sync — POST /sync. Accepts RunConfig or legacy SyncRequest, returns 202, posts callback when done."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from core.config import settings
from core.security import validate_etl_token
from models.run_config import RunConfig
from runner.dlt_runner import run, active_runs

logger = logging.getLogger("etl.sync")
router = APIRouter()


class LegacySyncRequest(BaseModel):
    """Legacy format from NestJS — converted to RunConfig."""

    job_id: str | None = None
    run_id: str | None = None
    pipeline_id: str
    organization_id: str
    org_id: str | None = None
    source_connection_config: dict = {}
    dest_connection_config: dict = {}
    source_type: str | None = None
    dest_type: str | None = None
    replication_method: str = "FULL_TABLE"
    source_stream: str | None = None
    selected_streams: list[str] | None = None
    dest_table: str | None = None
    dest_schema: str = "public"
    replication_slot_name: str | None = None
    replication_key: str | None = None
    transform_script: str | None = None
    emit_method: str = "append"
    nestjs_callback_url: str | None = None
    nestjs_state_url: str | None = None
    on_transform_error: str = "fail"
    dlt_backend: str = "sqlalchemy"


def _legacy_to_run_config(raw: dict) -> RunConfig:
    """Convert legacy sync payload to RunConfig."""
    run_id = raw.get("run_id") or raw.get("job_id", "")
    org_id = raw.get("org_id") or raw.get("organization_id", "")
    source = raw.get("source_connection_config", {})
    dest = raw.get("dest_connection_config", {})
    source_stream = raw.get("source_stream")
    selected = raw.get("selected_streams")
    if selected:
        streams = selected
    elif source_stream:
        streams = [source_stream.split("-")[-1] if "-" in source_stream else source_stream]
    else:
        streams = []
    stream_configs = dict(raw.get("stream_configs", {}))
    dest_table = raw.get("dest_table")
    if dest_table and streams:
        stream_configs[streams[0]] = {**stream_configs.get(streams[0], {}), "dest_table": dest_table}

    return RunConfig(
        run_id=run_id,
        pipeline_id=raw.get("pipeline_id", ""),
        org_id=org_id,
        connector_type=raw.get("source_type", "postgres"),
        replication_method=(raw.get("replication_method") or "FULL_TABLE").upper(),
        source_host=source.get("host", "localhost"),
        source_port=int(source.get("port", 5432)),
        source_user=source.get("username", source.get("user", "")),
        source_password=source.get("password", ""),
        source_database=source.get("database", source.get("dbname", "")),
        source_schema=source.get("schema", "public"),
        source_ssl_mode=str(source.get("ssl_mode", source.get("sslmode", "require"))),
        dest_host=dest.get("host", "localhost"),
        dest_port=int(dest.get("port", 5432)),
        dest_user=dest.get("username", dest.get("user", "")),
        dest_password=dest.get("password", ""),
        dest_database=dest.get("database", dest.get("dbname", "")),
        dest_schema=raw.get("dest_schema", "public"),
        dest_ssl_mode=str(dest.get("ssl_mode", dest.get("sslmode", "require"))),
        selected_streams=streams,
        stream_configs=stream_configs,
        replication_key=raw.get("replication_key"),
        last_state=raw.get("last_state"),
        replication_slot_name=raw.get("replication_slot_name"),
        replication_pub_name=raw.get("replication_pub_name"),
        emit_method="merge" if raw.get("emit_method") in ("merge", "upsert") else raw.get("emit_method", "append"),
        transform_script=raw.get("transform_script"),
        on_transform_error=raw.get("on_transform_error", "fail"),
        dlt_backend=raw.get("dlt_backend", "sqlalchemy"),
        callback_url=raw.get("nestjs_callback_url"),
    )

_sync_tasks: set = set()


async def drain_sync_tasks(timeout: float = 300) -> None:
    """Wait for in-flight sync tasks to complete on shutdown."""
    if not _sync_tasks:
        return
    done, pending = await asyncio.wait(_sync_tasks, timeout=timeout)
    if pending:
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


async def _run_and_callback(config: RunConfig) -> None:
    """Execute run and post callback to NestJS."""
    try:
        payload = await run(config)
        callback_url = config.callback_url or settings.CALLBACK_URL
        if not callback_url:
            logger.error("CALLBACK_URL not set — cannot post callback for run %s", config.run_id)
            return
        async with httpx.AsyncClient(timeout=30) as client:
            token = settings.CALLBACK_TOKEN or settings.ETL_INTERNAL_TOKEN
            headers = {"Content-Type": "application/json"}
            if token:
                headers["X-Callback-Token"] = token
                headers["x-internal-token"] = token
            resp = await client.post(
                callback_url,
                json=payload.model_dump(mode="json"),
                headers=headers,
            )
            if resp.status_code >= 400:
                logger.error(
                    "Callback failed for run %s: HTTP %d %s",
                    config.run_id,
                    resp.status_code,
                    resp.text[:500],
                )
    except Exception:
        logger.exception("Run failed for %s", config.run_id)
    finally:
        active_runs.discard(config.run_id)


@router.post("/sync", dependencies=[Depends(validate_etl_token)])
async def sync(body: dict):
    if len(active_runs) >= settings.MAX_CONCURRENT_RUNS:
        return JSONResponse(
            status_code=429,
            content={
                "error": "Pod at capacity",
                "active_runs": len(active_runs),
                "max_runs": settings.MAX_CONCURRENT_RUNS,
            },
        )

    if "run_id" in body and "selected_streams" in body:
        config = RunConfig.model_validate(body)
    else:
        config = _legacy_to_run_config(body)
    run_id = config.run_id

    active_runs.add(run_id)
    task = asyncio.create_task(_run_and_callback(config))
    _sync_tasks.add(task)
    task.add_done_callback(_sync_tasks.discard)

    logger.info(
        "Sync accepted: run_id=%s pipeline=%s streams=%s method=%s",
        run_id,
        config.pipeline_id,
        len(config.selected_streams),
        config.replication_method,
    )

    return JSONResponse(
        status_code=202,
        content={"run_id": run_id, "job_id": run_id, "status": "accepted"},
    )
