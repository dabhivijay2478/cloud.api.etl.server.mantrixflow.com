"""FastAPI ETL microservice backed by Singer taps and safe Python transforms."""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Dict, List, Literal, Optional

import httpx
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, model_validator

from etl_logger import activity, logger as etl_log
from orchestration.collect import run_collect_via_meltano
from orchestration.discovery import run_discovery
from orchestration.pipeline_runner import (
    get_job_for_direction,
    run_pipeline_job,
)
from orchestration.connection_mapper import connection_config_to_tap_config
from utils import (
    catalog_to_schemas,
    ensure_supported_source,
    extract_schema,
    merge_state,
    select_catalog_streams,
)

load_dotenv()

APP_NAME = "python-etl"
APP_VERSION = "2.0.0"
TAP_TIMEOUT_SECONDS = int(os.getenv("TAP_TIMEOUT_SECONDS", "1200"))
# When ETL runs in Docker, use this so container can reach API on host (e.g. host.docker.internal:5000)
ETL_CALLBACK_BASE_URL = os.getenv("ETL_CALLBACK_BASE_URL", "").strip()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _humanize_mysql_auth_error(error_text: str) -> Optional[str]:
    message = str(error_text or "")
    if "pymysql.err.OperationalError: (1045" not in message and "Access denied for user" not in message:
        return None

    # Common in local Docker setups where MySQL sees the caller as host-gateway.
    host_hint = ""
    if "@'192.168.65.1'" in message:
        host_hint = (
            " The MySQL server is seeing requests from host '192.168.65.1' "
            "(Docker host gateway). Ensure this user is allowed from that host (or '%')."
        )

    return (
        "MySQL authentication failed (error 1045). "
        "Verify username/password and host-based grants for this MySQL user."
        f"{host_hint} Avoid using root for pipelines; create a dedicated integration user."
    )


class DiscoverSchemaRequest(BaseModel):
    source_type: Optional[str] = None
    connection_config: Dict[str, Any]
    source_config: Dict[str, Any] = Field(default_factory=dict)
    table_name: Optional[str] = None
    schema_name: Optional[str] = None
    query: Optional[str] = None
    sync_mode: Literal["full", "incremental"] = "full"
    replication_key: Optional[str] = None


class DiscoverSchemaResponse(BaseModel):
    columns: List[Dict[str, Any]]
    primary_keys: List[str]
    estimated_row_count: Optional[int] = None
    schemas: Optional[List[Dict[str, Any]]] = None


class CollectRequest(BaseModel):
    source_type: Optional[str] = None
    connection_config: Dict[str, Any]
    source_config: Dict[str, Any] = Field(default_factory=dict)
    table_name: Optional[str] = None
    schema_name: Optional[str] = None
    query: Optional[str] = None
    sync_mode: Literal["full", "incremental"] = "full"
    checkpoint: Optional[Dict[str, Any]] = None
    limit: Optional[int] = None
    offset: int = 0
    cursor: Optional[str] = None
    replication_key: Optional[str] = None


class CollectResponse(BaseModel):
    rows: List[Dict[str, Any]]
    total_rows: int
    next_cursor: Optional[str] = None
    has_more: bool
    metadata: Dict[str, Any] = Field(default_factory=dict)
    checkpoint: Dict[str, Any] = Field(default_factory=dict)


class DeltaCheckRequest(BaseModel):
    connection_config: Dict[str, Any]
    source_config: Dict[str, Any] = Field(default_factory=dict)
    table_name: Optional[str] = None
    schema_name: Optional[str] = None
    checkpoint: Optional[Dict[str, Any]] = None
    query: Optional[str] = None
    replication_key: Optional[str] = None


class DeltaCheckResponse(BaseModel):
    has_changes: bool
    checkpoint: Dict[str, Any] = Field(default_factory=dict)


class TestConnectionRequest(BaseModel):
    type: str
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    connection_string: Optional[str] = None
    connection_string_mongo: Optional[str] = None
    ssl: Optional[Any] = None
    database_type: Optional[str] = Field(None, alias="databaseType")
    auth_source: Optional[str] = None
    replica_set: Optional[str] = None
    tls: Optional[bool] = None

    model_config = {"populate_by_name": True}


class RunMeltanoPipelineRequest(BaseModel):
    """Dynamic Meltano-style pipeline. Connections come from DB (passed by API at run time)."""
    model_config = {"populate_by_name": True, "extra": "ignore"}

    direction: Literal[
        "postgres-to-postgres",
        "postgres-to-mysql",
        "postgres-to-mongodb",
        "mysql-to-postgres",
        "mysql-to-mysql",
        "mysql-to-mongodb",
        "mongodb-to-postgres",
        "mongodb-to-mysql",
        "mongodb-to-mongodb",
    ]
    source_connection_config: Dict[str, Any]
    dest_connection_config: Dict[str, Any]
    source_table: Optional[str] = None
    source_schema: Optional[str] = "public"
    dest_table: Optional[str] = None
    dest_schema: Optional[str] = "public"
    column_renames: Optional[Dict[str, str]] = None  # { source_col: dest_col } for table-to-table mapping
    sync_mode: Literal["full", "incremental"] = "full"
    write_mode: Literal["append", "upsert", "replace"] = "upsert"
    upsert_key: Optional[List[str]] = None
    transform_script: Optional[str] = None
    dbt_models: Optional[List[str]] = None  # Selected dbt models; empty/None = run all
    checkpoint: Optional[Dict[str, Any]] = None
    limit: Optional[int] = None
    replication_key: Optional[str] = None
    state_id: Optional[str] = None
    # Async callback (when present, returns 202 and POSTs result when done)
    job_id: Optional[str] = None
    meltano_job_id: Optional[str] = None
    callback_url: Optional[str] = None
    callback_token: Optional[str] = None
    pgmq_msg_id: Optional[int] = None


class RunMeltanoPipelineResponse(BaseModel):
    rows_read: int
    rows_written: int
    rows_skipped: int
    rows_failed: int
    checkpoint: Dict[str, Any] = Field(default_factory=dict)
    errors: List[Dict[str, Any]] = Field(default_factory=list)


class TestConnectionResponse(BaseModel):
    success: bool
    message: Optional[str] = None
    error: Optional[str] = None


app = FastAPI(title=APP_NAME, version=APP_VERSION)

cors_origins = [item.strip() for item in os.getenv("CORS_ORIGINS", "").split(",") if item.strip()]
if not cors_origins:
    cors_origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Ensure unhandled exceptions return JSON with detail (HTTPException handled by FastAPI)."""
    if isinstance(exc, HTTPException):
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
    etl_log.error("Unhandled exception: %s", exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc), "type": type(exc).__name__},
    )


def _require_jwt(authorization: Optional[str] = Header(default=None)) -> str:
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header is required")
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Authorization must use Bearer token")
    token = authorization.replace("Bearer ", "", 1).strip()
    if not token:
        raise HTTPException(status_code=401, detail="Bearer token is empty")
    return token


@app.get("/")
async def root() -> Dict[str, str]:
    return {"service": APP_NAME, "version": APP_VERSION, "status": "ok"}


@app.get("/dbt-models")
async def list_dbt_models(_token: str = Depends(_require_jwt)) -> Dict[str, List[str]]:
    """List dbt model names from transform/models/*.sql for UI model selector."""
    models_dir = os.path.join(BASE_DIR, "transform", "models")
    models: List[str] = []
    if os.path.isdir(models_dir):
        for f in os.listdir(models_dir):
            if f.endswith(".sql"):
                models.append(f[:-4])
    return {"models": sorted(models)}


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "healthy"}


@app.post("/discover-schema/{source_type}", response_model=DiscoverSchemaResponse)
async def discover_schema(
    source_type: str,
    payload: DiscoverSchemaRequest,
    _token: str = Depends(_require_jwt),
) -> DiscoverSchemaResponse:
    """Schema discovery via Meltano invoke. No legacy fallback."""
    normalized_source = ensure_supported_source(source_type)
    activity("datasource.schema_discovered", f"Discovering schema for {normalized_source}", source_type=normalized_source, metadata={
        "table_name": payload.table_name, "schema_name": payload.schema_name,
    })

    source_config = {
        **payload.source_config,
        **({"table": payload.table_name, "table_name": payload.table_name} if payload.table_name else {}),
        **({"schema": payload.schema_name, "schema_name": payload.schema_name} if payload.schema_name else {}),
        **({"query": payload.query} if payload.query else {}),
    }

    try:
        catalog = await run_discovery(
            normalized_source,
            payload.connection_config,
            source_config=source_config if any(v is not None for v in source_config.values()) else None,
            timeout_seconds=TAP_TIMEOUT_SECONDS,
        )
    except RuntimeError as exc:
        msg = str(exc)
        mysql_auth_error = _humanize_mysql_auth_error(msg)
        if mysql_auth_error:
            activity("request.error", f"Schema discovery failed: {mysql_auth_error}", level="error", source_type=normalized_source)
            raise HTTPException(status_code=400, detail=mysql_auth_error) from exc
        activity("request.error", f"Meltano discovery failed: {msg}", level="error", source_type=normalized_source)
        raise HTTPException(status_code=502, detail=msg or "Schema discovery failed") from exc
    except Exception as exc:
        msg = str(exc) or "Schema discovery failed"
        activity("request.error", msg, level="error", source_type=normalized_source)
        raise HTTPException(status_code=502, detail=msg) from exc

    schema = extract_schema(catalog, table_name=payload.table_name, schema_name=payload.schema_name)
    activity("datasource.schema_discovered", f"Schema discovered: {len(schema['columns'])} columns", source_type=normalized_source, metadata={
        "column_count": len(schema["columns"]), "primary_keys": schema["primary_keys"],
    })
    return DiscoverSchemaResponse(
        columns=schema["columns"],
        primary_keys=schema["primary_keys"],
        estimated_row_count=schema.get("estimated_row_count"),
        schemas=catalog_to_schemas(catalog),
    )


@app.post("/collect/{source_type}", response_model=CollectResponse)
async def collect(
    source_type: str,
    payload: CollectRequest,
    _token: str = Depends(_require_jwt),
) -> CollectResponse:
    """Collect data via Meltano invoke. No legacy fallback."""
    normalized_source = ensure_supported_source(source_type)
    activity("sync.collect", f"Collecting from {normalized_source}", source_type=normalized_source, metadata={
        "sync_mode": payload.sync_mode, "table_name": payload.table_name, "limit": payload.limit, "offset": payload.offset,
    })
    tap_config = connection_config_to_tap_config(
        normalized_source,
        payload.connection_config,
        {
            **payload.source_config,
            **({"table": payload.table_name} if payload.table_name else {}),
            **({"schema": payload.schema_name} if payload.schema_name else {}),
            **({"query": payload.query} if payload.query else {}),
        },
    )

    try:
        discovery_catalog = await run_discovery(
            normalized_source,
            payload.connection_config,
            source_config={
                "table": payload.table_name,
                "schema": payload.schema_name,
            } if payload.table_name or payload.schema_name else None,
            timeout_seconds=TAP_TIMEOUT_SECONDS,
        )
        selected_catalog, _ = select_catalog_streams(
            normalized_source,
            discovery_catalog,
            table_name=payload.table_name,
            schema_name=payload.schema_name,
            sync_mode=payload.sync_mode,
            replication_key=payload.replication_key,
        )
        collect_result = await run_collect_via_meltano(
            normalized_source,
            payload.connection_config,
            selected_catalog,
            state=payload.checkpoint,
            tap_config=tap_config,
            timeout_seconds=TAP_TIMEOUT_SECONDS,
        )
    except RuntimeError as exc:
        msg = str(exc)
        mysql_auth_error = _humanize_mysql_auth_error(msg)
        if mysql_auth_error:
            activity("request.error", f"Collect failed: {mysql_auth_error}", level="error", source_type=normalized_source)
            raise HTTPException(status_code=400, detail=mysql_auth_error) from exc
        activity("request.error", f"Meltano collect failed: {msg}", level="error", source_type=normalized_source)
        raise HTTPException(status_code=502, detail=msg or "Collect failed") from exc
    except ValueError as exc:
        activity("request.error", f"Collect validation error: {exc}", level="warn", source_type=normalized_source)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        msg = str(exc)
        activity("request.error", f"Unexpected collect error for {normalized_source}: {msg[:500]}", level="error", source_type=normalized_source)
        raise HTTPException(status_code=502, detail=msg or "Collect failed") from exc

    all_records = collect_result["records"]
    total_rows = len(all_records)
    start = max(payload.offset or 0, 0)
    end = total_rows if payload.limit is None else min(start + max(payload.limit, 0), total_rows)
    page_records = all_records[start:end]
    has_more = end < total_rows

    new_state = merge_state(payload.checkpoint, collect_result["state"])

    activity("sync.collect", f"Collected {total_rows} rows from {normalized_source}, returning {len(page_records)}", source_type=normalized_source, metadata={
        "total_rows": total_rows, "page_rows": len(page_records), "has_more": has_more,
    })
    return CollectResponse(
        rows=page_records,
        total_rows=total_rows,
        next_cursor=str(end) if has_more else None,
        has_more=has_more,
        metadata={
            "mode": payload.sync_mode,
            "selected_count": len(page_records),
            "total_collected": total_rows,
        },
        checkpoint=new_state,
    )


async def _run_and_callback(payload: RunMeltanoPipelineRequest) -> None:
    """Background: run meltano, then POST to callback_url."""
    direction = payload.direction
    _DIRECTION_MAP = {
        "postgres-to-postgres": ("postgresql", "postgresql"),
        "postgres-to-mysql": ("postgresql", "mysql"),
        "postgres-to-mongodb": ("postgresql", "mongodb"),
        "mysql-to-postgres": ("mysql", "postgresql"),
        "mysql-to-mysql": ("mysql", "mysql"),
        "mysql-to-mongodb": ("mysql", "mongodb"),
        "mongodb-to-postgres": ("mongodb", "postgresql"),
        "mongodb-to-mysql": ("mongodb", "mysql"),
        "mongodb-to-mongodb": ("mongodb", "mongodb"),
    }
    source_type, dest_type = _DIRECTION_MAP[direction]
    job_name = get_job_for_direction(source_type, dest_type)
    job_id = payload.job_id or payload.meltano_job_id or "unknown"
    meltano_job_id = payload.meltano_job_id or f"mantrix-{job_id}-{id(payload)}"
    callback_url = payload.callback_url or ""
    callback_token = payload.callback_token or ""
    pgmq_msg_id = payload.pgmq_msg_id or 0

    try:
        result = await run_pipeline_job(
            job_name,
            source_type,
            payload.source_connection_config,
            dest_type,
            payload.dest_connection_config,
            state_id=payload.state_id,
            checkpoint=payload.checkpoint,
            sync_mode=payload.sync_mode,
            dbt_models=payload.dbt_models,
            source_table=payload.source_table,
            source_schema=payload.source_schema,
            dest_table=payload.dest_table,
            column_renames=payload.column_renames,
            timeout_seconds=TAP_TIMEOUT_SECONDS,
            meltano_job_id=meltano_job_id,
        )
        status = "completed" if result.success else "failed"
        rows_synced = result.rows_written
        error_message = None if result.success else (result.user_message or (result.errors[0]["error"] if result.errors else "Pipeline failed"))
        user_message = result.user_message if not result.success else None
    except Exception as exc:
        status = "failed"
        rows_synced = 0
        error_message = str(exc)
        user_message = getattr(exc, "user_message", None) or error_message

    if callback_url:
        # ETL_CALLBACK_BASE_URL overrides when ETL runs in Docker (cannot reach localhost from API)
        effective_url = (
            f"{ETL_CALLBACK_BASE_URL.rstrip('/')}/api/internal/etl-callback"
            if ETL_CALLBACK_BASE_URL
            else callback_url
        )
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    effective_url,
                    json={
                        "jobId": job_id,
                        "pgmqMsgId": pgmq_msg_id,
                        "status": status,
                        "rowsSynced": rows_synced,
                        "stateId": payload.state_id,
                        "errorMessage": error_message,
                        "userMessage": user_message,
                    },
                    headers={"X-Internal-Token": callback_token, "Content-Type": "application/json"},
                )
                resp.raise_for_status()
        except Exception as cb_exc:
            etl_log.error(
                "Callback POST failed: %s (url=%s)",
                cb_exc,
                effective_url,
                exc_info=True,
            )


@app.post("/run-meltano-pipeline")
async def run_meltano_pipeline(
    payload: RunMeltanoPipelineRequest,
    _token: str = Depends(_require_jwt),
):
    """
    Run a Meltano pipeline with dynamic connections.
    When callback_url is present: returns 202 immediately, runs in background, POSTs callback when done.
    Otherwise: runs synchronously and returns result.
    """
    direction = payload.direction
    _DIRECTION_MAP = {
        "postgres-to-postgres": ("postgresql", "postgresql"),
        "postgres-to-mysql": ("postgresql", "mysql"),
        "postgres-to-mongodb": ("postgresql", "mongodb"),
        "mysql-to-postgres": ("mysql", "postgresql"),
        "mysql-to-mysql": ("mysql", "mysql"),
        "mysql-to-mongodb": ("mysql", "mongodb"),
        "mongodb-to-postgres": ("mongodb", "postgresql"),
        "mongodb-to-mysql": ("mongodb", "mysql"),
        "mongodb-to-mongodb": ("mongodb", "mongodb"),
    }
    if direction not in _DIRECTION_MAP:
        raise HTTPException(status_code=400, detail=f"Unsupported direction: {direction}")
    source_type, dest_type = _DIRECTION_MAP[direction]

    job_name = get_job_for_direction(source_type, dest_type)
    if job_name is None:
        raise HTTPException(
            status_code=400,
            detail=f"No Meltano job for direction {direction}. Add a job in meltano.yml for {source_type}→{dest_type}.",
        )
    if payload.transform_script is not None:
        raise HTTPException(
            status_code=400,
            detail="transform_script is not supported in Clean Engine. Use dbt in the Meltano job.",
        )

    activity(
        "sync.meltano_pipeline",
        f"Running {direction}: {source_type} -> {dest_type}",
        source_type=source_type,
        metadata={"job": job_name},
    )

    if payload.callback_url:
        asyncio.create_task(_run_and_callback(payload))
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=202,
            content={"accepted": True, "jobId": payload.job_id or payload.meltano_job_id or "queued"},
        )

    try:
        result = await run_pipeline_job(
            job_name,
            source_type,
            payload.source_connection_config,
            dest_type,
            payload.dest_connection_config,
            state_id=payload.state_id,
            checkpoint=payload.checkpoint,
            sync_mode=payload.sync_mode,
            dbt_models=payload.dbt_models,
            source_table=payload.source_table,
            source_schema=payload.source_schema,
            dest_table=payload.dest_table,
            column_renames=payload.column_renames,
            timeout_seconds=TAP_TIMEOUT_SECONDS,
            meltano_job_id=payload.meltano_job_id,
        )
    except Exception as exc:
        msg = str(exc)
        activity("request.error", f"Meltano pipeline failed: {msg}", level="error", source_type=source_type)
        raise HTTPException(status_code=502, detail=msg or "Pipeline failed") from exc

    if not result.success:
        detail = result.user_message or (result.errors[0]["error"] if result.errors else "Pipeline failed")
        raise HTTPException(status_code=502, detail=detail)

    activity(
        "sync.meltano_pipeline",
        f"Pipeline complete: {result.rows_read} read, {result.rows_written} written",
        source_type=source_type,
        metadata={"rows_read": result.rows_read, "rows_written": result.rows_written},
    )

    return RunMeltanoPipelineResponse(
        rows_read=result.rows_read,
        rows_written=result.rows_written,
        rows_skipped=result.rows_skipped,
        rows_failed=result.rows_failed,
        checkpoint=result.checkpoint,
        errors=result.errors,
    )


@app.post("/delta-check/{source_type}", response_model=DeltaCheckResponse)
async def delta_check(
    source_type: str,
    payload: DeltaCheckRequest,
    token: str = Depends(_require_jwt),
) -> DeltaCheckResponse:
    collect_payload = CollectRequest(
        source_type=source_type,
        connection_config=payload.connection_config,
        source_config=payload.source_config,
        table_name=payload.table_name,
        schema_name=payload.schema_name,
        query=payload.query,
        sync_mode="incremental",
        checkpoint=payload.checkpoint,
        limit=1,
        offset=0,
        replication_key=payload.replication_key,
    )
    result = await collect(source_type, collect_payload, token)
    return DeltaCheckResponse(has_changes=len(result.rows) > 0, checkpoint=result.checkpoint)


@app.post("/test-connection", response_model=TestConnectionResponse)
async def test_connection(
    payload: TestConnectionRequest,
    _token: str = Depends(_require_jwt),
) -> TestConnectionResponse:
    """Test connection via Meltano discovery. No direct DB access."""
    source_type = ensure_supported_source(payload.type)
    activity("datasource.connection_tested", f"Testing connection for {source_type}", source_type=source_type)
    config = payload.model_dump(exclude_none=True)
    connection_config = {k: v for k, v in config.items() if k != "type"}

    test_timeout = int(os.getenv("ETL_TEST_CONNECTION_TIMEOUT_SEC", "60"))
    try:
        await run_discovery(
            source_type,
            connection_config,
            timeout_seconds=test_timeout,
        )
        return TestConnectionResponse(success=True, message="Connection successful")
    except Exception as exc:
        return TestConnectionResponse(success=False, error=str(exc))
