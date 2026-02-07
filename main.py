"""FastAPI ETL microservice backed by Singer taps and safe Python transforms."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid
from typing import Any, Dict, List, Literal, Optional
from urllib.parse import quote_plus

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, model_validator
import certifi
from pymongo import MongoClient
from pymongo import UpdateOne
from sqlalchemy import JSON, BigInteger, Boolean, Column, Float, MetaData, Table, Text, create_engine
from sqlalchemy import inspect, text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert

from etl_logger import activity, logger as etl_log
from transformer import safe_exec_transform, validate_transform_script
from utils import (
    ETLRuntimeError,
    build_singer_config,
    catalog_to_schemas,
    chunked,
    ensure_supported_source,
    extract_schema,
    merge_state,
    parse_discovery_output,
    parse_singer_stream,
    run_command,
    select_catalog_streams,
    temporary_json_file,
)

load_dotenv()

APP_NAME = "python-etl"
APP_VERSION = "2.0.0"
TAP_TIMEOUT_SECONDS = int(os.getenv("TAP_TIMEOUT_SECONDS", "1200"))
EMIT_CHUNK_SIZE = int(os.getenv("EMIT_CHUNK_SIZE", "1000"))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONNECTORS_DIR = os.path.join(BASE_DIR, "connectors")


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


class TransformRequest(BaseModel):
    records: Optional[List[Dict[str, Any]]] = None
    rows: Optional[List[Dict[str, Any]]] = None
    transform_script: str

    @model_validator(mode="after")
    def validate_payload(self):
        if self.records is None and self.rows is None:
            raise ValueError("Either `records` or `rows` is required")
        return self

    def get_records(self) -> List[Dict[str, Any]]:
        return self.records if self.records is not None else (self.rows or [])


class TransformResponse(BaseModel):
    transformed_rows: List[Dict[str, Any]]
    errors: List[Dict[str, Any]]


class EmitRequest(BaseModel):
    destination_type: Optional[str] = None
    connection_config: Dict[str, Any]
    destination_config: Dict[str, Any] = Field(default_factory=dict)
    table_name: str
    schema_name: Optional[str] = None
    records: Optional[List[Dict[str, Any]]] = None
    rows: Optional[List[Dict[str, Any]]] = None
    write_mode: Literal["append", "upsert", "replace"] = "upsert"
    upsert_key: Optional[List[str]] = None

    @model_validator(mode="after")
    def validate_payload(self):
        if self.records is None and self.rows is None:
            raise ValueError("Either `records` or `rows` is required")
        return self

    def get_records(self) -> List[Dict[str, Any]]:
        return self.records if self.records is not None else (self.rows or [])


class EmitResponse(BaseModel):
    rows_written: int
    rows_skipped: int
    rows_failed: int
    errors: List[Dict[str, Any]]


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
    auth_source: Optional[str] = None
    replica_set: Optional[str] = None
    tls: Optional[bool] = None


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


def _require_jwt(authorization: Optional[str] = Header(default=None)) -> str:
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header is required")
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Authorization must use Bearer token")
    token = authorization.replace("Bearer ", "", 1).strip()
    if not token:
        raise HTTPException(status_code=401, detail="Bearer token is empty")
    return token


def _tap_pythonpath() -> str:
    connector_paths = [
        os.path.join(CONNECTORS_DIR, "tap-postgres"),
        os.path.join(CONNECTORS_DIR, "tap-mysql"),
        os.path.join(CONNECTORS_DIR, "tap-mongodb"),
    ]
    existing = os.environ.get("PYTHONPATH", "")
    return os.pathsep.join([*connector_paths, existing] if existing else connector_paths)


def _tap_command(source_type: str) -> List[str]:
    source_type = ensure_supported_source(source_type)
    if source_type == "postgresql":
        return [sys.executable, "-c", "import tap_postgres; tap_postgres.main()"]
    if source_type == "mysql":
        return [sys.executable, "-m", "tap_mysql"]
    if source_type == "mongodb":
        return [sys.executable, "-c", "import tap_mongodb; tap_mongodb.main()"]
    raise ValueError(f"Unsupported source type: {source_type}")


def _catalog_arg_name(source_type: str) -> str:
    return "--properties" if source_type == "postgresql" else "--catalog"


def _tap_env() -> Dict[str, str]:
    """Build environment for Singer tap subprocesses."""
    env = os.environ.copy()
    env["PYTHONPATH"] = _tap_pythonpath()
    # Ensure subprocesses can verify TLS certificates (fixes macOS CA issue).
    env.setdefault("SSL_CERT_FILE", certifi.where())
    env.setdefault("REQUESTS_CA_BUNDLE", certifi.where())
    return env


def _run_discovery_sync(source_type: str, singer_config: Dict[str, Any]) -> Dict[str, Any]:
    env = _tap_env()

    with temporary_json_file(singer_config) as config_path:
        command = [*_tap_command(source_type), "--config", config_path, "--discover"]
        output = run_command(command, env=env, timeout_seconds=TAP_TIMEOUT_SECONDS)
        return parse_discovery_output(output)


def _run_collect_sync(
    source_type: str,
    singer_config: Dict[str, Any],
    selected_catalog: Dict[str, Any],
    state: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    env = _tap_env()

    with temporary_json_file(singer_config) as config_path, temporary_json_file(selected_catalog) as catalog_path:
        command = [*_tap_command(source_type), "--config", config_path, _catalog_arg_name(source_type), catalog_path]
        if state:
            with temporary_json_file(state) as state_path:
                command.extend(["--state", state_path])
                output = run_command(command, env=env, timeout_seconds=TAP_TIMEOUT_SECONDS)
        else:
            output = run_command(command, env=env, timeout_seconds=TAP_TIMEOUT_SECONDS)

    records, new_state = parse_singer_stream(output)
    return {"records": records, "state": new_state}


def _infer_sql_type(value: Any):
    if isinstance(value, bool):
        return Boolean
    if isinstance(value, int):
        return BigInteger
    if isinstance(value, float):
        return Float
    if isinstance(value, (dict, list)):
        return JSON
    return Text


def _sanitize_record_for_sql(record: Dict[str, Any], json_columns: set) -> Dict[str, Any]:
    """Coerce MongoDB-style record values so PostgreSQL / MySQL can accept them.

    * dict/list values destined for a non-JSON column are serialised to JSON strings.
    * dict/list values for JSON columns are left as-is (SQLAlchemy handles them).
    * ``decimal.Decimal`` is converted to ``float`` for broad driver compatibility.
    """
    import decimal

    cleaned: Dict[str, Any] = {}
    for key, value in record.items():
        if isinstance(value, (dict, list)):
            if key in json_columns:
                cleaned[key] = value
            else:
                cleaned[key] = json.dumps(value, default=str)
        elif isinstance(value, decimal.Decimal):
            cleaned[key] = float(value)
        else:
            cleaned[key] = value
    return cleaned


# Fixed namespace for deterministic ObjectId → UUID conversion.
# Every MongoDB _id always maps to the same UUID.
_MONGO_OID_NS = uuid.UUID("a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d")


def _objectid_to_uuid(oid: str) -> str:
    """Convert a MongoDB ObjectId hex string to a deterministic UUID v5."""
    return str(uuid.uuid5(_MONGO_OID_NS, oid))


def _is_uuid_column(col) -> bool:
    """Check whether a SQLAlchemy column is a UUID / GUID type."""
    from sqlalchemy.dialects.postgresql import UUID as PG_UUID

    col_type = col.type
    type_str = str(col_type).upper()
    if isinstance(col_type, PG_UUID):
        return True
    return type_str in ("UUID", "GUID")


def _build_sqlalchemy_url(dest_type: str, connection_config: Dict[str, Any]) -> str:
    if connection_config.get("connection_string"):
        return str(connection_config["connection_string"])

    user = connection_config.get("username") or connection_config.get("user")
    password = connection_config.get("password")
    host = connection_config.get("host", "localhost")
    port = connection_config.get("port")
    database = connection_config.get("database") or connection_config.get("dbname")

    if not database:
        raise ValueError("Destination database is required")

    user_part = quote_plus(str(user)) if user else ""
    password_part = quote_plus(str(password)) if password else ""
    auth_part = ""
    if user_part:
        auth_part = user_part
        if password is not None:
            auth_part += f":{password_part}"
        auth_part += "@"

    if dest_type == "postgresql":
        driver = "postgresql+psycopg2"
        default_port = 5432
    elif dest_type == "mysql":
        driver = "mysql+pymysql"
        default_port = 3306
    else:
        raise ValueError(f"Unsupported SQL destination: {dest_type}")

    resolved_port = int(port or default_port)
    return f"{driver}://{auth_part}{host}:{resolved_port}/{database}"


def _prepare_sql_table(
    engine,
    dest_type: str,
    table_name: str,
    schema_name: Optional[str],
    records: List[Dict[str, Any]],
    upsert_keys: List[str],
) -> Table:
    if not records:
        raise ValueError("No records available to prepare destination table")

    metadata = MetaData()
    inspector = inspect(engine)
    has_table = inspector.has_table(table_name, schema=schema_name if dest_type == "postgresql" else None)

    if not has_table:
        # Scan ALL records (not just the first) to pick the most appropriate
        # SQL type for each column.  A dict/list anywhere promotes to JSON;
        # a float anywhere promotes BigInteger → Float, etc.
        column_types: Dict[str, Any] = {}
        all_keys: List[str] = []
        seen_keys: set = set()
        for rec in records:
            for key, value in rec.items():
                if not isinstance(key, str):
                    continue
                if key not in seen_keys:
                    all_keys.append(key)
                    seen_keys.add(key)
                inferred = _infer_sql_type(value)
                existing = column_types.get(key)
                if existing is None:
                    column_types[key] = inferred
                elif existing is not inferred:
                    # Widen: any conflict → Text (safest), but keep JSON if either is JSON.
                    if inferred is JSON or existing is JSON:
                        column_types[key] = JSON
                    else:
                        column_types[key] = Text

        columns: List[Column] = []
        for key in all_keys:
            nullable = key not in upsert_keys
            columns.append(
                Column(
                    key,
                    column_types[key],
                    primary_key=key in upsert_keys,
                    nullable=nullable,
                )
            )
        if not columns:
            raise ValueError("Could not infer destination columns from records")
        table = Table(
            table_name,
            metadata,
            *columns,
            schema=schema_name if dest_type == "postgresql" else None,
        )
        metadata.create_all(engine, tables=[table])
        return table

    return Table(
        table_name,
        metadata,
        autoload_with=engine,
        schema=schema_name if dest_type == "postgresql" else None,
    )


def _emit_to_sql(
    dest_type: str,
    connection_config: Dict[str, Any],
    table_name: str,
    schema_name: Optional[str],
    write_mode: str,
    upsert_keys: List[str],
    records: List[Dict[str, Any]],
) -> EmitResponse:
    if not records:
        return EmitResponse(rows_written=0, rows_skipped=0, rows_failed=0, errors=[])

    # Pre-normalise field names before table creation so auto-created tables
    # get ``id`` instead of ``_id`` and don't include ``_stream``.
    for rec in records:
        rec.pop("_stream", None)
        if "_id" in rec and "id" not in rec:
            rec["id"] = rec.pop("_id")

    engine = create_engine(_build_sqlalchemy_url(dest_type, connection_config), future=True, pool_pre_ping=True)
    table = _prepare_sql_table(engine, dest_type, table_name, schema_name, records, upsert_keys)
    table_columns = {column.name for column in table.columns}

    # Build a set of column names whose SQL type is JSON so the sanitiser
    # knows which values it can pass as dicts/lists.
    json_columns: set = set()
    for col in table.columns:
        if isinstance(col.type, JSON):
            json_columns.add(col.name)

    # Convert ObjectId strings to deterministic UUIDs when the ``id`` column
    # is UUID typed (common in pre-existing PostgreSQL tables).
    id_is_uuid = False
    for col in table.columns:
        if col.name == "id" and _is_uuid_column(col):
            id_is_uuid = True
            break

    if id_is_uuid:
        for rec in records:
            raw_id = rec.get("id")
            if isinstance(raw_id, str) and len(raw_id) == 24:
                try:
                    int(raw_id, 16)  # validate it's hex
                    rec["id"] = _objectid_to_uuid(raw_id)
                except ValueError:
                    pass  # not a hex ObjectId, leave as-is

    rows_written = 0
    rows_failed = 0
    rows_skipped = 0
    errors: List[Dict[str, Any]] = []

    filtered_records = []
    for item in records:
        cleaned = {key: value for key, value in item.items() if key in table_columns}
        if cleaned:
            filtered_records.append(_sanitize_record_for_sql(cleaned, json_columns))
        else:
            rows_skipped += 1

    with engine.begin() as conn:
        if write_mode == "replace":
            if dest_type == "postgresql":
                schema = schema_name or "public"
                conn.execute(text(f'TRUNCATE TABLE "{schema}"."{table_name}" RESTART IDENTITY'))
            else:
                conn.execute(text(f"DELETE FROM `{table_name}`"))

        for batch in chunked(filtered_records, EMIT_CHUNK_SIZE):
            if not batch:
                continue
            try:
                if write_mode == "upsert":
                    if not upsert_keys:
                        raise ValueError("write_mode=upsert requires `upsert_key`")
                    if dest_type == "postgresql":
                        insert_stmt = pg_insert(table).values(batch)
                        update_columns = {
                            column.name: insert_stmt.excluded[column.name]
                            for column in table.columns
                            if column.name not in upsert_keys
                        }
                        if not update_columns:
                            update_columns = {upsert_keys[0]: insert_stmt.excluded[upsert_keys[0]]}
                        stmt = insert_stmt.on_conflict_do_update(index_elements=upsert_keys, set_=update_columns)
                        conn.execute(stmt)
                    else:
                        insert_stmt = mysql_insert(table).values(batch)
                        update_columns = {
                            column.name: insert_stmt.inserted[column.name]
                            for column in table.columns
                            if column.name not in upsert_keys
                        }
                        if not update_columns:
                            update_columns = {upsert_keys[0]: insert_stmt.inserted[upsert_keys[0]]}
                        stmt = insert_stmt.on_duplicate_key_update(**update_columns)
                        conn.execute(stmt)
                else:
                    conn.execute(table.insert(), batch)

                rows_written += len(batch)
            except Exception as exc:  # pragma: no cover - database-specific failures
                etl_log.error("Emit batch failed: %s", exc, exc_info=True)
                rows_failed += len(batch)
                errors.append({"error": str(exc), "batch_size": len(batch)})

    engine.dispose()
    return EmitResponse(
        rows_written=rows_written,
        rows_skipped=rows_skipped,
        rows_failed=rows_failed,
        errors=errors,
    )


def _emit_to_mongodb(
    connection_config: Dict[str, Any],
    table_name: str,
    write_mode: str,
    upsert_keys: List[str],
    records: List[Dict[str, Any]],
) -> EmitResponse:
    connection_uri = connection_config.get("connection_string_mongo") or connection_config.get("connection_string")
    if not connection_uri:
        host = connection_config.get("host", "localhost")
        port = int(connection_config.get("port", 27017))
        db_name = connection_config.get("database") or connection_config.get("dbname")
        if not db_name:
            raise ValueError("MongoDB database is required")
        user = connection_config.get("username") or connection_config.get("user")
        password = connection_config.get("password")
        if user and password:
            connection_uri = f"mongodb://{quote_plus(str(user))}:{quote_plus(str(password))}@{host}:{port}/{db_name}"
        else:
            connection_uri = f"mongodb://{host}:{port}/{db_name}"

    database_name = connection_config.get("database") or connection_config.get("dbname")
    if not database_name:
        raise ValueError("MongoDB database is required")

    client = MongoClient(connection_uri, tlsCAFile=certifi.where())
    collection = client[database_name][table_name]
    rows_written = 0
    rows_failed = 0
    errors: List[Dict[str, Any]] = []

    try:
        if write_mode == "replace":
            collection.delete_many({})

        if write_mode == "upsert":
            if not upsert_keys:
                upsert_keys = ["_id"]
            operations = []
            for record in records:
                filter_doc = {key: record.get(key) for key in upsert_keys}
                operations.append(UpdateOne(filter_doc, {"$set": record}, upsert=True))
            if operations:
                result = collection.bulk_write(operations, ordered=False)
                rows_written = result.upserted_count + result.modified_count + result.matched_count
        else:
            if records:
                result = collection.insert_many(records, ordered=False)
                rows_written = len(result.inserted_ids)
    except Exception as exc:  # pragma: no cover - database-specific failures
        rows_failed = len(records)
        errors.append({"error": str(exc)})
    finally:
        client.close()

    return EmitResponse(
        rows_written=rows_written,
        rows_skipped=0,
        rows_failed=rows_failed,
        errors=errors,
    )


@app.get("/")
async def root() -> Dict[str, str]:
    return {"service": APP_NAME, "version": APP_VERSION, "status": "ok"}


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "healthy"}


@app.post("/discover-schema/{source_type}", response_model=DiscoverSchemaResponse)
async def discover_schema(
    source_type: str,
    payload: DiscoverSchemaRequest,
    _token: str = Depends(_require_jwt),
) -> DiscoverSchemaResponse:
    normalized_source = ensure_supported_source(source_type)
    activity("datasource.schema_discovered", f"Discovering schema for {normalized_source}", source_type=normalized_source, metadata={
        "table_name": payload.table_name, "schema_name": payload.schema_name,
    })
    singer_config = build_singer_config(
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
        catalog = await asyncio.to_thread(_run_discovery_sync, normalized_source, singer_config)
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
    except ETLRuntimeError as exc:
        mysql_auth_error = _humanize_mysql_auth_error(str(exc))
        if mysql_auth_error:
            activity("request.error", f"Schema discovery failed: {mysql_auth_error}", level="error", source_type=normalized_source)
            raise HTTPException(status_code=400, detail=mysql_auth_error) from exc
        activity("request.error", f"Singer discovery failed: {exc}", level="error", source_type=normalized_source)
        raise HTTPException(status_code=502, detail=f"Singer discovery failed: {exc}") from exc
    except ValueError as exc:
        activity("request.error", f"Schema discovery validation error: {exc}", level="warn", source_type=normalized_source)
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/collect/{source_type}", response_model=CollectResponse)
async def collect(
    source_type: str,
    payload: CollectRequest,
    _token: str = Depends(_require_jwt),
) -> CollectResponse:
    normalized_source = ensure_supported_source(source_type)
    activity("sync.collect", f"Collecting from {normalized_source}", source_type=normalized_source, metadata={
        "sync_mode": payload.sync_mode, "table_name": payload.table_name, "limit": payload.limit, "offset": payload.offset,
    })
    singer_config = build_singer_config(
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
        discovery_catalog = await asyncio.to_thread(_run_discovery_sync, normalized_source, singer_config)
        selected_catalog, _selected_streams = select_catalog_streams(
            normalized_source,
            discovery_catalog,
            table_name=payload.table_name,
            schema_name=payload.schema_name,
            sync_mode=payload.sync_mode,
            replication_key=payload.replication_key,
        )
        collect_result = await asyncio.to_thread(
            _run_collect_sync,
            normalized_source,
            singer_config,
            selected_catalog,
            payload.checkpoint,
        )
    except ETLRuntimeError as exc:
        error_msg = str(exc)
        activity("request.error", f"Singer collect failed for {normalized_source}: {error_msg[:500]}", level="error", source_type=normalized_source, metadata={
            "table_name": payload.table_name, "sync_mode": payload.sync_mode,
        })
        mysql_auth_error = _humanize_mysql_auth_error(error_msg)
        if mysql_auth_error:
            raise HTTPException(status_code=400, detail=mysql_auth_error) from exc
        raise HTTPException(status_code=502, detail=f"Singer collect failed: {error_msg[:2000]}") from exc
    except ValueError as exc:
        activity("request.error", f"Collect validation error: {exc}", level="warn", source_type=normalized_source)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        error_msg = str(exc)
        activity("request.error", f"Unexpected collect error for {normalized_source}: {error_msg[:500]}", level="error", source_type=normalized_source)
        raise HTTPException(status_code=500, detail=f"Internal collect error: {error_msg[:2000]}") from exc

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


@app.post("/transform", response_model=TransformResponse)
async def transform(
    payload: TransformRequest,
    _token: str = Depends(_require_jwt),
) -> TransformResponse:
    validation = validate_transform_script(payload.transform_script)
    if not validation.get("valid", False):
        activity("request.error", f"Invalid transform script: {validation.get('error')}", level="warn")
        raise HTTPException(status_code=400, detail=validation.get("error", "Invalid transform script"))

    records = payload.get_records()
    activity("sync.transform", f"Transforming {len(records)} rows", metadata={"input_rows": len(records)})
    result = safe_exec_transform(records, payload.transform_script)
    activity("sync.transform", f"Transformed: {len(result['transformed_rows'])} rows, {len(result['errors'])} errors", metadata={
        "output_rows": len(result["transformed_rows"]), "error_count": len(result["errors"]),
    })
    return TransformResponse(
        transformed_rows=result["transformed_rows"],
        errors=result["errors"],
    )


@app.post("/emit/{dest_type}", response_model=EmitResponse)
async def emit(
    dest_type: str,
    payload: EmitRequest,
    _token: str = Depends(_require_jwt),
) -> EmitResponse:
    normalized_dest = ensure_supported_source(dest_type)
    records = payload.get_records()
    activity("sync.emit", f"Emitting {len(records)} rows to {normalized_dest}", source_type=normalized_dest, metadata={
        "row_count": len(records), "write_mode": payload.write_mode, "table": payload.table_name,
    })

    if normalized_dest in {"postgresql", "mysql"}:
        try:
            return _emit_to_sql(
                normalized_dest,
                payload.connection_config,
                payload.table_name,
                payload.schema_name,
                payload.write_mode,
                payload.upsert_key or [],
                records,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:  # pragma: no cover - database-specific failures
            raise HTTPException(status_code=500, detail=f"Emit failed: {exc}") from exc

    if normalized_dest == "mongodb":
        try:
            return _emit_to_mongodb(
                payload.connection_config,
                payload.table_name,
                payload.write_mode,
                payload.upsert_key or [],
                records,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:  # pragma: no cover - database-specific failures
            raise HTTPException(status_code=500, detail=f"Emit failed: {exc}") from exc

    raise HTTPException(status_code=400, detail=f"Unsupported destination type: {dest_type}")


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
    source_type = ensure_supported_source(payload.type)
    activity("datasource.connection_tested", f"Testing connection for {source_type}", source_type=source_type)
    config = payload.model_dump(exclude_none=True)

    if source_type == "mongodb":
        uri = config.get("connection_string_mongo") or config.get("connection_string")
        if not uri:
            host = config.get("host", "localhost")
            port = int(config.get("port", 27017))
            db_name = config.get("database", "admin")
            user = config.get("username") or config.get("user")
            password = config.get("password")
            if user and password:
                uri = f"mongodb://{quote_plus(str(user))}:{quote_plus(str(password))}@{host}:{port}/{db_name}"
            else:
                uri = f"mongodb://{host}:{port}/{db_name}"
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000, tlsCAFile=certifi.where())
            client.admin.command("ping")
            client.close()
            return TestConnectionResponse(success=True, message="MongoDB connection successful")
        except Exception as exc:
            return TestConnectionResponse(success=False, error=str(exc))

    try:
        url = _build_sqlalchemy_url(source_type, config)
        engine = create_engine(url, future=True, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return TestConnectionResponse(success=True, message="Connection successful")
    except Exception as exc:
        return TestConnectionResponse(success=False, error=str(exc))
