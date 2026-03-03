"""
dlt runner — Collect → Transform → Emit
Uses dlt verified sources only: sql_database, pg_replication, mongodb
Shared config via config.connections (no duplication).

Credentials: Never read from .dlt/secrets.toml for API runs.
They come from the request payload (NestJS fetches from DB, decrypts, passes source_config/dest_config).
"""

from typing import Any, Callable, Optional

import dlt
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.pipeline.pipeline import Pipeline
from dlt.common.pipeline import get_dlt_pipelines_dir
from dlt.common.utils import digest256
from dlt.pipeline.progress import _from_name as collector_from_name, _NULL_COLLECTOR
from dlt.pipeline.configuration import PipelineConfiguration, PipelineRuntimeConfiguration
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext

from config.connections import (
    build_postgres_conn_str,
    build_mongo_conn_url,
    parse_stream,
)
from pg_replication import replication_resource
from pg_replication.helpers import init_replication
from mongodb import mongodb_collection

# ---------------------------------------------------------------------------
# Constants — no hardcoded literals in business logic
# ---------------------------------------------------------------------------

DEFAULT_DATASET_NAME = "mantrixflow_data"
DEFAULT_SCHEMA = "public"
DEFAULT_MONGODB_DATABASE = "test"
PIPELINE_NAME_PREFIX = "mantrixflow_sync"
REPLICATION_SLOT_PREFIX = "mantrix_slot"
REPLICATION_PUB_PREFIX = "mantrix_pub"

POSTGRES_SOURCE_TYPES = ("source-postgres", "postgres", "postgresql")
MONGODB_SOURCE_TYPES = ("source-mongodb-v2", "mongodb")

# Per-entity schema contract: allow new tables, freeze columns and data types.
# See https://dlthub.com/docs/general-usage/schema-contracts
FROZEN_SCHEMA_CONTRACT: dict = {
    "tables": "evolve",
    "columns": "freeze",
    "data_type": "freeze",
}


def _write_disposition(write_mode: str) -> str:
    """Map write_mode to dlt write_disposition."""
    if write_mode == "replace":
        return "replace"
    if write_mode == "upsert":
        return "merge"
    return "append"


def _compile_transform_script(script: str) -> Callable[[dict], dict]:
    """
    Compile user Python script to get transform(row) -> dict.
    Contract: script must define def transform(row: dict) -> dict.
    Safe execution: restricted namespace, no file/network access.
    """
    restricted_globals: dict = {
        "__builtins__": {
            "dict": dict,
            "list": list,
            "str": str,
            "int": int,
            "float": float,
            "bool": bool,
            "None": None,
            "True": True,
            "False": False,
            "type": type,
            "len": len,
            "range": range,
            "enumerate": enumerate,
            "zip": zip,
            "map": map,
            "filter": filter,
            "sorted": sorted,
            "min": min,
            "max": max,
            "sum": sum,
            "abs": abs,
            "round": round,
            "getattr": getattr,
            "hasattr": hasattr,
            "isinstance": isinstance,
            "set": set,
            "tuple": tuple,
            "repr": repr,
            "print": lambda *a, **k: None,  # no-op
        },
    }
    code = compile(script, "<transform_script>", "exec")
    exec(code, restricted_globals)
    transform_fn = restricted_globals.get("transform")
    if not callable(transform_fn):
        raise ValueError(
            "Transform script must define a function named 'transform' with signature: def transform(row: dict) -> dict"
        )
    return transform_fn  # type: ignore


def verify_destination_table_exists(
    dest_config: dict,
    dest_schema: str,
    dest_table: str,
) -> bool:
    """
    Check if the destination table exists in Postgres.
    Used when destination_table_exists=True to enforce sync-to-existing-only.
    """
    from sqlalchemy import create_engine, text

    schema = (dest_schema or DEFAULT_SCHEMA).replace("-", "_")
    conn_str = build_postgres_conn_str(dest_config)
    engine = create_engine(conn_str)
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = :schema AND table_name = :table
                LIMIT 1
                """
            ),
            {"schema": schema, "table": dest_table},
        )
        return result.fetchone() is not None


def _dataset_name(organization_id: Optional[str] = None, dataset_name: Optional[str] = None) -> str:
    """Derive dataset name for multi-tenant isolation. Valid Postgres schema identifier."""
    if dataset_name:
        return dataset_name.replace("-", "_")
    if organization_id:
        return f"org_{organization_id.replace('-', '_')}"
    return DEFAULT_DATASET_NAME


def _create_pipeline(
    pipeline_name: str,
    destination: Any,
    dataset_name: str,
) -> Pipeline:
    """Create a Pipeline without config injection to avoid credentials leaking into pipeline kwargs."""
    pipelines_dir = get_dlt_pipelines_dir()
    pipeline_salt = digest256(pipeline_name)
    progress = collector_from_name(_NULL_COLLECTOR)
    runtime = PipelineRuntimeConfiguration(pluggable_run_context=PluggableRunContext())
    config = PipelineConfiguration(
        pipeline_name=pipeline_name,
        dataset_name=dataset_name,
        pipeline_salt=pipeline_salt,
        runtime=runtime,
    )
    return Pipeline(
        pipeline_name=pipeline_name,
        pipelines_dir=pipelines_dir,
        pipeline_salt=pipeline_salt,
        destination=destination,
        staging=None,
        dataset_name=dataset_name,
        import_schema_path=None,
        export_schema_path=None,
        dev_mode=False,
        progress=progress,
        must_attach_to_local_pipeline=False,
        config=config,
        refresh=None,
    )


def _build_run_kw(
    write_disposition: str,
    dest_table: str,
    destination_table_exists: bool,
) -> dict:
    """Build the common kwargs dict for pipeline.run()."""
    kw: dict = {"write_disposition": write_disposition, "table_name": dest_table}
    if destination_table_exists:
        kw["schema_contract"] = FROZEN_SCHEMA_CONTRACT
    return kw


def _make_column_filter(
    selected_columns: list,
    always_include: Optional[list] = None,
) -> Callable[[dict], dict]:
    """Return an add_map function that keeps only *selected_columns*.
    *always_include* ensures keys like ``_id`` are never dropped (needed for MongoDB)."""
    col_set = set(selected_columns)
    if always_include:
        col_set.update(always_include)

    def _filter(row: dict) -> dict:
        return {k: v for k, v in row.items() if k in col_set}

    return _filter


# ---------------------------------------------------------------------------
# Full sync
# ---------------------------------------------------------------------------

def run_full_sync(
    source_type: str,
    source_config: dict,
    dest_config: dict,
    source_stream: str,
    dest_table: str,
    pipeline_id: str = "default",
    write_mode: str = "append",
    upsert_key: Optional[list] = None,
    organization_id: Optional[str] = None,
    dataset_name: Optional[str] = None,
    dest_schema: Optional[str] = None,
    custom_sql: Optional[str] = None,
    destination_table_exists: bool = False,
    transform_script: Optional[str] = None,
    transform_type: Optional[str] = None,
    selected_columns: Optional[list] = None,
) -> dict:
    """
    Run full sync: Collect → Transform → Emit.
    Postgres: sql_table | MongoDB: mongodb_collection
    """
    ds_name = _dataset_name(organization_id, dataset_name)
    schema = (dest_schema or ds_name).replace("-", "_")
    dest_creds = build_postgres_conn_str(dest_config)
    pipeline = _create_pipeline(
        pipeline_name=f"{PIPELINE_NAME_PREFIX}_{pipeline_id}",
        destination=dlt.destinations.postgres(dest_creds),
        dataset_name=schema,
    )
    wd = _write_disposition(write_mode)
    effective_transform_type = (transform_type or "dlt").lower()

    use_custom_sql = effective_transform_type == "dbt" and custom_sql and custom_sql.strip()
    use_script = effective_transform_type == "script" and transform_script and transform_script.strip()

    if source_type in POSTGRES_SOURCE_TYPES:
        source = _build_postgres_source(
            source_config, source_stream,
            custom_sql=custom_sql if use_custom_sql else None,
            selected_columns=selected_columns,
        )
    elif source_type in MONGODB_SOURCE_TYPES:
        source = _build_mongodb_source(source_config, source_stream)
        if selected_columns:
            source = source.add_map(_make_column_filter(selected_columns, always_include=["_id"]))
    else:
        raise ValueError(f"Unsupported source type: {source_type}")

    if use_script:
        source = source.add_map(_compile_transform_script(transform_script))

    run_kw = _build_run_kw(wd, dest_table, destination_table_exists)
    if upsert_key:
        run_kw["primary_key"] = upsert_key

    load_info = pipeline.run(source, **run_kw)
    rows = _count_rows(load_info)

    return {
        "rows_synced": rows,
        "sync_mode": "full",
        "load_info": str(load_info),
    }


# ---------------------------------------------------------------------------
# Incremental sync
# ---------------------------------------------------------------------------

def run_incremental_sync(
    source_type: str,
    source_config: dict,
    dest_config: dict,
    source_stream: str,
    dest_table: str,
    cursor_field: str,
    pipeline_id: str = "default",
    write_mode: str = "merge",
    upsert_key: Optional[list] = None,
    initial_state: Optional[dict] = None,
    organization_id: Optional[str] = None,
    dataset_name: Optional[str] = None,
    dest_schema: Optional[str] = None,
    custom_sql: Optional[str] = None,
    destination_table_exists: bool = False,
    transform_script: Optional[str] = None,
    transform_type: Optional[str] = None,
    selected_columns: Optional[list] = None,
) -> dict:
    """
    Run incremental sync.
    Postgres: pg_replication (WAL, log-based) — no cursor. Custom SQL applies to initial snapshot only.
    MongoDB: mongodb_collection + dlt.sources.incremental(cursor_field). Custom SQL not supported.
    """
    effective_transform_type = (transform_type or "dlt").lower()

    if effective_transform_type == "dbt" and custom_sql and custom_sql.strip():
        if source_type in MONGODB_SOURCE_TYPES:
            raise ValueError(
                "Custom SQL is only supported for Postgres source. Use Field Mapping for MongoDB."
            )

    ds_name = _dataset_name(organization_id, dataset_name)
    schema = (dest_schema or ds_name).replace("-", "_")
    dest_creds = build_postgres_conn_str(dest_config)
    pipeline = _create_pipeline(
        pipeline_name=f"{PIPELINE_NAME_PREFIX}_{pipeline_id}",
        destination=dlt.destinations.postgres(dest_creds),
        dataset_name=schema,
    )
    wd = _write_disposition(write_mode)

    use_custom_sql = effective_transform_type == "dbt" and custom_sql and custom_sql.strip()
    use_script = effective_transform_type == "script" and transform_script and transform_script.strip()

    if source_type in POSTGRES_SOURCE_TYPES:
        return _run_postgres_incremental(
            source_config, source_stream, dest_table,
            pipeline_id, pipeline, wd,
            custom_sql=custom_sql if use_custom_sql else None,
            destination_table_exists=destination_table_exists,
            transform_script=transform_script if use_script else None,
            selected_columns=selected_columns,
            upsert_key=upsert_key,
        )
    elif source_type in MONGODB_SOURCE_TYPES:
        return _run_mongodb_incremental(
            source_config, source_stream, dest_table,
            cursor_field, initial_state, pipeline_id, pipeline, wd,
            destination_table_exists=destination_table_exists,
            transform_script=transform_script if use_script else None,
            selected_columns=selected_columns,
            upsert_key=upsert_key,
        )
    else:
        raise ValueError(f"Unsupported source type: {source_type}")


# ---------------------------------------------------------------------------
# Postgres helpers
# ---------------------------------------------------------------------------

def _build_postgres_source(
    source_config: dict,
    source_stream: str,
    custom_sql: Optional[str] = None,
    selected_columns: Optional[list] = None,
) -> Any:
    """Create a dlt sql_table resource for Postgres, optionally with custom SQL or column selection."""
    from dlt.sources.sql_database import sql_table

    schema_name, table_name = parse_stream(source_stream)
    creds = build_postgres_conn_str(source_config)

    if custom_sql and custom_sql.strip():
        from sqlalchemy import text as sa_text

        source_table_ref = f'"{schema_name}"."{table_name}"'
        processed_sql = custom_sql.strip().replace("{{source_table}}", source_table_ref)
        processed_sql = processed_sql.replace("{{source_schema}}", f'"{schema_name}"')
        processed_sql = processed_sql.replace("{{source_table_name}}", f'"{table_name}"')

        def _query_adapter(query, table, incremental=None, engine=None):
            return sa_text(processed_sql)

        return sql_table(
            table=table_name,
            schema=schema_name,
            credentials=creds,
            query_adapter_callback=_query_adapter,
        )

    kw: dict = {"table": table_name, "schema": schema_name, "credentials": creds}
    if selected_columns:
        kw["included_columns"] = selected_columns
    return sql_table(**kw)


def _run_postgres_incremental(
    source_config: dict,
    source_stream: str,
    dest_table: str,
    pipeline_id: str,
    pipeline: dlt.Pipeline,
    write_disposition: str,
    custom_sql: Optional[str] = None,
    destination_table_exists: bool = False,
    transform_script: Optional[str] = None,
    selected_columns: Optional[list] = None,
    upsert_key: Optional[list] = None,
) -> dict:
    """Postgres incremental via pg_replication (WAL, log-based). No cursor.
    When custom_sql is provided, it applies to the initial snapshot only; WAL changes are raw."""
    schema_name, table_name = parse_stream(source_stream)
    creds = ConnectionStringCredentials(build_postgres_conn_str(source_config))
    slot_name = f"{REPLICATION_SLOT_PREFIX}_{pipeline_id}"
    pub_name = f"{REPLICATION_PUB_PREFIX}_{pipeline_id}"

    run_kw = _build_run_kw(write_disposition, dest_table, destination_table_exists)
    if upsert_key:
        run_kw["primary_key"] = upsert_key

    if custom_sql and custom_sql.strip():
        init_replication(
            slot_name=slot_name,
            pub_name=pub_name,
            schema_name=schema_name,
            table_names=table_name,
            credentials=creds,
            persist_snapshots=False,
            reset=False,
        )
        snapshot = _build_postgres_source(source_config, source_stream, custom_sql=custom_sql)
        if transform_script:
            snapshot = snapshot.add_map(_compile_transform_script(transform_script))
        pipeline.run(snapshot, **run_kw)
    else:
        snapshot = init_replication(
            slot_name=slot_name,
            pub_name=pub_name,
            schema_name=schema_name,
            table_names=table_name,
            credentials=creds,
            persist_snapshots=True,
            reset=False,
        )
        if snapshot is not None:
            if selected_columns:
                snapshot = snapshot.add_map(_make_column_filter(selected_columns))
            if transform_script:
                snapshot = snapshot.add_map(_compile_transform_script(transform_script))
            pipeline.run(snapshot, **run_kw)

    changes = replication_resource(slot_name, pub_name, credentials=creds)
    if selected_columns:
        changes = changes.add_map(_make_column_filter(selected_columns))
    if transform_script:
        changes = changes.add_map(_compile_transform_script(transform_script))
    load_info = pipeline.run(changes, **run_kw)
    rows = _count_rows(load_info)

    return {
        "rows_synced": rows,
        "sync_mode": "incremental",
        "load_info": str(load_info),
    }


# ---------------------------------------------------------------------------
# MongoDB helpers
# ---------------------------------------------------------------------------

def _build_mongodb_source(source_config: dict, source_stream: str) -> Any:
    """Create a dlt mongodb_collection resource."""
    conn_url = build_mongo_conn_url(source_config)
    db_name = source_config.get("database", DEFAULT_MONGODB_DATABASE)
    _, coll_name = parse_stream(source_stream)
    return mongodb_collection(
        connection_url=conn_url,
        database=db_name,
        collection=coll_name,
    )


def _run_mongodb_incremental(
    source_config: dict,
    source_stream: str,
    dest_table: str,
    cursor_field: str,
    initial_state: Optional[dict],
    pipeline_id: str,
    pipeline: dlt.Pipeline,
    write_disposition: str,
    destination_table_exists: bool = False,
    transform_script: Optional[str] = None,
    selected_columns: Optional[list] = None,
    upsert_key: Optional[list] = None,
) -> dict:
    """MongoDB incremental via mongodb_collection + dlt.sources.incremental."""
    conn_url = build_mongo_conn_url(source_config)
    db_name = source_config.get("database", DEFAULT_MONGODB_DATABASE)
    _, coll_name = parse_stream(source_stream)

    state = initial_state or {}
    state_blob = state.get("state_blob") or state
    last_val = state_blob.get("cursor_value") or state_blob.get("last_value")

    incremental = dlt.sources.incremental(
        cursor_field,
        initial_value=last_val,
    )
    source = mongodb_collection(
        connection_url=conn_url,
        database=db_name,
        collection=coll_name,
        incremental=incremental,
    )
    if selected_columns:
        source = source.add_map(_make_column_filter(selected_columns, always_include=["_id"]))
    if transform_script:
        source = source.add_map(_compile_transform_script(transform_script))

    run_kw = _build_run_kw(write_disposition, dest_table, destination_table_exists)
    if upsert_key:
        run_kw["primary_key"] = upsert_key
    load_info = pipeline.run(source, **run_kw)
    rows = _count_rows(load_info)

    last_value = state_blob.get("cursor_value")
    try:
        state_sources = pipeline.state.get("sources", {}) or {}
        for _key, src in state_sources.items():
            res = (src.get("resources") or {}).get(coll_name, {})
            inc = res.get("incremental", {})
            if inc.get("cursor_value") is not None:
                last_value = inc["cursor_value"]
                break
    except Exception:
        pass

    return {
        "rows_synced": rows,
        "sync_mode": "incremental",
        "new_state": {
            "cursor_value": last_value,
            "state_blob": {"cursor_value": last_value},
        },
        "load_info": str(load_info),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _count_rows(load_info: Any) -> int:
    """Extract row count from dlt LoadInfo."""
    rows = 0
    try:
        for pkg in getattr(load_info, "load_packages", []) or []:
            for j in getattr(pkg, "jobs", {}).get("completed_jobs", []) or []:
                rows += getattr(j, "rows_count", 0) or 0
    except Exception:
        pass
    return rows


# ---------------------------------------------------------------------------
# Discovery & Preview (unchanged, used by /discover-schema and /preview)
# ---------------------------------------------------------------------------

def discover_postgres(connection_config: dict, schema_name: str = DEFAULT_SCHEMA) -> dict:
    """Discover Postgres tables and columns."""
    from sqlalchemy import create_engine, inspect

    conn_str = build_postgres_conn_str(connection_config)
    engine = create_engine(conn_str)
    insp = inspect(engine)
    tables = insp.get_table_names(schema=schema_name or DEFAULT_SCHEMA)
    columns = []
    for t in tables:
        for col in insp.get_columns(t, schema=schema_name or DEFAULT_SCHEMA):
            columns.append({"name": col["name"], "type": str(col["type"]), "table": t})
    return {"columns": columns, "primary_keys": [], "tables": tables, "estimated_row_count": None}


def discover_mongodb(
    connection_config: dict,
    database: Optional[str] = None,
    collection: Optional[str] = None,
) -> dict:
    """Discover MongoDB collections and sample columns."""
    import pymongo

    conn_url = build_mongo_conn_url(connection_config)
    client = pymongo.MongoClient(conn_url)
    db_name = database or connection_config.get("database", DEFAULT_MONGODB_DATABASE)
    db = client[db_name]
    collections = db.list_collection_names()
    columns = []
    if collection and collection in collections:
        sample = db[collection].find_one()
        if sample:
            for k in sample.keys():
                columns.append({"name": k, "type": "string", "nullable": True})
    return {"columns": columns, "primary_keys": ["_id"], "collections": collections}


def preview_postgres(connection_config: dict, source_stream: str, limit: int = 50) -> dict:
    """Preview rows from Postgres table."""
    from sqlalchemy import create_engine, text

    conn_str = build_postgres_conn_str(connection_config)
    schema_name, table_name = parse_stream(source_stream)
    engine = create_engine(conn_str)
    with engine.connect() as conn:
        result = conn.execute(
            text('SELECT * FROM "' + schema_name + '"."' + table_name + '" LIMIT :lim'),
            {"lim": limit},
        )
        rows = [dict(zip(result.keys(), r)) for r in result]
        cols = list(result.keys()) if rows else []
    return {"records": rows, "columns": cols, "total": len(rows), "stream": source_stream}


def preview_mongodb(connection_config: dict, source_stream: str, limit: int = 50) -> dict:
    """Preview documents from MongoDB collection."""
    import pymongo
    from bson import ObjectId

    conn_url = build_mongo_conn_url(connection_config)
    db_name = connection_config.get("database", DEFAULT_MONGODB_DATABASE)
    _, coll_name = parse_stream(source_stream)
    client = pymongo.MongoClient(conn_url)
    coll = client[db_name][coll_name]
    docs = list(coll.find().limit(limit))
    records = []
    for d in docs:
        r = dict(d)
        if "_id" in r and isinstance(r["_id"], ObjectId):
            r["_id"] = str(r["_id"])
        records.append(r)
    cols = list(records[0].keys()) if records else []
    return {"records": records, "columns": cols, "total": len(records), "stream": source_stream}
