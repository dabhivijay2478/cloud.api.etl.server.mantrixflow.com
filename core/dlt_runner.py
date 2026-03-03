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


def _make_column_map_fn(
    column_map: list,
) -> Optional[Callable[[dict], dict]]:
    """
    Build a map function from column_map [{from_col, to_col}, ...].
    When column_map is non-empty: ONLY mapped columns are synced (filter + rename).
    Unmapped source columns are dropped. When empty: returns None (all columns pass through).
    """
    if not column_map:
        return None
    mapping = {
        m["from_col"]: m["to_col"]
        for m in column_map
        if m.get("from_col") and m.get("to_col")
    }
    if not mapping:
        return None

    # Case-insensitive lookup for source columns (Postgres lowercases, MongoDB may vary)
    mapping_lower = {k.lower(): v for k, v in mapping.items()}

    def rename_and_filter(row: dict) -> dict:
        out = {}
        for k, v in row.items():
            # Support schema.table.column format from sql_table
            simple_k = k.split(".")[-1] if "." in k else k
            key_lower = simple_k.lower()
            if key_lower in mapping_lower:
                out[mapping_lower[key_lower]] = v
        return out

    return rename_and_filter


def _write_disposition(write_mode: str) -> str:
    """Map write_mode to dlt write_disposition."""
    if write_mode == "replace":
        return "replace"
    if write_mode == "upsert":
        return "merge"
    return "append"


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

    schema = (dest_schema or "public").replace("-", "_")
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
    return "mantrixflow_data"


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


def run_full_sync(
    source_type: str,
    source_config: dict,
    dest_config: dict,
    source_stream: str,
    dest_table: str,
    pipeline_id: str = "default",
    write_mode: str = "append",
    upsert_key: Optional[list] = None,
    column_map: Optional[list] = None,
    organization_id: Optional[str] = None,
    dataset_name: Optional[str] = None,
    dest_schema: Optional[str] = None,
    custom_sql: Optional[str] = None,
) -> dict:
    """
    Run full sync: Collect → Transform → Emit.
    Postgres: sql_table | MongoDB: mongodb_collection
    """
    ds_name = _dataset_name(organization_id, dataset_name)
    schema = (dest_schema or ds_name).replace("-", "_")
    dest_creds = build_postgres_conn_str(dest_config)
    pipeline = _create_pipeline(
        pipeline_name=f"mantrixflow_sync_{pipeline_id}",
        destination=dlt.destinations.postgres(dest_creds),
        dataset_name=schema,
    )
    wd = _write_disposition(write_mode)

    if source_type in ("source-postgres", "postgres", "postgresql"):
        from sqlalchemy import text as sa_text

        from dlt.sources.sql_database import sql_table

        schema_name, table_name = parse_stream(source_stream)
        creds = build_postgres_conn_str(source_config)
        source_table_ref = f'"{schema_name}"."{table_name}"'

        if custom_sql and custom_sql.strip():
            # Custom SQL: substitute {{source_table}} with "schema"."table"
            processed_sql = custom_sql.strip().replace(
                "{{source_table}}", source_table_ref
            )
            processed_sql = processed_sql.replace(
                "{{source_schema}}", f'"{schema_name}"'
            )
            processed_sql = processed_sql.replace(
                "{{source_table_name}}", f'"{table_name}"'
            )

            def _query_adapter(query, table, incremental=None, engine=None):
                return sa_text(processed_sql)

            source = sql_table(
                table=table_name,
                schema=schema_name,
                credentials=creds,
                query_adapter_callback=_query_adapter,
            )
        else:
            source = sql_table(
                table=table_name,
                schema=schema_name,
                credentials=creds,
            )
    elif source_type in ("source-mongodb-v2", "mongodb"):
        conn_url = build_mongo_conn_url(source_config)
        db_name = source_config.get("database", "test")
        _, coll_name = parse_stream(source_stream)
        source = mongodb_collection(
            connection_url=conn_url,
            database=db_name,
            collection=coll_name,
        )
    else:
        raise ValueError(f"Unsupported source type: {source_type}")

    # Apply column_map (rename/filter) before load if configured
    map_fn = _make_column_map_fn(column_map or [])
    if map_fn is not None:
        source = source.add_map(map_fn)

    load_info = pipeline.run(
        source,
        write_disposition=wd,
        table_name=dest_table,
    )
    rows = _count_rows(load_info)

    return {
        "rows_synced": rows,
        "sync_mode": "full",
        "load_info": str(load_info),
    }


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
    column_map: Optional[list] = None,
    organization_id: Optional[str] = None,
    dataset_name: Optional[str] = None,
    dest_schema: Optional[str] = None,
    custom_sql: Optional[str] = None,
) -> dict:
    """
    Run incremental sync.
    Postgres: pg_replication (WAL, log-based) — no cursor. Custom SQL applies to initial snapshot only.
    MongoDB: mongodb_collection + dlt.sources.incremental(cursor_field). Custom SQL not supported.
    """
    if custom_sql and custom_sql.strip():
        if source_type in ("source-mongodb-v2", "mongodb"):
            raise ValueError(
                "Custom SQL is only supported for Postgres source. Use Field Mapping for MongoDB."
            )
    ds_name = _dataset_name(organization_id, dataset_name)
    schema = (dest_schema or ds_name).replace("-", "_")
    dest_creds = build_postgres_conn_str(dest_config)
    pipeline = _create_pipeline(
        pipeline_name=f"mantrixflow_{pipeline_id}",
        destination=dlt.destinations.postgres(dest_creds),
        dataset_name=schema,
    )
    wd = _write_disposition(write_mode)

    if source_type in ("source-postgres", "postgres", "postgresql"):
        return _run_postgres_incremental(
            source_config, dest_config, source_stream, dest_table,
            pipeline_id, pipeline, wd, column_map,
            custom_sql=custom_sql,
        )
    elif source_type in ("source-mongodb-v2", "mongodb"):
        return _run_mongodb_incremental(
            source_config, dest_config, source_stream, dest_table,
            cursor_field, initial_state, pipeline_id, pipeline, wd, column_map,
        )
    else:
        raise ValueError(f"Unsupported source type: {source_type}")


def _run_postgres_incremental(
    source_config: dict,
    dest_config: dict,
    source_stream: str,
    dest_table: str,
    pipeline_id: str,
    pipeline: dlt.Pipeline,
    write_disposition: str,
    column_map: Optional[list] = None,
    custom_sql: Optional[str] = None,
) -> dict:
    """Postgres incremental via pg_replication (WAL, log-based). No cursor.
    When custom_sql is provided, it applies to the initial snapshot only; WAL changes are raw."""
    schema_name, table_name = parse_stream(source_stream)
    creds = ConnectionStringCredentials(build_postgres_conn_str(source_config))
    slot_name = f"mantrix_slot_{pipeline_id}"
    pub_name = f"mantrix_pub_{pipeline_id}"

    map_fn = _make_column_map_fn(column_map or [])
    use_custom_sql = custom_sql and custom_sql.strip()

    if use_custom_sql:
        # Custom SQL: set up slot/pub only (no built-in snapshot), then use sql_table for initial load
        init_replication(
            slot_name=slot_name,
            pub_name=pub_name,
            schema_name=schema_name,
            table_names=table_name,
            credentials=creds,
            persist_snapshots=False,
            reset=False,
        )
        from sqlalchemy import text as sa_text

        from dlt.sources.sql_database import sql_table

        source_table_ref = f'"{schema_name}"."{table_name}"'
        processed_sql = custom_sql.strip().replace("{{source_table}}", source_table_ref)
        processed_sql = processed_sql.replace("{{source_schema}}", f'"{schema_name}"')
        processed_sql = processed_sql.replace("{{source_table_name}}", f'"{table_name}"')

        def _query_adapter(query, table, incremental=None, engine=None):
            return sa_text(processed_sql)

        snapshot = sql_table(
            table=table_name,
            schema=schema_name,
            credentials=build_postgres_conn_str(source_config),
            query_adapter_callback=_query_adapter,
        )
        if map_fn is not None:
            snapshot = snapshot.add_map(map_fn)
        pipeline.run(
            snapshot,
            write_disposition=write_disposition,
            table_name=dest_table,
        )
    else:
        # No custom SQL: use init_replication snapshot
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
            if map_fn is not None:
                snapshot = snapshot.add_map(map_fn)
            pipeline.run(
                snapshot,
                write_disposition=write_disposition,
                table_name=dest_table,
            )

    changes = replication_resource(slot_name, pub_name, credentials=creds)
    if map_fn is not None:
        changes = changes.add_map(map_fn)
    load_info = pipeline.run(
        changes,
        write_disposition=write_disposition,
        table_name=dest_table,
    )
    rows = _count_rows(load_info)

    return {
        "rows_synced": rows,
        "sync_mode": "incremental",
        "load_info": str(load_info),
    }


def _run_mongodb_incremental(
    source_config: dict,
    dest_config: dict,
    source_stream: str,
    dest_table: str,
    cursor_field: str,
    initial_state: Optional[dict],
    pipeline_id: str,
    pipeline: dlt.Pipeline,
    write_disposition: str,
    column_map: Optional[list] = None,
) -> dict:
    """MongoDB incremental via mongodb_collection + dlt.sources.incremental."""
    conn_url = build_mongo_conn_url(source_config)
    db_name = source_config.get("database", "test")
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

    map_fn = _make_column_map_fn(column_map or [])
    if map_fn is not None:
        source = source.add_map(map_fn)

    load_info = pipeline.run(
        source,
        write_disposition=write_disposition,
        table_name=dest_table,
    )
    rows = _count_rows(load_info)

    # dlt stores incremental state in pipeline; NestJS checkpoint can use state_blob
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


def discover_postgres(connection_config: dict, schema_name: str = "public") -> dict:
    """Discover Postgres tables and columns."""
    from sqlalchemy import create_engine, inspect

    conn_str = build_postgres_conn_str(connection_config)
    engine = create_engine(conn_str)
    insp = inspect(engine)
    tables = insp.get_table_names(schema=schema_name or "public")
    columns = []
    for t in tables:
        for col in insp.get_columns(t, schema=schema_name or "public"):
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
    db_name = database or connection_config.get("database", "test")
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
            text(f'SELECT * FROM "{schema_name}"."{table_name}" LIMIT {limit}')
        )
        rows = [dict(zip(result.keys(), r)) for r in result]
        cols = list(result.keys()) if rows else []
    return {"records": rows, "columns": cols, "total": len(rows), "stream": source_stream}


def preview_mongodb(connection_config: dict, source_stream: str, limit: int = 50) -> dict:
    """Preview documents from MongoDB collection."""
    import pymongo
    from bson import ObjectId

    conn_url = build_mongo_conn_url(connection_config)
    db_name = connection_config.get("database", "test")
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
