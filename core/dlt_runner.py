"""
dlt runner — Collect → Transform → Emit
Uses dlt verified sources only: sql_database, pg_replication, mongodb
Shared config via config.connections (no duplication).

Credentials: Never read from .dlt/secrets.toml for API runs.
They come from the request payload (NestJS fetches from DB, decrypts, passes source_config/dest_config).
"""

from typing import Any, Optional

import dlt
from dlt.sources.credentials import ConnectionStringCredentials

from config.connections import (
    build_postgres_conn_str,
    build_mongo_conn_url,
    parse_stream,
)
from pg_replication import replication_resource
from pg_replication.helpers import init_replication
from mongodb import mongodb_collection


def _write_disposition(write_mode: str) -> str:
    """Map write_mode to dlt write_disposition."""
    if write_mode == "replace":
        return "replace"
    if write_mode == "upsert":
        return "merge"
    return "append"


def _dataset_name(organization_id: Optional[str] = None, dataset_name: Optional[str] = None) -> str:
    """Derive dataset name for multi-tenant isolation. Valid Postgres schema identifier."""
    if dataset_name:
        return dataset_name.replace("-", "_")
    if organization_id:
        return f"org_{organization_id.replace('-', '_')}"
    return "mantrixflow_data"


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
) -> dict:
    """
    Run full sync: Collect → Transform → Emit.
    Postgres: sql_table | MongoDB: mongodb_collection
    """
    ds_name = _dataset_name(organization_id, dataset_name)
    schema = (dest_schema or ds_name).replace("-", "_")
    pipeline = dlt.pipeline(
        pipeline_name=f"mantrixflow_sync_{pipeline_id}",
        destination="postgres",
        dataset_name=schema,
        credentials=build_postgres_conn_str(dest_config),
    )
    wd = _write_disposition(write_mode)

    if source_type in ("source-postgres", "postgres", "postgresql"):
        from dlt.sources.sql_database import sql_table

        schema_name, table_name = parse_stream(source_stream)
        creds = build_postgres_conn_str(source_config)
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

    load_info = pipeline.run(source, write_disposition=wd, table_name=dest_table)
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
    organization_id: Optional[str] = None,
    dataset_name: Optional[str] = None,
    dest_schema: Optional[str] = None,
) -> dict:
    """
    Run incremental sync.
    Postgres: pg_replication (WAL, log-based) — no cursor
    MongoDB: mongodb_collection + dlt.sources.incremental(cursor_field)
    """
    ds_name = _dataset_name(organization_id, dataset_name)
    schema = (dest_schema or ds_name).replace("-", "_")
    pipeline = dlt.pipeline(
        pipeline_name=f"mantrixflow_{pipeline_id}",
        destination="postgres",
        dataset_name=schema,
        credentials=build_postgres_conn_str(dest_config),
    )
    wd = _write_disposition(write_mode)

    if source_type in ("source-postgres", "postgres", "postgresql"):
        return _run_postgres_incremental(
            source_config, dest_config, source_stream, dest_table,
            pipeline_id, pipeline, wd,
        )
    elif source_type in ("source-mongodb-v2", "mongodb"):
        return _run_mongodb_incremental(
            source_config, dest_config, source_stream, dest_table,
            cursor_field, initial_state, pipeline_id, pipeline, wd,
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
) -> dict:
    """Postgres incremental via pg_replication (WAL, log-based)."""
    schema_name, table_name = parse_stream(source_stream)
    creds = ConnectionStringCredentials(build_postgres_conn_str(source_config))
    slot_name = f"mantrix_slot_{pipeline_id}"
    pub_name = f"mantrix_pub_{pipeline_id}"

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
        pipeline.run(snapshot, write_disposition=write_disposition, table_name=dest_table)

    changes = replication_resource(slot_name, pub_name, credentials=creds)
    load_info = pipeline.run(changes, write_disposition=write_disposition, table_name=dest_table)
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

    load_info = pipeline.run(source, write_disposition=write_disposition, table_name=dest_table)
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
