"""Main sync — POST /sync/run-sync."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List

from core import dlt_runner

router = APIRouter()


class RunSyncRequest(BaseModel):
    job_id: str
    pipeline_id: str
    organization_id: str
    source_conn_id: Optional[str] = None
    dest_conn_id: Optional[str] = None
    source_config: dict
    dest_config: dict
    source_type: str
    dest_type: str
    source_stream: str
    dest_table: str
    sync_mode: str = "full"
    write_mode: str = "append"
    upsert_key: Optional[List[str]] = None
    cursor_field: Optional[str] = None
    column_map: Optional[List[dict]] = None
    transformations: Optional[List[dict]] = None
    callback_url: Optional[str] = None
    callback_token: Optional[str] = None
    initial_state: Optional[dict] = None
    dataset_name: Optional[str] = None
    dest_schema: Optional[str] = None
    destination_table_exists: Optional[bool] = None
    custom_sql: Optional[str] = None
    transform_type: Optional[str] = None


@router.post("/run-sync")
def run_sync(body: RunSyncRequest):
    """Run Collect → Transform → Emit via dlt."""
    try:
        if not body.source_stream:
            raise HTTPException(status_code=400, detail="source_stream is required")
        dest_table = body.dest_table or body.source_stream.split(".")[-1]

        # When destination_table_exists=True, verify table exists before sync (no auto-create)
        if body.destination_table_exists:
            dest_schema = body.dest_schema or "public"
            if not dlt_runner.verify_destination_table_exists(
                body.dest_config, dest_schema, dest_table
            ):
                raise HTTPException(
                    status_code=400,
                    detail="Destination table does not exist. Sync only supports existing tables.",
                )

        # Custom SQL only supported for Postgres source
        custom_sql = (body.custom_sql or "").strip() if body.custom_sql else None
        if custom_sql and body.transform_type == "dbt":
            if body.source_type in ("source-mongodb-v2", "mongodb"):
                raise HTTPException(
                    status_code=400,
                    detail="Custom SQL is only supported for Postgres source. Use Field Mapping for MongoDB.",
                )

        common_kw = {
            "organization_id": body.organization_id,
            "dataset_name": body.dataset_name,
            "dest_schema": body.dest_schema,
            "custom_sql": custom_sql,
        }
        # incremental and cdc both use log-based (Postgres) or cursor-based (MongoDB) incremental
        is_incremental = body.sync_mode in ("incremental", "cdc")
        # Postgres uses WAL (log-based), no cursor; MongoDB uses cursor_field
        cursor_field = (
            (body.cursor_field or "_id")
            if body.source_type in ("source-mongodb-v2", "mongodb")
            else ""
        )
        if is_incremental:
            result = dlt_runner.run_incremental_sync(
                source_type=body.source_type,
                source_config=body.source_config,
                dest_config=body.dest_config,
                source_stream=body.source_stream,
                dest_table=dest_table,
                cursor_field=cursor_field,
                pipeline_id=body.pipeline_id,
                write_mode=body.write_mode,
                upsert_key=body.upsert_key,
                initial_state=body.initial_state,
                column_map=body.column_map,
                **common_kw,
            )
        else:
            result = dlt_runner.run_full_sync(
                source_type=body.source_type,
                source_config=body.source_config,
                dest_config=body.dest_config,
                source_stream=body.source_stream,
                dest_table=dest_table,
                pipeline_id=body.pipeline_id,
                write_mode=body.write_mode,
                upsert_key=body.upsert_key,
                column_map=body.column_map,
                **common_kw,
            )

        return {
            "rows_synced": result.get("rows_synced", 0),
            "sync_mode": result.get("sync_mode", body.sync_mode),
            "new_cursor": result.get("new_cursor"),
            "new_state": result.get("new_state"),
        }
    except HTTPException:
        raise
    except Exception as e:
        return {
            "rows_synced": 0,
            "sync_mode": body.sync_mode,
            "error": str(e),
            "user_message": str(e),
        }
