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
    transform_script: Optional[str] = None
    initial_state: Optional[dict] = None
    dataset_name: Optional[str] = None
    dest_schema: Optional[str] = None
    destination_table_exists: Optional[bool] = None
    custom_sql: Optional[str] = None
    transform_type: Optional[str] = None
    dbt_model: Optional[str] = None
    selected_columns: Optional[List[str]] = None


@router.post("/run-sync")
def run_sync(body: RunSyncRequest):
    """Run Collect → Transform → Emit via dlt."""
    try:
        if not body.source_stream:
            raise HTTPException(status_code=400, detail="source_stream is required")
        dest_table = body.dest_table or body.source_stream.split(".")[-1]

        if body.destination_table_exists:
            dest_schema = body.dest_schema or "public"
            if not dlt_runner.verify_destination_table_exists(
                body.dest_config, dest_schema, dest_table
            ):
                raise HTTPException(
                    status_code=400,
                    detail="Destination table does not exist. Sync only supports existing tables.",
                )

        effective_transform_type = (body.transform_type or "dlt").lower()
        custom_sql = (body.custom_sql or "").strip() or None

        if effective_transform_type == "dbt" and custom_sql:
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
            "destination_table_exists": body.destination_table_exists or False,
            "transform_script": (body.transform_script or "").strip() or None,
            "transform_type": effective_transform_type,
            "selected_columns": body.selected_columns,
        }

        is_incremental = body.sync_mode in ("incremental", "cdc")
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
