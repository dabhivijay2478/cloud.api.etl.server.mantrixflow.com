"""Schema discovery — POST /discover-schema/{source_type}."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, Any

from core import dlt_runner

router = APIRouter()


class DiscoverRequest(BaseModel):
    source_type: str
    connection_config: dict
    source_config: Optional[dict] = None
    table_name: Optional[str] = None
    schema_name: Optional[str] = None
    query: Optional[str] = None


@router.post("/discover-schema/{source_type}")
def discover_schema(source_type: str, body: DiscoverRequest):
    """Discover schema (tables/collections, columns) from source."""
    try:
        conn = body.connection_config or body.source_config or {}
        if source_type in ("source-postgres", "postgres", "postgresql"):
            result = dlt_runner.discover_postgres(
                conn, schema_name=body.schema_name or "public"
            )
            if body.table_name:
                result["columns"] = [c for c in result["columns"] if c.get("table") == body.table_name]
        elif source_type in ("source-mongodb-v2", "mongodb"):
            result = dlt_runner.discover_mongodb(
                conn, database=conn.get("database"), collection=body.table_name
            )
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported source type: {source_type}")

        return {
            "columns": result.get("columns", []),
            "primary_keys": result.get("primary_keys", []),
            "estimated_row_count": result.get("estimated_row_count"),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
