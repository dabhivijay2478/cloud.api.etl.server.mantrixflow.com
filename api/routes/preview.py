"""Preview first N rows — POST /preview."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from core import dlt_runner

router = APIRouter()


class PreviewRequest(BaseModel):
    source_type: str
    source_config: dict
    source_stream: str
    limit: Optional[int] = 50


@router.post("/preview")
def preview(body: PreviewRequest):
    """Preview first N rows from source (no write)."""
    try:
        conn = body.source_config
        limit = body.limit or 50
        stream = body.source_stream
        if not stream:
            raise HTTPException(status_code=400, detail="source_stream is required")

        if body.source_type in ("source-postgres", "postgres", "postgresql"):
            result = dlt_runner.preview_postgres(conn, stream, limit)
        elif body.source_type in ("source-mongodb-v2", "mongodb"):
            result = dlt_runner.preview_mongodb(conn, stream, limit)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported source type: {body.source_type}")

        return {
            "records": result.get("records", []),
            "columns": result.get("columns", []),
            "total": result.get("total", 0),
            "stream": result.get("stream", stream),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
