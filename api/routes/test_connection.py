"""Test connection — POST /test-connection."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from config.connections import build_postgres_conn_str, build_mongo_conn_url

router = APIRouter()


class TestConnectionRequest(BaseModel):
    source_type: Optional[str] = None
    source_config: Optional[dict] = None
    connection_config: Optional[dict] = None


@router.post("/test-connection")
def test_connection(body: TestConnectionRequest):
    """Test connection to source."""
    try:
        config = body.source_config or body.connection_config or {}
        if not config:
            raise HTTPException(status_code=400, detail="source_config or connection_config required")

        source_type = (body.source_type or "").lower()
        if "postgres" in source_type:
            from sqlalchemy import create_engine, text
            conn_str = build_postgres_conn_str(config)
            engine = create_engine(conn_str)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return {"success": True, "message": "Connected"}
        if "mongo" in source_type:
            import pymongo
            conn_url = build_mongo_conn_url(config)
            client = pymongo.MongoClient(conn_url, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            return {"success": True, "message": "Connected"}
        raise HTTPException(status_code=400, detail=f"Unsupported source type: {body.source_type}")
    except HTTPException:
        raise
    except Exception as e:
        return {"success": False, "message": str(e)}
