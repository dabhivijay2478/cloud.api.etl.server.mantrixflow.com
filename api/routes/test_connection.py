"""Test connection — POST /test-connection."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, Any

router = APIRouter()


class TestConnectionRequest(BaseModel):
    source_type: Optional[str] = None
    source_config: Optional[dict] = None
    connection_config: Optional[dict] = None


def _build_postgres_conn_str(config: dict) -> str:
    if config.get("connection_string"):
        return config["connection_string"]
    host = config.get("host", "localhost")
    port = config.get("port", 5432)
    database = config.get("database", "postgres")
    username = config.get("username", "postgres")
    password = config.get("password", "")
    return f"postgresql://{username}:{password}@{host}:{port}/{database}"


def _build_mongo_conn_url(config: dict) -> str:
    if config.get("connection_string") or config.get("connection_string_mongo"):
        return config.get("connection_string_mongo") or config.get("connection_string", "")
    host = config.get("host", "localhost")
    port = config.get("port", 27017)
    database = config.get("database", "admin")
    username = config.get("username", "")
    password = config.get("password", "")
    if username and password:
        return f"mongodb://{username}:{password}@{host}:{port}/{database}"
    return f"mongodb://{host}:{port}/{database}"


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
            conn_str = _build_postgres_conn_str(config)
            engine = create_engine(conn_str)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return {"success": True, "message": "Connected"}
        if "mongo" in source_type:
            import pymongo
            conn_url = _build_mongo_conn_url(config)
            client = pymongo.MongoClient(conn_url, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            return {"success": True, "message": "Connected"}
        raise HTTPException(status_code=400, detail=f"Unsupported source type: {body.source_type}")
    except HTTPException:
        raise
    except Exception as e:
        return {"success": False, "message": str(e)}
