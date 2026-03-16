"""Test connection — POST /test-connection."""

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from core.connection_utils import test_sql_connection
from core.connector_support import normalize_source_type

logger = logging.getLogger("etl.test_connection")
router = APIRouter()


class TestConnectionRequest(BaseModel):
    source_type: str | None = None
    connection_config: dict | None = None


@router.post("/test-connection")
async def test_connection(body: TestConnectionRequest):
    config = body.connection_config or {}
    if not config:
        raise HTTPException(status_code=400, detail="connection_config is required")

    try:
        source_type = normalize_source_type(body.source_type)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    host = config.get("host", "?")
    port = config.get("port", "?")
    dbname = config.get("dbname", config.get("database", "?"))
    logger.info("Testing connection to %s:%s/%s (source_type: %s)", host, port, dbname, source_type)

    result = test_sql_connection(source_type, config)

    if result.get("success"):
        logger.info("Connection test PASSED for %s:%s/%s", host, port, dbname)
    else:
        logger.warning(
            "Connection test FAILED for %s:%s/%s: %s",
            host, port, dbname, result.get("error", "unknown"),
        )

    return result
