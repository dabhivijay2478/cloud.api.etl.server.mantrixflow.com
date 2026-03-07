"""Test connection — POST /test-connection.

Spawns tap-postgres --test to verify source DB connectivity.
"""

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from core.singer_runner import run_test_connection

logger = logging.getLogger("etl.test_connection")
router = APIRouter()


class TestConnectionRequest(BaseModel):
    source_type: str | None = None
    connection_config: dict | None = None
    source_config: dict | None = None


@router.post("/test-connection")
async def test_connection(body: TestConnectionRequest):
    config = body.connection_config or body.source_config or {}
    if not config:
        raise HTTPException(status_code=400, detail="connection_config is required")

    host = config.get("host", "?")
    port = config.get("port", "?")
    dbname = config.get("dbname", config.get("database", "?"))
    logger.info("Testing connection to %s:%s/%s", host, port, dbname)

    result = await run_test_connection(config)

    if result.get("success"):
        logger.info("Connection test PASSED for %s:%s/%s", host, port, dbname)
    else:
        logger.warning(
            "Connection test FAILED for %s:%s/%s: %s",
            host, port, dbname, result.get("error", "unknown"),
        )

    return result
