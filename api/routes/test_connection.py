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
    type: str | None = None  # Alias from NestJS
    connection_config: dict | None = None
    source_config: dict | None = None


@router.post("/test-connection")
async def test_connection(body: TestConnectionRequest):
    config = body.connection_config or body.source_config or {}
    if not config:
        raise HTTPException(status_code=400, detail="connection_config is required")

    source_type = (body.source_type or body.type or "postgres").lower()
    if source_type in ("source-postgres", "postgresql", "pgvector", "redshift"):
        source_type = "postgres"
    if source_type != "postgres":
        raise HTTPException(
            status_code=400,
            detail="Only PostgreSQL sources are supported",
        )

    host = config.get("host", "?")
    port = config.get("port", "?")
    dbname = config.get("dbname", config.get("database", "?"))
    logger.info("Testing connection to %s:%s/%s (source_type: %s)", host, port, dbname, source_type)

    result = await run_test_connection(config, source_type=source_type)

    if result.get("success"):
        logger.info("Connection test PASSED for %s:%s/%s", host, port, dbname)
    else:
        logger.warning(
            "Connection test FAILED for %s:%s/%s: %s",
            host, port, dbname, result.get("error", "unknown"),
        )

    return result
