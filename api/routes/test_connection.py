"""Test connection — POST /test-connection.

Uses psycopg2 for a fast SELECT 1 check (~1–2s). Avoids tap-postgres subprocess
which does full discovery and can exceed 60s for cloud DBs (Neon cold start).
"""

import asyncio
import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from core.config_builder import resolve_tap_type, test_connection_fast

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

    try:
        source_type = resolve_tap_type(body.source_type or body.type)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    host = config.get("host", "?")
    port = config.get("port", "?")
    dbname = config.get("dbname", config.get("database", "?"))
    logger.info("Testing connection to %s:%s/%s (source_type: %s)", host, port, dbname, source_type)

    result = await asyncio.to_thread(test_connection_fast, config, source_type=source_type)

    if result.get("success"):
        logger.info("Connection test PASSED for %s:%s/%s", host, port, dbname)
    else:
        logger.warning(
            "Connection test FAILED for %s:%s/%s: %s",
            host, port, dbname, result.get("error", "unknown"),
        )

    return result
