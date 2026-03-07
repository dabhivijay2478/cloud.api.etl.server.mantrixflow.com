"""Schema discovery — POST /discover.

Spawns tap-postgres --discover, parses the catalog into a stream list
with column info, primary keys, and LOG_BASED eligibility.

Retries once on transient connection/DNS errors (e.g. brief network blip).
"""

import asyncio
import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from core.singer_runner import run_discover
from core.catalog_builder import parse_discovered_streams

logger = logging.getLogger("etl.discover")
router = APIRouter()

DISCOVER_RETRY_DELAY_SEC = 2


def _user_friendly_connection_error(exc: BaseException) -> str | None:
    """Return a user-friendly message for common connection/DNS errors, or None."""
    msg = str(exc).lower()
    if "could not translate host name" in msg or "nodename nor servname" in msg:
        return (
            "Cannot resolve database hostname. Check that the host is correct "
            "(e.g. db.<project>.supabase.co for Supabase), the database is not paused, "
            "and the ETL server has network access to reach the host."
        )
    if "connection refused" in msg or "connection reset" in msg:
        return "Connection refused. Check host, port, and firewall settings."
    if "timeout" in msg or "timed out" in msg:
        return "Connection timed out. Check network connectivity and firewall."
    if "password authentication failed" in msg:
        return "Authentication failed. Check username and password."
    if "no such table" in msg or "relation does not exist" in msg:
        return "Table or schema not found. Verify the database and schema names."
    return None


class DiscoverRequest(BaseModel):
    connection_config: dict
    schema_name: str | None = None
    source_type: str | None = None


@router.post("/discover")
async def discover(body: DiscoverRequest):
    if not body.connection_config:
        raise HTTPException(status_code=400, detail="connection_config is required")

    source_type = (body.source_type or "postgres").lower()
    if source_type in ("source-postgres", "postgresql", "pgvector", "redshift"):
        source_type = "postgres"
    if source_type != "postgres":
        raise HTTPException(
            status_code=400,
            detail="Only PostgreSQL sources are supported",
        )

    host = body.connection_config.get("host", "?")
    dbname = body.connection_config.get("dbname", body.connection_config.get("database", "?"))
    logger.info(
        "Discovering schema for %s/%s (filter: %s, source_type: %s)",
        host, dbname, body.schema_name or "all", source_type,
    )

    raw_catalog = None
    last_exc: RuntimeError | None = None
    for attempt in range(2):
        try:
            raw_catalog = await run_discover(body.connection_config, source_type=source_type)
            break
        except RuntimeError as exc:
            last_exc = exc
            if attempt == 0 and _user_friendly_connection_error(exc):
                logger.warning(
                    "Discovery attempt %d failed for %s/%s, retrying in %ds: %s",
                    attempt + 1, host, dbname, DISCOVER_RETRY_DELAY_SEC, exc,
                )
                await asyncio.sleep(DISCOVER_RETRY_DELAY_SEC)
            else:
                break

    if raw_catalog is None and last_exc:
        logger.error("Discovery failed for %s/%s: %s", host, dbname, last_exc)
        detail = _user_friendly_connection_error(last_exc) or str(last_exc)
        raise HTTPException(status_code=500, detail=detail)

    streams = parse_discovered_streams(raw_catalog)

    if body.schema_name:
        streams = [
            s for s in streams
            if body.schema_name in s["stream_name"] or body.schema_name in s["tap_stream_id"]
        ]

    logger.info(
        "Discovered %d stream(s) for %s/%s",
        len(streams), host, dbname,
    )

    return {"streams": streams, "raw_catalog": raw_catalog}
