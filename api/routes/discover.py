"""Schema discovery — POST /discover."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from core.connection_utils import discover_sql_schema
from core.connector_support import normalize_source_type
from core.security import validate_etl_token

logger = logging.getLogger("etl.discover")
router = APIRouter()

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
    connector_type: str | None = None


@router.post("/discover", dependencies=[Depends(validate_etl_token)])
async def discover(body: DiscoverRequest):
    if not body.connection_config:
        raise HTTPException(status_code=400, detail="connection_config is required")

    try:
        st = body.connector_type or body.source_type
        source_type = normalize_source_type(st)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    schema_name = body.schema_name
    host = body.connection_config.get("host", "?")
    dbname = body.connection_config.get("dbname", body.connection_config.get("database", "?"))
    logger.info(
        "Discovering schema for %s/%s (filter: %s, source_type: %s)",
        host, dbname, schema_name or "all", source_type,
    )

    try:
        result = discover_sql_schema(
            source_type,
            body.connection_config,
            schema_name=schema_name,
        )
    except Exception as exc:
        logger.error("Discovery failed for %s/%s: %s", host, dbname, exc)
        detail = _user_friendly_connection_error(exc) or str(exc)
        raise HTTPException(status_code=500, detail=detail) from exc

    streams = result.get("streams", [])

    logger.info(
        "Discovered %d stream(s) for %s/%s",
        len(streams), host, dbname,
    )

    return result
