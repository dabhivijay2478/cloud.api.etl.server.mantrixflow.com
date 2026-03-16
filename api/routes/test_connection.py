"""Test connection — POST /test-connection."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from core.connection_utils import test_sql_connection
from core.connector_support import normalize_source_type
from core.security import validate_etl_token

logger = logging.getLogger("etl.test_connection")
router = APIRouter()


class TestConnectionRequest(BaseModel):
    connector_type: str | None = None
    source_type: str | None = None
    connection_config: dict | None = None
    check_cdc: bool = False
    host: str | None = None
    port: int | None = None
    user: str | None = None
    password: str | None = None
    database: str | None = None
    ssl_mode: str | None = None


@router.post("/test-connection", dependencies=[Depends(validate_etl_token)])
async def test_connection(body: TestConnectionRequest):
    config = body.connection_config or {}
    if not config and body.host:
        config = {
            "host": body.host,
            "port": body.port or 5432,
            "username": body.user,
            "user": body.user,
            "password": body.password,
            "database": body.database,
            "dbname": body.database,
            "ssl_mode": body.ssl_mode,
        }
    if not config:
        raise HTTPException(status_code=400, detail="connection_config is required")

    source_type = body.connector_type or body.source_type
    try:
        source_type = normalize_source_type(source_type)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    host = config.get("host", "?")
    port = config.get("port", "?")
    dbname = config.get("dbname", config.get("database", "?"))
    logger.info("Testing connection to %s:%s/%s (source_type: %s)", host, port, dbname, source_type)

    result = test_sql_connection(source_type, config)

    if body.check_cdc and source_type == "postgres" and result.get("success"):
        from core.postgres_admin import verify_wal_level, verify_replication_role
        wal = verify_wal_level(config)
        repl = verify_replication_role(config)
        result["cdc_prerequisites"] = {
            "wal_level": wal.get("wal_level"),
            "wal_level_ok": wal.get("ok"),
            "replication_role_ok": repl.get("ok"),
        }

    if result.get("success"):
        logger.info("Connection test PASSED for %s:%s/%s", host, port, dbname)
    else:
        logger.warning(
            "Connection test FAILED for %s:%s/%s: %s",
            host, port, dbname, result.get("error", "unknown"),
        )

    return result
