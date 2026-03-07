"""Introspect table — POST /introspect-table.

Returns actual PostgreSQL column types, identity info, and constraints
for an existing table. Uses psycopg directly (not Singer) so the caller
gets real PG types instead of Singer JSON Schema types.
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

logger = logging.getLogger("etl.introspect")
router = APIRouter()

INTROSPECT_SQL = """\
SELECT
    c.column_name,
    c.data_type,
    c.udt_name,
    c.is_nullable,
    c.column_default,
    c.is_identity,
    c.identity_generation,
    c.character_maximum_length,
    c.numeric_precision,
    c.numeric_scale
FROM information_schema.columns c
WHERE c.table_schema = %s
  AND c.table_name   = %s
ORDER BY c.ordinal_position
"""


class IntrospectRequest(BaseModel):
    connection_config: dict[str, Any]
    schema_name: str = "public"
    table_name: str


def _build_dsn(conn: dict[str, Any]) -> str:
    host = conn.get("host") or conn.get("hostname", "localhost")
    port = int(conn.get("port", 5432))
    user = conn.get("user") or conn.get("username", "postgres")
    password = conn.get("password", "")
    database = conn.get("database") or conn.get("dbname", "postgres")

    ssl = conn.get("ssl")
    needs_ssl = False
    if isinstance(ssl, bool):
        needs_ssl = ssl
    elif isinstance(ssl, str) and str(ssl).lower() in ("true", "1", "yes"):
        needs_ssl = True
    elif isinstance(ssl, dict):
        needs_ssl = bool(ssl.get("enabled") or ssl.get("require"))

    sslmode = "require" if needs_ssl else "prefer"
    return (
        f"postgresql://{user}:{password}@{host}:{port}/{database}"
        f"?sslmode={sslmode}"
    )


@router.post("/introspect-table")
async def introspect_table(body: IntrospectRequest):
    dsn = _build_dsn(body.connection_config)
    masked_host = body.connection_config.get("host", "?")
    logger.info(
        "Introspecting %s.%s on %s",
        body.schema_name,
        body.table_name,
        masked_host,
    )

    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(INTROSPECT_SQL, (body.schema_name, body.table_name))
                rows = cur.fetchall()
    except Exception as exc:
        logger.error("Introspect failed for %s.%s: %s", body.schema_name, body.table_name, exc)
        raise HTTPException(status_code=500, detail=f"Database introspection failed: {exc}")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Table {body.schema_name}.{body.table_name} not found or has no columns",
        )

    columns = []
    for row in rows:
        (
            col_name, data_type, udt_name, is_nullable,
            col_default, is_identity, identity_generation,
            char_max_len, numeric_precision, numeric_scale,
        ) = row

        columns.append({
            "name": col_name,
            "data_type": data_type,
            "udt_name": udt_name,
            "is_nullable": is_nullable == "YES",
            "has_default": col_default is not None,
            "is_identity": is_identity == "YES",
            "identity_generation": identity_generation or None,
            "character_maximum_length": char_max_len,
            "numeric_precision": numeric_precision,
            "numeric_scale": numeric_scale,
        })

    logger.info(
        "Introspected %s.%s: %d column(s)",
        body.schema_name,
        body.table_name,
        len(columns),
    )

    return {"columns": columns}
