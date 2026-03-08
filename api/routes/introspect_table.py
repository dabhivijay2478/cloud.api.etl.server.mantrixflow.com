"""Introspect table — POST /introspect-table.

Returns actual PostgreSQL column types, identity info, and constraints
for an existing table. Uses psycopg directly (not Singer) so the caller
gets real PG types instead of Singer JSON Schema types.

Connection handling is fully delegated to core.config_builder.build_psycopg_dsn
so SSL, pooler, URL-encoding, and timeout behaviour is identical across all routes.
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg
from core.config_builder import build_psycopg_dsn
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


@router.post("/introspect-table")
async def introspect_table(body: IntrospectRequest):
    dsn = build_psycopg_dsn(body.connection_config)
    host = body.connection_config.get("host") or body.connection_config.get(
        "hostname", "?"
    )

    logger.info(
        "Introspecting %s.%s on %s",
        body.schema_name,
        body.table_name,
        host,
    )

    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(INTROSPECT_SQL, (body.schema_name, body.table_name))
                rows = cur.fetchall()
    except Exception as exc:
        logger.error(
            "Introspect failed for %s.%s on %s: %s",
            body.schema_name,
            body.table_name,
            host,
            exc,
        )
        raise HTTPException(
            status_code=500,
            detail=f"Database introspection failed: {exc}",
        )

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Table {body.schema_name}.{body.table_name} not found "
                "or has no columns"
            ),
        )

    columns = []
    for row in rows:
        (
            col_name,
            data_type,
            udt_name,
            is_nullable,
            col_default,
            is_identity,
            identity_generation,
            char_max_len,
            numeric_precision,
            numeric_scale,
        ) = row

        columns.append(
            {
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
            }
        )

    logger.info(
        "Introspected %s.%s: %d column(s)",
        body.schema_name,
        body.table_name,
        len(columns),
    )

    return {"columns": columns}
