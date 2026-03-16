"""Introspect table — POST /introspect-table."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import inspect

from core.connection_utils import create_sqlalchemy_engine

logger = logging.getLogger("etl.introspect")
router = APIRouter()


class IntrospectRequest(BaseModel):
    connection_config: dict[str, Any]
    schema_name: str = "public"
    table_name: str


@router.post("/introspect-table")
async def introspect_table(body: IntrospectRequest):
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
        engine = create_sqlalchemy_engine("postgres", body.connection_config)
        inspector = inspect(engine)
        rows = inspector.get_columns(body.table_name, schema=body.schema_name)
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

    columns = [
        {
            "name": str(column["name"]),
            "data_type": str(column.get("type")).lower(),
            "udt_name": str(column.get("type")).lower(),
            "is_nullable": bool(column.get("nullable", True)),
            "has_default": column.get("default") is not None,
            "is_identity": False,
            "identity_generation": None,
            "character_maximum_length": None,
            "numeric_precision": None,
            "numeric_scale": None,
        }
        for column in rows
    ]

    logger.info(
        "Introspected %s.%s: %d column(s)",
        body.schema_name,
        body.table_name,
        len(columns),
    )

    return {"columns": columns}
