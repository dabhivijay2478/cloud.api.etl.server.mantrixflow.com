"""Token validation for ETL routes."""

from __future__ import annotations

from fastapi import Header, HTTPException

from core.config import settings


def validate_etl_token(x_etl_token: str | None = Header(None, alias="X-ETL-Token")) -> None:
    """Validate X-ETL-Token header. Raises 401 if missing or wrong."""
    if not settings.ETL_INTERNAL_TOKEN:
        return
    if not x_etl_token or x_etl_token != settings.ETL_INTERNAL_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or missing X-ETL-Token")
