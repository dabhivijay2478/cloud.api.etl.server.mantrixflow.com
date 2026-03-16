"""CallbackPayload — sent to NestJS after run completes."""

from __future__ import annotations

from pydantic import BaseModel


class CallbackPayload(BaseModel):
    """Payload posted to NestJS callback endpoint."""

    run_id: str
    status: str
    rows_written: int = 0
    rows_dropped: int = 0
    rows_failed: int = 0
    duration_seconds: float = 0.0
    last_cursor_value: dict | None = None
    schema_evolutions: int = 0
    error_message: str | None = None
    source_tool: str = "dlt_sql_postgres"
    dest_tool: str = "dlt_dest_postgres"
