"""RunConfig — POST /sync request body."""

from __future__ import annotations

from pydantic import BaseModel


class RunConfig(BaseModel):
    """Configuration for a pipeline sync run."""

    run_id: str
    pipeline_id: str
    org_id: str
    connector_type: str
    replication_method: str

    # Source credentials (already decrypted by NestJS)
    source_host: str
    source_port: int
    source_user: str
    source_password: str
    source_database: str
    source_schema: str = "public"
    source_ssl_mode: str = "require"

    # Destination credentials (already decrypted)
    dest_host: str
    dest_port: int = 5432
    dest_user: str
    dest_password: str
    dest_database: str
    dest_schema: str = "public"
    dest_ssl_mode: str = "require"

    # What to sync
    selected_streams: list[str]
    stream_configs: dict = {}

    # Incremental state
    replication_key: str | None = None
    last_state: dict | None = None

    # CDC (LOG_BASED)
    replication_slot_name: str | None = None
    replication_pub_name: str | None = None

    # How to write
    emit_method: str = "merge"

    # Transform
    transform_script: str | None = None
    on_transform_error: str = "fail"

    # Performance
    dlt_backend: str = "sqlalchemy"
    chunk_size: int = 10000

    # Optional override for callback URL (legacy payloads)
    callback_url: str | None = None
