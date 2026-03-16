"""Shared SQL connection helpers."""

from __future__ import annotations

from core.connection_utils import build_sqlalchemy_url, parse_stream_name


def build_postgres_conn_str(config: dict) -> str:
    return build_sqlalchemy_url("postgres", config).replace("postgresql+psycopg2://", "postgresql://", 1)


def parse_stream(stream: str) -> tuple[str, str]:
    ref = parse_stream_name(stream, default_namespace="public")
    return ref.namespace or "public", ref.name
