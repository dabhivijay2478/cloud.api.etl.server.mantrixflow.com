"""Shared connection helpers — Postgres only (MongoDB removed)."""

from __future__ import annotations

from urllib.parse import quote_plus


def build_postgres_conn_str(config: dict) -> str:
    """Build a SQLAlchemy-compatible connection string for quick test-connection fallback."""
    host = config.get("host") or config.get("hostname", "localhost")
    port = config.get("port", 5432)
    database = config.get("database", "postgres")
    username = config.get("username") or config.get("user", "postgres")
    password = config.get("password", "")

    if password and ("@" in password or ":" in password):
        password = quote_plus(password)

    base = f"postgresql://{username}:{password}@{host}:{port}/{database}"

    ssl = config.get("ssl") or {}
    if isinstance(ssl, dict) and ssl.get("enabled"):
        base = f"{base}?sslmode=require"

    return base
