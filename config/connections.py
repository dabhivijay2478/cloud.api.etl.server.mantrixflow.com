"""
Shared connection builders — single source of truth.
Used by: core.dlt_runner, api.routes.discover, api.routes.preview, api.routes.sync.
Avoids duplicate connection logic.
"""

from typing import Tuple


def build_postgres_conn_str(config: dict) -> str:
    """Build Postgres connection string from config dict.
    Adds sslmode=require when ssl.enabled is true (Neon, managed Postgres).
    """
    if config.get("connection_string"):
        base = config["connection_string"]
    else:
        host = config.get("host", "localhost")
        port = config.get("port", 5432)
        database = config.get("database", "postgres")
        username = config.get("username", "postgres")
        password = config.get("password", "")
        base = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    ssl = config.get("ssl") or {}
    if isinstance(ssl, dict) and ssl.get("enabled"):
        sep = "&" if "?" in base else "?"
        base = f"{base}{sep}sslmode=require"
    return base


def build_mongo_conn_url(config: dict) -> str:
    """Build MongoDB connection URL from config dict."""
    if config.get("connection_string") or config.get("connection_string_mongo"):
        return config.get("connection_string_mongo") or config.get("connection_string", "")
    host = config.get("host", "localhost")
    port = config.get("port", 27017)
    database = config.get("database", "admin")
    username = config.get("username", "")
    password = config.get("password", "")
    if username and password:
        return f"mongodb://{username}:{password}@{host}:{port}/{database}"
    return f"mongodb://{host}:{port}/{database}"


def parse_stream(source_stream: str) -> Tuple[str, str]:
    """
    Parse source_stream (schema.table or database.collection) into (schema_or_db, table_or_coll).
    Returns (schema, table) for SQL; (database, collection) for MongoDB.
    """
    if "." in source_stream:
        parts = source_stream.split(".", 1)
        return parts[0], parts[1]
    return "public", source_stream
