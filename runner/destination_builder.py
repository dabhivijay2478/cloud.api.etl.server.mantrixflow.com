"""Build dlt destination for PostgreSQL."""

from __future__ import annotations

import dlt


def build_postgres_destination(creds: dict) -> Any:
    """
    Returns dlt.destinations.postgres destination.
    Never logs the connection string.
    """
    user = creds.get("user", creds.get("username", ""))
    password = creds.get("password", "")
    host = creds.get("host", "localhost")
    port = creds.get("port", 5432)
    database = creds.get("database", creds.get("dbname", ""))
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return dlt.destinations.postgres(credentials=conn_str)
