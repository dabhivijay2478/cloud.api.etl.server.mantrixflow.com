"""Build SQLAlchemy connection strings from connector type and credentials."""

from __future__ import annotations

from urllib.parse import quote_plus

from core.connector_support import normalize_dest_type, normalize_source_type

DIALECT_MAP = {
    "postgres": "postgresql+psycopg2",
    "mysql": "mysql+pymysql",
    "mariadb": "mariadb+pymysql",
    "mssql": "mssql+pyodbc",
    "oracle": "oracle+cx_oracle",
    "sqlite": "sqlite",
    "cockroachdb": "cockroachdb+psycopg2",
}


def _get(creds: dict, *keys: str, default=None):
    for key in keys:
        if key in creds and creds[key] not in (None, ""):
            return creds[key]
    return default


def build_connection_string(connector_type: str, creds: dict, *, role: str = "source") -> str:
    """
    Build SQLAlchemy URL from connector_type and credential dict.
    NEVER log the returned string — it contains the password.
    """
    normalized = (
        normalize_source_type(connector_type)
        if role == "source"
        else normalize_dest_type(connector_type)
    )

    if normalized == "sqlite":
        database = _get(creds, "database", "dbname", "path", default="")
        if not database:
            raise ValueError("SQLite connections require a database/path value")
        if str(database).startswith("sqlite:"):
            return str(database)
        if str(database) == ":memory:":
            return "sqlite:///:memory:"
        return f"sqlite:///{database}"

    dialect = DIALECT_MAP.get(normalized)
    if not dialect:
        raise ValueError(f"Unsupported connector type: {normalized}")

    user = quote_plus(str(_get(creds, "user", "username", default="")).strip())
    password = quote_plus(str(_get(creds, "password", "pass", default="")))
    host = str(_get(creds, "host", "hostname", default="localhost")).strip()
    port = _get(creds, "port")
    database = quote_plus(str(_get(creds, "database", "dbname", default="")).strip())

    auth = f"{user}:{password}" if password else user
    host_part = f"{host}:{int(port)}" if port not in (None, "") else host

    if normalized == "mssql":
        driver = _get(creds, "driver", default="ODBC Driver 17 for SQL Server")
        query = f"?driver={quote_plus(str(driver))}"
        return f"{dialect}://{auth}@{host_part}/{database}{query}"

    query_parts = []
    ssl_mode = _get(creds, "ssl_mode", "sslmode")
    if ssl_mode and normalized in {"postgres", "cockroachdb"}:
        query_parts.append(f"sslmode={quote_plus(str(ssl_mode))}")
    query = f"?{'&'.join(query_parts)}" if query_parts else ""
    return f"{dialect}://{auth}@{host_part}/{database}{query}"


def build_connection_string_safe(connector_type: str, creds: dict, *, role: str = "source") -> str:
    """Same as build_connection_string but returns URL with password replaced by ***."""
    conn_str = build_connection_string(connector_type, creds, role=role)
    if "password" in creds and creds["password"]:
        conn_str = conn_str.replace(str(creds["password"]), "***")
    return conn_str
