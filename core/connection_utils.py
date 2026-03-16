"""Shared connection, discovery, and naming helpers for the dlt ETL service."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Iterable
from urllib.parse import quote_plus

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

from core.connector_support import normalize_dest_type, normalize_source_type

logger = logging.getLogger("etl.connection")

DEFAULT_SQL_SCHEMA = "public"

SQLALCHEMY_URL_FIELDS = (
    "sqlalchemy_url",
    "connection_url",
    "database_url",
    "url",
)

SQLALCHEMY_DIALECTS: dict[str, str] = {
    "postgres": "postgresql+psycopg2",
    "mysql": "mysql+pymysql",
    "mariadb": "mariadb+pymysql",
    "sqlite": "sqlite",
    "mssql": "mssql+pyodbc",
    "oracle": "oracle+cx_oracle",
    "cockroachdb": "cockroachdb+psycopg2",
}

SYSTEM_SCHEMAS: dict[str, set[str]] = {
    "postgres": {"information_schema", "pg_catalog", "pg_toast"},
    "mysql": {"information_schema", "mysql", "performance_schema", "sys"},
    "mariadb": {"information_schema", "mysql", "performance_schema", "sys"},
    "mssql": {"information_schema", "sys"},
}

TIMESTAMPISH_COLUMN_TOKENS = (
    "updated_at",
    "updatedat",
    "modified_at",
    "modifiedat",
    "created_at",
    "createdat",
    "_id",
    "id",
)


@dataclass(frozen=True)
class StreamRef:
    namespace: str | None
    name: str


def normalize_connector_type(
    connector_type: str | None,
    *,
    role: str = "source",
) -> str:
    if role == "dest":
        return normalize_dest_type(connector_type)
    return normalize_source_type(connector_type)


def parse_stream_name(
    stream_name: str,
    *,
    default_namespace: str | None = None,
) -> StreamRef:
    raw = (stream_name or "").strip()
    if not raw:
        raise ValueError("source_stream is required")

    if "-" in raw:
        namespace, name = raw.rsplit("-", 1)
        return StreamRef(namespace=namespace or default_namespace, name=name)

    if "." in raw:
        namespace, name = raw.split(".", 1)
        return StreamRef(namespace=namespace or default_namespace, name=name)

    return StreamRef(namespace=default_namespace, name=raw)


def _get(
    config: dict[str, Any],
    *keys: str,
    default: Any = None,
) -> Any:
    for key in keys:
        if key in config and config[key] not in (None, ""):
            return config[key]
    return default


def _as_bool(value: Any, default: bool | None = None) -> bool | None:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on", "enabled", "require"}:
            return True
        if normalized in {"0", "false", "no", "off", "disabled"}:
            return False
    return default


def _parse_ssl_mode(config: dict[str, Any]) -> str | None:
    ssl = config.get("ssl")
    ssl_mode = _get(config, "ssl_mode", "sslmode")
    if isinstance(ssl_mode, str) and ssl_mode.strip():
        return ssl_mode.strip()

    parsed = _as_bool(ssl)
    if parsed is True:
        return "require"
    if parsed is False:
        return "disable"

    if isinstance(ssl, dict):
        if ssl.get("mode"):
            return str(ssl["mode"])
        if ssl.get("enabled") is True or ssl.get("require") is True:
            return "require"
        if ssl.get("enabled") is False or ssl.get("require") is False:
            return "disable"

    return None


def _scrub_passwords(message: str, config: dict[str, Any] | None = None) -> str:
    scrubbed = re.sub(r"(://[^:/@\s]+:)([^@/\s]+)(@)", r"\1***\3", message)

    if config:
        password = _get(config, "password", "pass", default="")
        if isinstance(password, str) and password:
            scrubbed = scrubbed.replace(password, "***")

        conn_string = _get(config, "connection_string", default="")
        if isinstance(conn_string, str) and conn_string:
            scrubbed = scrubbed.replace(conn_string, re.sub(r":[^:@/]+@", ":***@", conn_string))

    return scrubbed


def build_sqlalchemy_url(
    connector_type: str | None,
    config: dict[str, Any],
    *,
    role: str = "source",
) -> str:
    normalized = normalize_connector_type(connector_type, role=role)

    for key in SQLALCHEMY_URL_FIELDS:
        existing = _get(config, key)
        if isinstance(existing, str) and existing.strip():
            return existing.strip()

    if normalized == "sqlite":
        database = _get(config, "database", "dbname", "path", "file", "filename")
        if not database:
            raise ValueError("SQLite connections require a database/path value")
        if str(database).startswith("sqlite:"):
            return str(database)
        if str(database) == ":memory:":
            return "sqlite:///:memory:"
        if str(database).startswith("/"):
            return f"sqlite:////{str(database).lstrip('/')}"
        return f"sqlite:///{database}"

    dialect = SQLALCHEMY_DIALECTS.get(normalized)
    if not dialect:
        raise ValueError(
            f"Unsupported SQL connector type '{normalized}'. Provide a full SQLAlchemy URL "
            "in connection_config.sqlalchemy_url to use custom dialects."
        )

    username = quote_plus(str(_get(config, "username", "user", default="")).strip())
    password = quote_plus(str(_get(config, "password", "pass", default="")))
    host = str(_get(config, "host", "hostname", default="localhost")).strip()
    port = _get(config, "port")
    database = quote_plus(str(_get(config, "database", "dbname", default="")).strip())

    auth = username
    if password:
        auth = f"{auth}:{password}"
    elif auth:
        auth = f"{auth}"

    host_part = host
    if port not in (None, ""):
        host_part = f"{host_part}:{int(port)}"

    if normalized == "mssql":
        driver = _get(config, "driver", "odbc_driver", default="ODBC Driver 18 for SQL Server")
        query_parts = [f"driver={quote_plus(str(driver))}"]
        if _get(config, "trust_server_certificate") is not None:
            query_parts.append(
                f"TrustServerCertificate={'yes' if _as_bool(config.get('trust_server_certificate')) else 'no'}"
            )
        return f"{dialect}://{auth}@{host_part}/{database}?{'&'.join(query_parts)}"

    query_parts: list[str] = []
    ssl_mode = _parse_ssl_mode(config)
    if ssl_mode and normalized in {"postgres", "cockroachdb"}:
        query_parts.append(f"sslmode={quote_plus(ssl_mode)}")
    connect_timeout = _get(config, "connect_timeout", "connection_timeout")
    if connect_timeout not in (None, "") and normalized in {"postgres", "mysql", "mariadb"}:
        key = "connect_timeout" if normalized in {"mysql", "mariadb"} else "connect_timeout"
        query_parts.append(f"{key}={int(connect_timeout)}")

    query = f"?{'&'.join(query_parts)}" if query_parts else ""
    return f"{dialect}://{auth}@{host_part}/{database}{query}"


def create_sqlalchemy_engine(
    connector_type: str | None,
    config: dict[str, Any],
    *,
    role: str = "source",
):
    url = build_sqlalchemy_url(connector_type, config, role=role)
    return create_engine(url, pool_pre_ping=True)


def test_sql_connection(
    connector_type: str | None,
    config: dict[str, Any],
) -> dict[str, Any]:
    try:
        engine = create_sqlalchemy_engine(connector_type, config)
        with engine.connect() as connection:
            connection.exec_driver_sql("SELECT 1")
            dialect = connection.engine.dialect
            version_info = getattr(dialect, "server_version_info", None)
            database_name = _get(config, "database", "dbname")
            return {
                "success": True,
                "database": database_name,
                "server_version": ".".join(str(part) for part in version_info)
                if version_info
                else None,
            }
    except (SQLAlchemyError, ValueError) as exc:
        return {"success": False, "error": _scrub_passwords(str(exc), config)}


def _filter_schema_names(schema_names: Iterable[str], connector_type: str | None) -> list[str]:
    normalized = normalize_connector_type(connector_type)
    excluded = SYSTEM_SCHEMAS.get(normalized, set())
    result = [schema for schema in schema_names if schema not in excluded]
    return result or list(schema_names)


def _coerce_sql_type(column_type: Any) -> str:
    return str(column_type).strip().lower() or "text"


def _to_json_schema_type(sql_type: str, nullable: bool) -> str | list[str]:
    if not nullable:
        return sql_type
    return [sql_type, "null"]


def _replication_key_candidates(
    columns: list[dict[str, Any]],
    primary_keys: list[str],
) -> list[str]:
    candidates: list[str] = []

    for column in columns:
        name = str(column["name"]).lower()
        sql_type = str(column["type"]).lower()
        if name in TIMESTAMPISH_COLUMN_TOKENS or name.endswith("_at"):
            candidates.append(column["name"])
            continue
        if column["name"] in primary_keys and any(token in sql_type for token in ("int", "numeric", "decimal")):
            candidates.append(column["name"])

    ordered = []
    seen = set()
    for candidate in candidates:
        if candidate not in seen:
            ordered.append(candidate)
            seen.add(candidate)
    return ordered


def discover_sql_schema(
    connector_type: str | None,
    config: dict[str, Any],
    *,
    schema_name: str | None = None,
) -> dict[str, Any]:
    engine = create_sqlalchemy_engine(connector_type, config)
    inspector = inspect(engine)

    if schema_name:
        schema_names = [schema_name]
    else:
        schema_names = _filter_schema_names(inspector.get_schema_names(), connector_type)

    database_name = (
        _get(config, "database", "dbname")
        or _get(config, "path", "file")
        or normalize_connector_type(connector_type)
    )

    streams: list[dict[str, Any]] = []
    schemas: list[dict[str, Any]] = []

    for current_schema in schema_names:
        table_names = inspector.get_table_names(schema=current_schema)
        tables: list[dict[str, Any]] = []

        for table_name in table_names:
            raw_columns = inspector.get_columns(table_name, schema=current_schema)
            pk_info = inspector.get_pk_constraint(table_name, schema=current_schema) or {}
            primary_keys = list(pk_info.get("constrained_columns") or [])
            indexes = [
                idx.get("name")
                for idx in inspector.get_indexes(table_name, schema=current_schema)
                if idx.get("name")
            ]

            columns: list[dict[str, Any]] = []
            schema_properties: dict[str, Any] = {}
            for column in raw_columns:
                column_type = _coerce_sql_type(column.get("type"))
                nullable = bool(column.get("nullable", True))
                column_name = str(column.get("name"))
                columns.append(
                    {
                        "name": column_name,
                        "type": column_type,
                        "nullable": nullable,
                        "primary_key": column_name in primary_keys,
                    }
                )
                schema_properties[column_name] = {"type": _to_json_schema_type(column_type, nullable)}

            candidates = _replication_key_candidates(columns, primary_keys)
            replication_methods = ["FULL_TABLE"]
            if candidates:
                replication_methods.append("INCREMENTAL")
            if normalize_connector_type(connector_type) == "postgres":
                replication_methods.append("LOG_BASED")

            stream_name = f"{current_schema}-{table_name}" if current_schema else table_name
            table_info = {
                "name": table_name,
                "row_count_estimate": None,
                "columns": columns,
                "primary_keys": primary_keys,
                "indexes": indexes,
                "replication_key_candidates": candidates,
            }
            tables.append({"name": table_name, "schema": current_schema, "type": "table"})
            streams.append(
                {
                    "stream_name": stream_name,
                    "tap_stream_id": stream_name,
                    "stream": stream_name,
                    "schema": {"properties": schema_properties},
                    "columns": [{**column, "table": table_name} for column in columns],
                    "key_properties": primary_keys,
                    "primary_keys": primary_keys,
                    "indexes": indexes,
                    "replication_key_candidates": candidates,
                    "replication_methods": replication_methods,
                    "log_based_eligible": "LOG_BASED" in replication_methods,
                    "incremental_eligible": "INCREMENTAL" in replication_methods,
                    "table_info": table_info,
                }
            )

        schemas.append({"name": current_schema, "tables": tables})

    return {
        "streams": streams,
        "schemas": schemas,
        "databases": [{"name": database_name, "schemas": schemas}],
        "raw_catalog": {"streams": streams},
    }
