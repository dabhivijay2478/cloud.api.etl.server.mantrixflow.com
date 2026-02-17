"""Map generic connection_config to Meltano environment variables and tap config.

Meltano env format: MELTANO_EXTRACTOR_<PLUGIN>_<SETTING> (e.g. MELTANO_EXTRACTOR_TAP_POSTGRES_SQLALCHEMY_URL)
"""

from __future__ import annotations

from typing import Any, Dict, Literal, Optional
from urllib.parse import parse_qs, quote_plus, unquote_plus, urlparse

from utils import ensure_supported_source


def _parse_mongo_uri(uri: str) -> Dict[str, Any]:
    """Parse a MongoDB connection string into individual config fields for Singer tap.

    Handles both ``mongodb://`` and ``mongodb+srv://`` schemes using stdlib only.
    """
    parsed_url = urlparse(uri)
    result: Dict[str, Any] = {
        "host": parsed_url.hostname or "localhost",
        "port": parsed_url.port or 27017,
        "user": unquote_plus(parsed_url.username or ""),
        "password": unquote_plus(parsed_url.password or ""),
        "database": (parsed_url.path.lstrip("/") or "admin"),
        "ssl": "true" if uri.startswith("mongodb+srv://") else "false",
    }
    if parsed_url.query:
        opts = parse_qs(parsed_url.query)
        if opts.get("replicaSet"):
            result["replica_set"] = opts["replicaSet"][0]
        if opts.get("authSource"):
            result["auth_source"] = opts["authSource"][0]
    if uri.startswith("mongodb+srv://"):
        result["verify_mode"] = "false"
    return result

Role = Literal["extractor", "loader"]

# Plugin names as used in meltano.yml (extractors)
SOURCE_TYPE_TO_TAP: Dict[str, str] = {
    "postgresql": "tap-postgres",
    "mysql": "tap-mysql",
    "mongodb": "tap-mongodb",
}

# Loader plugin names
SOURCE_TYPE_TO_LOADER: Dict[str, str] = {
    "postgresql": "target-postgres",
    "mysql": "target-mysql",
    "mongodb": "target-mongodb",
}


def _plugin_name_to_env_prefix(plugin_name: str) -> str:
    """Convert tap-postgres -> TAP_POSTGRES for env var prefix."""
    return plugin_name.upper().replace("-", "_")


def _build_postgres_sqlalchemy_url(conn: Dict[str, Any]) -> str:
    """Build PostgreSQL SQLAlchemy URL from connection_config."""
    url = conn.get("connection_string")
    if url:
        return str(url)

    user = conn.get("username") or conn.get("user")
    password = conn.get("password")
    host = conn.get("host", "localhost")
    port = int(conn.get("port") or 5432)
    database = conn.get("database") or conn.get("dbname")
    if not database:
        raise ValueError("PostgreSQL connection requires 'database' or 'dbname'")

    auth = ""
    if user:
        auth = quote_plus(str(user))
        if password is not None:
            auth += f":{quote_plus(str(password))}"
        auth += "@"

    return f"postgresql://{auth}{host}:{port}/{database}"


def _build_mysql_sqlalchemy_url(conn: Dict[str, Any]) -> str:
    """Build MySQL SQLAlchemy URL from connection_config."""
    url = conn.get("connection_string")
    if url:
        return str(url)

    user = conn.get("username") or conn.get("user")
    password = conn.get("password")
    host = conn.get("host", "localhost")
    port = int(conn.get("port") or 3306)
    database = conn.get("database") or conn.get("dbname")
    if not database:
        raise ValueError("MySQL connection requires 'database' or 'dbname'")

    auth = ""
    if user:
        auth = quote_plus(str(user))
        if password is not None:
            auth += f":{quote_plus(str(password))}"
        auth += "@"

    return f"mysql+pymysql://{auth}{host}:{port}/{database}"


def _build_mongodb_connection_config(conn: Dict[str, Any]) -> Dict[str, Any]:
    """Build MongoDB config; parse URI if needed. Returns dict of Meltano config keys -> values."""
    mongo_uri = conn.get("connection_string_mongo") or conn.get("connection_string")
    if mongo_uri and not conn.get("host"):
        parsed = _parse_mongo_uri(mongo_uri)
        database = conn.get("database") or conn.get("dbname") or parsed.get("database", "admin")
        return {
            "mongodb_connection_string": mongo_uri,
            "database": database,
        }

    host = conn.get("host", "localhost")
    port = int(conn.get("port") or 27017)
    database = conn.get("database") or conn.get("dbname") or "admin"
    user = conn.get("username") or conn.get("user")
    password = conn.get("password")

    if user and password:
        uri = f"mongodb://{quote_plus(str(user))}:{quote_plus(str(password))}@{host}:{port}/{database}"
    else:
        uri = f"mongodb://{host}:{port}/{database}"

    result: Dict[str, Any] = {
        "mongodb_connection_string": uri,
        "database": database,
    }
    if conn.get("replica_set"):
        result["replica_set"] = conn["replica_set"]
    if conn.get("auth_source") or conn.get("authSource"):
        result["auth_source"] = conn.get("auth_source") or conn.get("authSource")
    return result


def _connection_config_to_extractor_config(
    source_type: str,
    connection_config: Dict[str, Any],
    *,
    sync_mode: Optional[str] = None,
) -> Dict[str, Any]:
    """Convert API connection_config to Meltano extractor config keys/values.

    When sync_mode is 'incremental', sets default_replication_method to LOG_BASED
    (CDC). When 'full' or absent, uses FULL_TABLE.
    """
    source_type = ensure_supported_source(source_type)
    conn = connection_config or {}

    if source_type == "postgresql":
        config = {"sqlalchemy_url": _build_postgres_sqlalchemy_url(conn)}
    elif source_type == "mysql":
        config = {"sqlalchemy_url": _build_mysql_sqlalchemy_url(conn)}
    elif source_type == "mongodb":
        config = _build_mongodb_connection_config(conn)
    else:
        raise ValueError(f"Unsupported source type: {source_type}")

    # Map sync_mode to Meltano default_replication_method
    if sync_mode == "incremental":
        config["default_replication_method"] = "LOG_BASED"
    else:
        config["default_replication_method"] = "FULL_TABLE"

    return config


def connection_config_to_tap_config(
    source_type: str,
    connection_config: Dict[str, Any],
    source_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build tap config for --config file (e.g. collect invoke).

    Merges connection config with source_config overrides (table, schema, query).
    Uses sync_mode from connection_config when present for replication method.
    """
    sync_mode = connection_config.get("sync_mode") or connection_config.get("syncMode")
    config = _connection_config_to_extractor_config(
        source_type, connection_config, sync_mode=sync_mode
    )
    if source_config:
        for key, value in source_config.items():
            if value is not None:
                config[key] = value
    return config


def _connection_config_to_loader_config(
    dest_type: str, connection_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Convert API connection_config to Meltano loader config keys/values."""
    dest_type = ensure_supported_source(dest_type)
    conn = connection_config or {}

    if dest_type == "postgresql":
        return {"sqlalchemy_url": _build_postgres_sqlalchemy_url(conn)}
    if dest_type == "mysql":
        return {"sqlalchemy_url": _build_mysql_sqlalchemy_url(conn)}
    if dest_type == "mongodb":
        return _build_mongodb_connection_config(conn)

    raise ValueError(f"Unsupported destination type: {dest_type}")


def _config_dict_to_env_vars(
    plugin_name: str, config: Dict[str, Any], prefix: str = "MELTANO_EXTRACTOR"
) -> Dict[str, str]:
    """Convert config dict to MELTANO_* env vars. Values must be strings."""
    env_prefix = f"{prefix}_{_plugin_name_to_env_prefix(plugin_name)}"
    env: Dict[str, str] = {}
    for key, value in config.items():
        if value is None:
            continue
        env_key = f"{env_prefix}_{key.upper()}"
        env[env_key] = str(value)
    return env


def connection_config_to_meltano_env(
    source_type: str,
    connection_config: Dict[str, Any],
    role: Role = "extractor",
    plugin_name: str | None = None,
    *,
    sync_mode: Optional[str] = None,
) -> Dict[str, str]:
    """Map generic connection_config to Meltano environment variables.

    Args:
        source_type: One of postgresql, mysql, mongodb.
        connection_config: API-style connection config (host, port, user, password, database, etc.).
        role: 'extractor' or 'loader' (determines plugin type prefix).
        plugin_name: Override plugin name; if None, derived from source_type.

    Returns:
        Dict of env var name -> string value for subprocess environment.
    """
    source_type = ensure_supported_source(source_type)

    if role == "extractor":
        plugin = plugin_name or SOURCE_TYPE_TO_TAP.get(source_type)
        prefix = "MELTANO_EXTRACTOR"
        config = _connection_config_to_extractor_config(
            source_type, connection_config, sync_mode=sync_mode
        )
    else:
        plugin = plugin_name or SOURCE_TYPE_TO_LOADER.get(source_type)
        prefix = "MELTANO_LOADER"
        config = _connection_config_to_loader_config(source_type, connection_config)

    if not plugin:
        raise ValueError(f"No plugin for source_type={source_type} role={role}")

    return _config_dict_to_env_vars(plugin, config, prefix=prefix)


def _connection_config_to_dbt_postgres_env(conn: Dict[str, Any]) -> Dict[str, str]:
    """Build DBT_POSTGRES_* env vars for dbt-postgres utility (same DB as target-postgres)."""
    env: Dict[str, str] = {}
    url = conn.get("connection_string")
    if url:
        parsed = urlparse(url)
        if parsed.hostname:
            env["DBT_POSTGRES_HOST"] = parsed.hostname
        if parsed.port:
            env["DBT_POSTGRES_PORT"] = str(parsed.port)
        if parsed.username:
            env["DBT_POSTGRES_USER"] = parsed.username
        if parsed.password:
            env["DBT_POSTGRES_PASSWORD"] = parsed.password
        if parsed.path and len(parsed.path) > 1:
            env["DBT_POSTGRES_DBNAME"] = parsed.path.lstrip("/").split("/")[0]
        return env

    host = conn.get("host")
    port = conn.get("port")
    user = conn.get("username") or conn.get("user")
    password = conn.get("password")
    database = conn.get("database") or conn.get("dbname")
    schema = conn.get("schema", "public")

    if host is not None:
        env["DBT_POSTGRES_HOST"] = str(host)
    if port is not None:
        env["DBT_POSTGRES_PORT"] = str(port)
    if user is not None:
        env["DBT_POSTGRES_USER"] = str(user)
    if password is not None:
        env["DBT_POSTGRES_PASSWORD"] = str(password)
    if database is not None:
        env["DBT_POSTGRES_DBNAME"] = str(database)
    env["DBT_POSTGRES_SCHEMA"] = str(schema)
    return env


def connection_config_to_meltano_env_for_pipeline(
    source_type: str,
    source_connection_config: Dict[str, Any],
    dest_type: str,
    dest_connection_config: Dict[str, Any],
    *,
    sync_mode: Optional[str] = None,
    dbt_models: Optional[list[str]] = None,
) -> Dict[str, str]:
    """Build combined env vars for a full pipeline (extractor + loader + dbt when dest is postgres).

    Merges extractor, loader, and dbt-postgres env vars when destination is PostgreSQL.
    When sync_mode is 'incremental', sets default_replication_method=LOG_BASED for CDC.
    When dbt_models is provided, sets DBT_SELECT for dbt model selection.
    """
    extractor_env = connection_config_to_meltano_env(
        source_type, source_connection_config, role="extractor", sync_mode=sync_mode
    )
    loader_env = connection_config_to_meltano_env(
        dest_type, dest_connection_config, role="loader"
    )
    result = {**extractor_env, **loader_env}

    if dest_type == "postgresql":
        dbt_env = _connection_config_to_dbt_postgres_env(dest_connection_config)
        result.update(dbt_env)
        if dbt_models and len(dbt_models) > 0:
            # Pass model selection to dbt. Standard dbt does not read DBT_SELECT;
            # a dbt_project.yml var or wrapper can use env_var('DBT_SELECT') to apply --select.
            result["DBT_SELECT"] = " ".join(dbt_models)

    return result
