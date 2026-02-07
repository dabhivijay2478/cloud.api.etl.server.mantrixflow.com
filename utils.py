"""Shared ETL helpers for Singer execution, state handling, and schema parsing."""

from __future__ import annotations

import copy
import json
import os
import subprocess
import tempfile
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional, Tuple


class ETLRuntimeError(RuntimeError):
    """Raised when a tap execution fails."""


SUPPORTED_SOURCES = {"postgresql", "mysql", "mongodb"}


def normalize_source_type(source_type: str) -> str:
    normalized = source_type.strip().lower()
    if normalized in {"postgres", "postgresql"}:
        return "postgresql"
    if normalized in {"mysql", "mariadb"}:
        return "mysql"
    if normalized in {"mongodb", "mongo"}:
        return "mongodb"
    return normalized


def ensure_supported_source(source_type: str) -> str:
    normalized = normalize_source_type(source_type)
    if normalized not in SUPPORTED_SOURCES:
        raise ValueError(
            f"Unsupported source type '{source_type}'. Supported: {', '.join(sorted(SUPPORTED_SOURCES))}"
        )
    return normalized


def build_singer_config(
    source_type: str,
    connection_config: Dict[str, Any],
    source_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    source_type = ensure_supported_source(source_type)
    source_config = source_config or {}
    conn = connection_config or {}

    cfg: Dict[str, Any] = {}

    # Common relational mapping.
    host = conn.get("host")
    port = conn.get("port")
    username = conn.get("username") or conn.get("user")
    password = conn.get("password")
    database = conn.get("database") or conn.get("dbname")

    if host is not None:
        cfg["host"] = host
    if port is not None:
        cfg["port"] = int(port)
    if username is not None:
        cfg["user"] = username
    if password is not None:
        cfg["password"] = password

    if source_type == "postgresql" and database is not None:
        cfg["dbname"] = database
    if source_type == "mysql" and database is not None:
        cfg["database"] = database
    if source_type == "mongodb" and database is not None:
        cfg["database"] = database

    if conn.get("ssl") is not None:
        ssl_value = conn["ssl"]
        if isinstance(ssl_value, bool):
            cfg["ssl"] = "true" if ssl_value else "false"
        else:
            cfg["ssl"] = str(ssl_value).lower()

    if conn.get("replica_set") is not None:
        cfg["replica_set"] = conn["replica_set"]
    if conn.get("auth_source") is not None:
        cfg["auth_source"] = conn["auth_source"]
    if conn.get("tls") is not None:
        cfg["tls"] = conn["tls"]

    # Pass-through source specific override options.
    for key, value in source_config.items():
        if value is not None:
            cfg[key] = value

    # Allow full connection string input for compatibility.
    if conn.get("connection_string"):
        cfg["connection_string"] = conn["connection_string"]
    if conn.get("connection_string_mongo"):
        cfg["connection_string_mongo"] = conn["connection_string_mongo"]

    return cfg


@contextmanager
def temporary_json_file(payload: Dict[str, Any]):
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp:
        path = tmp.name
        json.dump(payload, tmp)
        tmp.flush()
    try:
        yield path
    finally:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass


def run_command(command: List[str], env: Dict[str, str], timeout_seconds: int) -> str:
    process = subprocess.run(
        command,
        capture_output=True,
        text=True,
        env=env,
        timeout=timeout_seconds,
        check=False,
    )
    if process.returncode != 0:
        stderr = (process.stderr or "").strip()
        stdout = (process.stdout or "").strip()
        message = stderr or stdout or "Unknown tap error"
        raise ETLRuntimeError(message[:4000])
    return process.stdout or ""


def parse_discovery_output(raw_output: str) -> Dict[str, Any]:
    text = raw_output.strip()
    if not text:
        return {"streams": []}

    try:
        data = json.loads(text)
        if isinstance(data, dict) and "streams" in data:
            return data
    except json.JSONDecodeError:
        pass

    # Some taps may include logs in stdout; parse the last valid JSON object.
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for line in reversed(lines):
        try:
            data = json.loads(line)
            if isinstance(data, dict) and "streams" in data:
                return data
        except json.JSONDecodeError:
            continue
    return {"streams": []}


def parse_singer_stream(stdout: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    latest_state: Dict[str, Any] = {}

    for raw in stdout.splitlines():
        line = raw.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue

        message_type = message.get("type")
        if message_type == "RECORD":
            record = message.get("record", {})
            if isinstance(record, dict):
                record["_stream"] = message.get("stream")
                records.append(record)
        elif message_type == "STATE":
            value = message.get("value")
            if isinstance(value, dict):
                latest_state = value

    return records, latest_state


def metadata_root(stream: Dict[str, Any]) -> Dict[str, Any]:
    metadata = stream.get("metadata") or []
    for item in metadata:
        if item.get("breadcrumb") == []:
            item.setdefault("metadata", {})
            return item["metadata"]
    metadata.append({"breadcrumb": [], "metadata": {}})
    stream["metadata"] = metadata
    return metadata[-1]["metadata"]


def stream_matches(
    stream: Dict[str, Any],
    table_name: Optional[str] = None,
    schema_name: Optional[str] = None,
) -> bool:
    if not table_name and not schema_name:
        return True

    table_candidates = {
        str(stream.get("table_name") or "").lower(),
        str(stream.get("stream") or "").lower(),
        str(stream.get("tap_stream_id") or "").lower(),
    }
    root = metadata_root(stream)
    stream_schema = (
        root.get("schema-name")
        or root.get("database-name")
        or stream.get("schema_name")
        or stream.get("database_name")
    )
    stream_schema_lower = str(stream_schema or "").lower()

    if table_name and table_name.lower() not in table_candidates:
        return False
    if schema_name and schema_name.lower() != stream_schema_lower:
        return False
    return True


def select_catalog_streams(
    source_type: str,
    catalog: Dict[str, Any],
    table_name: Optional[str],
    schema_name: Optional[str],
    sync_mode: str,
    replication_key: Optional[str] = None,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    source_type = ensure_supported_source(source_type)
    selected_catalog = copy.deepcopy(catalog)
    streams = selected_catalog.get("streams") or []

    selected_streams: List[Dict[str, Any]] = []
    for stream in streams:
        root = metadata_root(stream)
        is_selected = stream_matches(stream, table_name=table_name, schema_name=schema_name)
        root["selected"] = bool(is_selected)

        if not is_selected:
            continue

        selected_streams.append(stream)

        desired_mode = "INCREMENTAL" if sync_mode == "incremental" else "FULL_TABLE"
        if source_type == "postgresql" and sync_mode == "incremental":
            desired_mode = "INCREMENTAL"
        root["replication-method"] = desired_mode

        chosen_replication_key = replication_key
        if not chosen_replication_key:
            valid_keys = root.get("valid-replication-keys") or []
            if isinstance(valid_keys, list) and valid_keys:
                chosen_replication_key = valid_keys[0]
        if chosen_replication_key:
            root["replication-key"] = chosen_replication_key

        for item in stream.get("metadata", []):
            breadcrumb = item.get("breadcrumb") or []
            if len(breadcrumb) >= 2 and breadcrumb[0] == "properties":
                item.setdefault("metadata", {})
                item["metadata"]["selected"] = True

    if table_name and not selected_streams:
        raise ValueError(f"Table '{table_name}' was not found in discovery catalog")

    return selected_catalog, selected_streams


def extract_schema(
    catalog: Dict[str, Any],
    table_name: Optional[str] = None,
    schema_name: Optional[str] = None,
) -> Dict[str, Any]:
    streams = catalog.get("streams") or []
    selected = None

    for stream in streams:
        if stream_matches(stream, table_name=table_name, schema_name=schema_name):
            selected = stream
            break
    if selected is None and streams:
        selected = streams[0]

    if not selected:
        return {"columns": [], "primary_keys": []}

    schema = selected.get("schema") or {}
    properties = schema.get("properties") or {}

    columns = []
    for key, value in properties.items():
        json_type = "string"
        nullable = True
        if isinstance(value, dict):
            field_type = value.get("type", "string")
            if isinstance(field_type, list):
                non_null = [t for t in field_type if t != "null"]
                json_type = non_null[0] if non_null else "string"
                nullable = "null" in field_type
            else:
                json_type = str(field_type)
                nullable = field_type == "null"
        columns.append({"name": key, "type": json_type, "nullable": nullable})

    root = metadata_root(selected)
    primary_keys = root.get("table-key-properties") or selected.get("key_properties") or []
    estimated_row_count = root.get("row-count")

    return {
        "columns": columns,
        "primary_keys": list(primary_keys),
        "estimated_row_count": estimated_row_count,
    }


def catalog_to_schemas(catalog: Dict[str, Any]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for stream in catalog.get("streams") or []:
        root = metadata_root(stream)
        resolved_schema = (
            root.get("schema-name")
            or root.get("database-name")
            or stream.get("schema_name")
            or stream.get("database_name")
            or "public"
        )
        schema_key = str(resolved_schema)

        resolved_table = stream.get("table_name") or stream.get("stream") or stream.get("tap_stream_id")
        table_name = str(resolved_table or "").strip()
        if not table_name:
            continue
        if "." in table_name:
            table_name = table_name.split(".")[-1]

        table_entry = {
            "name": table_name,
            "schema": schema_key,
            "type": "table",
        }
        row_count = root.get("row-count")
        if row_count is not None:
            table_entry["rowCount"] = row_count

        schema_tables = grouped.setdefault(schema_key, {})
        schema_tables[table_name] = table_entry

    schemas: List[Dict[str, Any]] = []
    for schema_key in sorted(grouped.keys()):
        tables = list(grouped[schema_key].values())
        tables.sort(key=lambda item: str(item.get("name", "")))
        schemas.append({"name": schema_key, "tables": tables})
    return schemas


def merge_state(current_state: Optional[Dict[str, Any]], new_state: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    current_state = current_state or {}
    new_state = new_state or {}
    if not current_state:
        return new_state
    if not new_state:
        return current_state

    merged = copy.deepcopy(current_state)
    merged_bookmarks = merged.setdefault("bookmarks", {})
    incoming_bookmarks = new_state.get("bookmarks", {})
    if isinstance(incoming_bookmarks, dict):
        merged_bookmarks.update(incoming_bookmarks)

    # Copy over other top-level keys from incoming state.
    for key, value in new_state.items():
        if key != "bookmarks":
            merged[key] = value
    return merged


def chunked(items: List[Dict[str, Any]], chunk_size: int) -> Iterable[List[Dict[str, Any]]]:
    if chunk_size <= 0:
        yield items
        return
    for idx in range(0, len(items), chunk_size):
        yield items[idx : idx + chunk_size]
