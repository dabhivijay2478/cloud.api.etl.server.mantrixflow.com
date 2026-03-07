"""Build Singer catalog JSON for tap-postgres.

Takes the raw --discover output and produces a catalog with the desired
stream selected and replication_method set.
"""

from __future__ import annotations

import copy
import logging
from typing import Any

logger = logging.getLogger("etl.catalog")

VALID_REPLICATION_METHODS = {"FULL_TABLE", "LOG_BASED"}


def _normalize(name: str) -> str:
    """Normalize a stream name for flexible matching (dots → dashes, lowercase)."""
    return name.replace(".", "-").lower()


def _matches_stream(tap_stream_id: str, stream_name: str, selected: str) -> bool:
    """Check if a discovered stream matches the requested selected_stream.

    Matching strategy (in priority order):
    1. Exact match on tap_stream_id or stream name
    2. Normalized match (dots ↔ dashes)
    3. tap_stream_id or stream ends with the selected name (handles db-prefix)
    """
    if tap_stream_id == selected or stream_name == selected:
        return True

    norm_selected = _normalize(selected)
    if _normalize(tap_stream_id) == norm_selected or _normalize(stream_name) == norm_selected:
        return True

    if tap_stream_id.endswith(f"-{_normalize(selected)}"):
        return True
    if _normalize(stream_name).endswith(f"-{norm_selected.split('-', 1)[-1]}") and norm_selected.split("-", 1)[0] in _normalize(stream_name):
        return True

    return False


def build_catalog(
    discovered_catalog: dict[str, Any],
    selected_stream: str,
    replication_method: str = "FULL_TABLE",
) -> dict[str, Any]:
    """Return a catalog dict with *one* stream selected and replication_method set.

    Parameters
    ----------
    discovered_catalog:
        Raw JSON from ``tap-postgres --discover``.
    selected_stream:
        The ``tap_stream_id`` to select (e.g. ``"public-orders"``).
    replication_method:
        ``"FULL_TABLE"`` or ``"LOG_BASED"``.
    """
    if replication_method not in VALID_REPLICATION_METHODS:
        raise ValueError(
            f"Invalid replication_method: {replication_method}. "
            f"Must be one of {VALID_REPLICATION_METHODS}"
        )

    catalog = copy.deepcopy(discovered_catalog)

    matched_any = False
    available_names: list[str] = []

    for stream in catalog.get("streams", []):
        tap_stream_id = stream.get("tap_stream_id", "")
        stream_name = stream.get("stream", "")
        available_names.append(f"{stream_name} (tap_stream_id={tap_stream_id})")
        is_selected = _matches_stream(tap_stream_id, stream_name, selected_stream)
        if is_selected:
            matched_any = True
        _set_stream_metadata(stream, is_selected, replication_method if is_selected else None)

    if matched_any:
        logger.info("Stream '%s' matched in catalog", selected_stream)
    else:
        logger.warning(
            "No stream matched '%s' in catalog! Available streams: %s",
            selected_stream,
            ", ".join(available_names[:20]),
        )

    return catalog


def parse_discovered_streams(discovered_catalog: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse --discover output into a simplified list of streams.

    Returns a list of dicts, each with:
    - stream_name
    - tap_stream_id
    - stream (NestJS-compatible alias for stream_name)
    - schema (NestJS-compatible: {properties: {col: {type: [...]}}})
    - key_properties (NestJS-compatible alias for primary_keys)
    - columns: [{name, type, nullable}]
    - primary_keys: [str]
    - replication_methods: [str]  (available methods)
    """
    results: list[dict[str, Any]] = []

    for stream in discovered_catalog.get("streams", []):
        schema = stream.get("schema", {})
        properties = schema.get("properties", {})
        key_properties = stream.get("key_properties", [])

        columns = []
        schema_properties: dict[str, Any] = {}
        for col_name, col_schema in properties.items():
            col_type_raw = col_schema.get("type", "string")
            if isinstance(col_type_raw, list):
                col_type = next((t for t in col_type_raw if t != "null"), "string")
                nullable = "null" in col_type_raw
                schema_properties[col_name] = {"type": col_type_raw}
            else:
                col_type = col_type_raw
                nullable = False
                schema_properties[col_name] = {"type": [col_type] if not nullable else [col_type, "null"]}

            columns.append({
                "name": col_name,
                "type": col_type,
                "nullable": nullable,
                "is_primary_key": col_name in key_properties,
            })

        available_methods = _get_available_replication_methods(stream)
        stream_name = stream.get("stream", "")

        results.append({
            "stream_name": stream_name,
            "tap_stream_id": stream.get("tap_stream_id", ""),
            "stream": stream_name,
            "schema": {"properties": schema_properties},
            "key_properties": key_properties,
            "columns": columns,
            "primary_keys": key_properties,
            "replication_methods": available_methods,
            "log_based_eligible": "LOG_BASED" in available_methods,
        })

    return results


def _set_stream_metadata(
    stream: dict[str, Any],
    selected: bool,
    replication_method: str | None,
) -> None:
    """Mutate stream metadata to mark it selected / set replication_method."""
    metadata = stream.get("metadata", [])

    found_root = False
    for entry in metadata:
        breadcrumb = entry.get("breadcrumb", [])
        if breadcrumb == []:
            found_root = True
            entry.setdefault("metadata", {})
            entry["metadata"]["selected"] = selected
            if replication_method and selected:
                entry["metadata"]["replication-method"] = replication_method
            break

    if not found_root:
        root_entry: dict[str, Any] = {
            "breadcrumb": [],
            "metadata": {"selected": selected},
        }
        if replication_method and selected:
            root_entry["metadata"]["replication-method"] = replication_method
        metadata.append(root_entry)
        stream["metadata"] = metadata

    if not metadata:
        stream["metadata"] = [
            {
                "breadcrumb": [],
                "metadata": {
                    "selected": selected,
                    **({"replication-method": replication_method} if replication_method and selected else {}),
                },
            }
        ]


def _get_available_replication_methods(stream: dict[str, Any]) -> list[str]:
    """Extract available replication methods from stream metadata."""
    methods = ["FULL_TABLE"]
    for entry in stream.get("metadata", []):
        if entry.get("breadcrumb") == []:
            meta = entry.get("metadata", {})
            forced = meta.get("forced-replication-method")
            if forced:
                return [forced]
            valid = meta.get("valid-replication-keys") or []
            if valid or meta.get("replication-method") == "LOG_BASED":
                methods.append("LOG_BASED")
            break
    return methods
