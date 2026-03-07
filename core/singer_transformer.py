"""Singer message transformer — reads stdin, transforms, writes stdout.

Runnable as a subprocess: ``python -m core.singer_transformer``

Accepts env vars for configuration:
- COLUMN_MAP: JSON string of {"old_name": "new_name"} renames
- DROP_COLUMNS: comma-separated column names to drop
- TRANSFORM_SCRIPT: base64-encoded Python transform script (must define ``transform(row) -> dict``)
- COLUMN_SQL_TYPES: JSON string of {"output_col": "pg_type"} for x-sql-datatype (e.g. {"id": "uuid"})
- UPSERT_KEY: JSON array of output column names to use as key_properties for upsert

When a transform is used: RECORDs are transformed; SCHEMA is updated to match output columns.
STATE messages pass through unchanged.
"""

from __future__ import annotations

import base64
import json
import os
import re
import sys
from typing import Any, Callable

SDC_PREFIX = "_sdc_"


def _parse_transform_output_mappings(script: str) -> dict[str, str]:
    """Parse transform script to extract output_key -> source_column mappings."""
    result: dict[str, str] = {}
    if not script:
        return result
    try:
        match = re.search(r"return\s*\{([^}]*)\}", script, re.DOTALL)
        if not match:
            return result
        body = match.group(1)
        for m in re.finditer(
            r'["\']?([a-zA-Z_][a-zA-Z0-9_]*)["\']?\s*:\s*([^,}]+)',
            body,
        ):
            out_key = m.group(1)
            expr = m.group(2).strip()
            src = None
            get_m = re.search(r'record\.get\s*\(\s*["\']([^"\']+)["\']\s*\)', expr)
            if get_m:
                src = get_m.group(1)
            else:
                bracket_m = re.search(r'record\s*\[\s*["\']([^"\']+)["\']\s*\]', expr)
                if bracket_m:
                    src = bracket_m.group(1)
                else:
                    dot_m = re.search(r"record\.([a-zA-Z_][a-zA-Z0-9_]*)\b", expr)
                    if dot_m:
                        src = dot_m.group(1)
            if src:
                result[out_key] = src
    except Exception:
        pass
    return result


def _load_column_map() -> dict[str, str]:
    raw = os.environ.get("COLUMN_MAP", "")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def _load_drop_columns() -> set[str]:
    raw = os.environ.get("DROP_COLUMNS", "")
    if not raw:
        return set()
    return {c.strip() for c in raw.split(",") if c.strip()}


def _load_column_sql_types() -> dict[str, str]:
    """Return output column -> PostgreSQL type for x-sql-datatype (e.g. {"id": "uuid"})."""
    raw = os.environ.get("COLUMN_SQL_TYPES", "")
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return {k: v for k, v in (data or {}).items() if isinstance(v, str)}
    except json.JSONDecodeError:
        return {}


def _load_upsert_key() -> list[str] | None:
    """Return user-specified upsert key columns (output column names) or None."""
    raw = os.environ.get("UPSERT_KEY", "")
    if not raw:
        return None
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return [str(c) for c in data if c]
        return None
    except json.JSONDecodeError:
        return None


def _load_transform_script() -> str:
    """Return decoded transform script or empty string."""
    raw_b64 = os.environ.get("TRANSFORM_SCRIPT", "")
    if not raw_b64:
        return ""
    try:
        return base64.b64decode(raw_b64).decode("utf-8")
    except Exception:
        return ""


def _load_transform_fn() -> Callable[[dict[str, Any]], dict[str, Any] | None] | None:
    script = _load_transform_script()
    if not script:
        return None
    try:
        namespace: dict[str, Any] = {}
        exec(script, namespace)  # noqa: S102
        fn = namespace.get("transform")
        if callable(fn):
            return fn
    except Exception as exc:
        print(f"WARNING: Failed to load transform script: {exc}", file=sys.stderr)
    return None


def transform_record(
    record: dict[str, Any],
    column_map: dict[str, str],
    drop_columns: set[str],
    transform_fn: Callable[[dict[str, Any]], dict[str, Any] | None] | None,
) -> dict[str, Any] | None:
    """Apply column map, drop columns, user transform, preserving _sdc_ columns."""
    sdc_fields = {k: v for k, v in record.items() if k.startswith(SDC_PREFIX)}
    working = {k: v for k, v in record.items() if not k.startswith(SDC_PREFIX)}

    if drop_columns:
        working = {k: v for k, v in working.items() if k not in drop_columns}

    if column_map:
        working = {column_map.get(k, k): v for k, v in working.items()}

    if transform_fn:
        try:
            working = transform_fn(working)
        except Exception as exc:
            print(f"WARNING: transform() raised: {exc}", file=sys.stderr)
            return None

        if working is None:
            return None
        if not isinstance(working, dict):
            print(
                "WARNING: transform() must return dict or None; dropping record",
                file=sys.stderr,
            )
            return None

    working.update(sdc_fields)
    return working


def _allows_null(prop: dict[str, Any]) -> bool:
    prop_type = prop.get("type")
    if isinstance(prop_type, list):
        return "null" in prop_type
    return prop_type == "null"


def _infer_schema_property(value: Any) -> dict[str, Any]:
    """Infer a permissive Singer property schema from a Python value."""
    if isinstance(value, bool):
        return {"type": ["boolean", "null"]}
    if isinstance(value, int) and not isinstance(value, bool):
        return {"type": ["integer", "null"]}
    if isinstance(value, float):
        return {"type": ["number", "null"]}
    if isinstance(value, list):
        return {"type": ["array", "null"]}
    if isinstance(value, dict):
        return {"type": ["object", "null"]}
    return {"type": ["string", "null"]}


def _transform_schema(
    schema: dict[str, Any],
    transformed_record: dict[str, Any],
    output_to_source: dict[str, str],
    column_sql_types: dict[str, str],
) -> dict[str, Any]:
    """Build a schema that matches the transform output columns.

    Preserves source column type/format when mapping (e.g. company_id -> id).
    Falls back to string when no mapping. Adds x-sql-datatype when COLUMN_SQL_TYPES provided.
    Preserves _sdc_* properties from the original schema if present.
    """
    properties: dict[str, Any] = {}
    required: list[str] = []
    orig_props = schema.get("properties") or {}
    orig_required = set(schema.get("required") or [])
    output_keys = [k for k in transformed_record.keys() if not k.startswith(SDC_PREFIX)]

    for key in output_keys:
        src_col = output_to_source.get(key)
        if src_col and src_col in orig_props:
            prop = dict(orig_props[src_col])
        else:
            prop = _infer_schema_property(transformed_record.get(key))
        if key in column_sql_types:
            prop["x-sql-datatype"] = column_sql_types[key]
        properties[key] = prop
        if src_col and src_col in orig_required and not _allows_null(prop):
            required.append(key)
    for k, v in orig_props.items():
        if k.startswith(SDC_PREFIX):
            properties[k] = v
    new_schema = {**schema, "properties": properties}
    if required:
        new_schema["required"] = required
    else:
        new_schema.pop("required", None)
    return new_schema


def _map_key_properties(
    original_key_properties: list[str],
    output_to_source: dict[str, str],
    output_keys: list[str],
) -> list[str]:
    """Map source key_properties to output columns using transform mapping.

    If source PK company_id maps to output id, return [id].
    If no mapping possible, return [] to avoid target-postgres key validation errors.
    """
    source_to_output = {v: k for k, v in output_to_source.items()}
    new_keys: list[str] = []
    for src in original_key_properties:
        out = source_to_output.get(src)
        if out and out in output_keys:
            new_keys.append(out)
    return new_keys


def run() -> None:
    """Main loop: read Singer JSON lines from stdin, transform, write to stdout."""
    column_map = _load_column_map()
    drop_columns = _load_drop_columns()
    column_sql_types = _load_column_sql_types()
    upsert_key = _load_upsert_key()
    transform_fn = _load_transform_fn()
    output_to_source: dict[str, str] = {}
    if transform_fn:
        output_to_source = _parse_transform_output_mappings(_load_transform_script())

    pending_schema_by_stream: dict[str, dict[str, Any]] = {}

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            sys.stdout.write(line + "\n")
            sys.stdout.flush()
            continue

        msg_type = msg.get("type", "").upper()
        stream_name = msg.get("stream") or "__default__"

        if msg_type == "RECORD":
            transformed_record = transform_record(
                msg.get("record", {}), column_map, drop_columns, transform_fn
            )
            if transformed_record is None:
                continue
            msg["record"] = transformed_record
            if transform_fn and stream_name in pending_schema_by_stream:
                schema_msg = pending_schema_by_stream.pop(stream_name)
                schema = schema_msg.get("schema", {})
                schema_msg["schema"] = _transform_schema(
                    schema,
                    transformed_record,
                    output_to_source,
                    column_sql_types,
                )
                output_keys = [k for k in transformed_record.keys() if not k.startswith(SDC_PREFIX)]
                if upsert_key:
                    schema_msg["key_properties"] = [k for k in upsert_key if k in output_keys]
                else:
                    original_keys = schema_msg.get("key_properties") or []
                    schema_msg["key_properties"] = _map_key_properties(
                        original_keys,
                        output_to_source,
                        output_keys,
                    )
                sys.stdout.write(json.dumps(schema_msg, separators=(",", ":")) + "\n")
                sys.stdout.flush()
        elif msg_type == "SCHEMA":
            if transform_fn:
                pending_schema_by_stream[stream_name] = msg
                continue
            if upsert_key:
                schema_props = (msg.get("schema") or {}).get("properties") or {}
                msg["key_properties"] = [k for k in upsert_key if k in schema_props]
            if column_sql_types:
                schema_props = (msg.get("schema") or {}).get("properties") or {}
                for key, sql_type in column_sql_types.items():
                    if key in schema_props:
                        schema_props[key]["x-sql-datatype"] = sql_type

        sys.stdout.write(json.dumps(msg, separators=(",", ":")) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    run()
