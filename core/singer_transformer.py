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


def _load_transform_fn() -> Callable[[dict[str, Any]], dict[str, Any]] | None:
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
    transform_fn: Callable[[dict[str, Any]], dict[str, Any]] | None,
) -> dict[str, Any]:
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

    working.update(sdc_fields)
    return working


def _get_transform_output_keys(
    transform_fn: Callable[[dict[str, Any]], dict[str, Any]],
) -> list[str] | None:
    """Get output column names from transform by running it on an empty record."""
    try:
        result = transform_fn({})
        if isinstance(result, dict):
            return list(result.keys())
    except Exception:
        pass
    return None


def _transform_schema(
    schema: dict[str, Any],
    output_keys: list[str],
    output_to_source: dict[str, str],
    column_sql_types: dict[str, str],
) -> dict[str, Any]:
    """Build a schema that matches the transform output columns.

    Preserves source column type/format when mapping (e.g. company_id -> id).
    Falls back to string when no mapping. Adds x-sql-datatype when COLUMN_SQL_TYPES provided.
    Preserves _sdc_* properties from the original schema if present.
    """
    properties: dict[str, Any] = {}
    orig_props = schema.get("properties") or {}
    for key in output_keys:
        src_col = output_to_source.get(key)
        if src_col and src_col in orig_props:
            prop = dict(orig_props[src_col])
        else:
            prop = {"type": ["string", "null"]}
        if key in column_sql_types:
            prop["x-sql-datatype"] = column_sql_types[key]
        properties[key] = prop
    for k, v in orig_props.items():
        if k.startswith(SDC_PREFIX):
            properties[k] = v
    return {**schema, "properties": properties}


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
    transform_output_keys: list[str] | None = None
    output_to_source: dict[str, str] = {}
    if transform_fn:
        transform_output_keys = _get_transform_output_keys(transform_fn)
        if transform_output_keys:
            output_to_source = _parse_transform_output_mappings(_load_transform_script())

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

        if msg_type == "RECORD":
            msg["record"] = transform_record(
                msg.get("record", {}), column_map, drop_columns, transform_fn
            )
        elif msg_type == "SCHEMA":
            if transform_output_keys:
                schema = msg.get("schema", {})
                msg["schema"] = _transform_schema(
                    schema, transform_output_keys, output_to_source, column_sql_types
                )
                if upsert_key:
                    # Only include keys that exist in transformed output (avoids target-postgres error)
                    msg["key_properties"] = [k for k in upsert_key if k in transform_output_keys]
                else:
                    original_keys = msg.get("key_properties") or []
                    msg["key_properties"] = _map_key_properties(
                        original_keys, output_to_source, transform_output_keys
                    )
            elif upsert_key:
                # No transform: filter by schema properties to avoid invalid key_properties
                schema_props = (msg.get("schema") or {}).get("properties") or {}
                msg["key_properties"] = [k for k in upsert_key if k in schema_props]

        sys.stdout.write(json.dumps(msg, separators=(",", ":")) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    run()
