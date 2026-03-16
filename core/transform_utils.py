"""Helpers for record-level transforms used by preview and sync paths."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from typing import Any, Callable

SDC_PREFIX = "_sdc_"


@dataclass
class TransformCounters:
    rows_read: int = 0
    rows_written: int = 0
    rows_dropped: int = 0
    transform_errors: int = 0


def compile_transform_fn(
    script: str | None,
) -> Callable[[dict[str, Any]], dict[str, Any] | None] | None:
    if not script:
        return None

    namespace: dict[str, Any] = {}
    exec(script, namespace)  # noqa: S102
    transform_fn = namespace.get("transform")
    if not callable(transform_fn):
        raise ValueError("transform_script must define callable transform(record)")
    return transform_fn


def has_record_level_transform(
    *,
    column_map: dict[str, str] | None,
    drop_columns: list[str] | None,
    transform_script: str | None,
) -> bool:
    return bool(column_map or drop_columns or (transform_script or "").strip())


def apply_record_transform(
    record: dict[str, Any],
    *,
    column_map: dict[str, str] | None = None,
    drop_columns: set[str] | None = None,
    transform_fn: Callable[[dict[str, Any]], dict[str, Any] | None] | None = None,
    on_error: str = "fail",
) -> dict[str, Any] | None:
    working = {key: value for key, value in record.items() if not key.startswith(SDC_PREFIX)}

    if drop_columns:
        working = {key: value for key, value in working.items() if key not in drop_columns}

    if column_map:
        working = {column_map.get(key, key): value for key, value in working.items()}

    if transform_fn is None:
        return working

    try:
        transformed = transform_fn(working)
    except Exception:
        if on_error == "skip":
            return None
        raise

    if transformed is None:
        return None
    if not isinstance(transformed, dict):
        if on_error == "skip":
            return None
        raise ValueError("transform(record) must return dict or None")
    return transformed


def count_data_item_rows(item: Any) -> int:
    if item is None:
        return 0
    if isinstance(item, list):
        return len(item)
    if hasattr(item, "num_rows"):
        return int(item.num_rows)
    if hasattr(item, "shape") and isinstance(item.shape, tuple) and item.shape:
        return int(item.shape[0])
    return 1


def extract_transform_output_mappings(script: str) -> dict[str, str]:
    """Best-effort mapping of output keys to source keys for static analysis."""
    if not script:
        return {}

    try:
        tree = ast.parse(script)
    except SyntaxError:
        return {}

    def _extract_record_access(node: ast.expr) -> str | None:
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "record"
            and node.func.attr == "get"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            return node.args[0].value

        if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name) and node.value.id == "record":
            slice_node = node.slice
            if isinstance(slice_node, ast.Constant) and isinstance(slice_node.value, str):
                return slice_node.value
        return None

    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef) or node.name != "transform":
            continue

        assignments: dict[str, str] = {}
        for statement in ast.walk(node):
            if (
                isinstance(statement, ast.Assign)
                and len(statement.targets) == 1
                and isinstance(statement.targets[0], ast.Name)
            ):
                source = _extract_record_access(statement.value)
                if source:
                    assignments[statement.targets[0].id] = source

        mappings: dict[str, str] = {}
        for statement in ast.walk(node):
            if not isinstance(statement, ast.Return) or not isinstance(statement.value, ast.Dict):
                continue
            for key_node, value_node in zip(statement.value.keys, statement.value.values):
                if not isinstance(key_node, ast.Constant) or not isinstance(key_node.value, str):
                    continue
                source = _extract_record_access(value_node)
                if source:
                    mappings[key_node.value] = source
                    continue
                if isinstance(value_node, ast.Name) and value_node.id in assignments:
                    mappings[key_node.value] = assignments[value_node.id]
            return mappings

    return {}

