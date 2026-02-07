"""Safe transformation runtime for user-provided Python scripts."""

from __future__ import annotations

import ast
import json
from typing import Any, Dict, List, Tuple


def _safe_import(name, globals=None, locals=None, fromlist=(), level=0):  # noqa: A002
    """Allow imports only for the json module."""
    if level != 0:
        raise ImportError("Relative imports are not allowed")
    if name == "json":
        return json
    raise ImportError(f"Only json imports are allowed (got: {name})")


SAFE_BUILTINS = {
    "abs": abs,
    "all": all,
    "any": any,
    "bool": bool,
    "dict": dict,
    "enumerate": enumerate,
    "filter": filter,
    "float": float,
    "int": int,
    "isinstance": isinstance,
    "len": len,
    "list": list,
    "map": map,
    "max": max,
    "min": min,
    "range": range,
    "round": round,
    "set": set,
    "sorted": sorted,
    "str": str,
    "sum": sum,
    "tuple": tuple,
    "zip": zip,
    "Exception": Exception,
    "ValueError": ValueError,
    "TypeError": TypeError,
    "KeyError": KeyError,
    "__import__": _safe_import,
}

FORBIDDEN_NODES: Tuple[type[ast.AST], ...] = (
    ast.Global,
    ast.Nonlocal,
    ast.With,
    ast.AsyncWith,
    ast.Try,
    ast.Raise,
    ast.ClassDef,
    ast.Lambda,
)

FORBIDDEN_NAMES = {
    "__import__",
    "open",
    "exec",
    "eval",
    "compile",
    "globals",
    "locals",
    "vars",
    "input",
    "help",
    "dir",
    "breakpoint",
    "memoryview",
}


def _validate_ast(script: str) -> None:
    tree = ast.parse(script, mode="exec")

    has_transform = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name != "json":
                    raise ValueError("Only `import json` is allowed")
            continue

        if isinstance(node, ast.ImportFrom):
            if node.module != "json" or node.level != 0:
                raise ValueError("Only imports from `json` are allowed")
            if any(alias.name == "*" for alias in node.names):
                raise ValueError("Wildcard imports are not allowed")
            continue

        if isinstance(node, FORBIDDEN_NODES):
            raise ValueError(f"Forbidden syntax detected: {node.__class__.__name__}")
        if isinstance(node, ast.Name) and node.id in FORBIDDEN_NAMES:
            raise ValueError(f"Forbidden name usage: {node.id}")
        if isinstance(node, ast.Attribute) and node.attr.startswith("__"):
            raise ValueError("Dunder attribute access is not allowed")

        if isinstance(node, ast.FunctionDef) and node.name == "transform":
            has_transform = True
            if len(node.args.args) != 1:
                raise ValueError("transform(record) must accept exactly one argument")

    if not has_transform:
        raise ValueError("Transform script must define `def transform(record): ...`")


def _compile_transform(script: str):
    _validate_ast(script)
    compiled = compile(script, "<transform_script>", "exec")
    global_scope = {"__builtins__": SAFE_BUILTINS, "json": json}
    local_scope: Dict[str, Any] = {}
    exec(compiled, global_scope, local_scope)
    transform_fn = local_scope.get("transform")
    if not callable(transform_fn):
        raise ValueError("Transform script must define callable `transform(record)`")
    return transform_fn


def validate_transform_script(transform_script: str) -> Dict[str, Any]:
    if not transform_script or not transform_script.strip():
        return {"valid": False, "error": "Transform script is empty"}

    try:
        transform_fn = _compile_transform(transform_script)
        test_record = {"sample": 1}
        test_result = transform_fn(test_record)
        if not isinstance(test_result, dict):
            return {"valid": False, "error": "transform(record) must return a dict"}
        return {"valid": True}
    except Exception as exc:  # pragma: no cover - explicit error transport
        return {"valid": False, "error": str(exc)}


def safe_exec_transform(records: List[Dict[str, Any]], transform_script: str) -> Dict[str, Any]:
    if not transform_script or not transform_script.strip():
        return {"transformed_rows": records, "errors": []}

    try:
        transform_fn = _compile_transform(transform_script)
    except Exception as exc:
        return {
            "transformed_rows": [],
            "errors": [
                {
                    "record_index": -1,
                    "error": f"Invalid transform script: {exc}",
                    "type": "script_validation_error",
                }
            ],
        }

    transformed_rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for idx, record in enumerate(records):
        try:
            # Controlled local scope for per-record transform context.
            local_scope = {"record": record, "json": json}
            result = transform_fn(local_scope["record"])
            if not isinstance(result, dict):
                raise TypeError(f"transform(record) must return dict, got {type(result).__name__}")
            transformed_rows.append(result)
        except Exception as exc:
            errors.append(
                {
                    "record_index": idx,
                    "error": str(exc),
                    "type": exc.__class__.__name__,
                }
            )

    return {"transformed_rows": transformed_rows, "errors": errors}
