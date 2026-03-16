"""Compile and wrap user transform scripts for dlt add_map."""

from __future__ import annotations

import logging
from typing import Any, Callable

logger = logging.getLogger("etl.transform")


def compile_transform(script: str | None) -> Callable[[dict[str, Any]], dict[str, Any] | None] | None:
    """
    Compiles the user's transform script string into a callable.
    The script must define a function named 'transform'.
    Raises SyntaxError if the script has syntax errors.
    Returns the compiled transform function, or None if script is None/empty.
    """
    if not script or not script.strip():
        return None
    namespace: dict[str, Any] = {}
    exec(script, namespace)  # noqa: S102
    transform_fn = namespace.get("transform")
    if not callable(transform_fn):
        raise ValueError("transform_script must define callable transform(record)")
    return transform_fn


def make_add_map_fn(
    transform_fn: Callable[[dict[str, Any]], dict[str, Any] | None],
    on_error: str,
    drop_counter: list,
) -> Callable[[Any], Any]:
    """
    Wraps transform_fn as a function suitable for dlt add_map.
    For each record:
    - Calls transform_fn(record)
    - If result is None: increments drop_counter[0], returns None
    - If on_error == 'skip' and exception raised: logs warning, increments drop_counter[0], returns None
    - If on_error == 'fail' and exception raised: re-raises
    """
    def _map(item: Any) -> Any:
        if isinstance(item, list):
            result = []
            for row in item:
                if not isinstance(row, dict):
                    continue
                try:
                    transformed = transform_fn(row)
                except Exception as exc:
                    if on_error == "skip":
                        logger.warning("Transform skipped record: %s", exc)
                        drop_counter[0] += 1
                        continue
                    raise
                if transformed is None:
                    drop_counter[0] += 1
                    continue
                result.append(transformed)
            return result

        if not isinstance(item, dict):
            return item
        try:
            transformed = transform_fn(item)
        except Exception as exc:
            if on_error == "skip":
                logger.warning("Transform skipped record: %s", exc)
                drop_counter[0] += 1
                return None
            raise
        if transformed is None:
            drop_counter[0] += 1
            return None
        return transformed

    return _map
