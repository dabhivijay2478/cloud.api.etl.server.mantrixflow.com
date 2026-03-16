"""Extract run metrics from dlt pipeline after completion."""

from __future__ import annotations

import time
from typing import Any


def extract_metrics(
    pipeline: Any,
    started_at: float,
    drop_counter: list,
) -> dict[str, Any]:
    """
    Extracts run metrics from pipeline.last_trace after pipeline.run() completes.
    Returns dict with rows_written, rows_dropped, rows_failed, duration_seconds, schema_evolutions.
    """
    duration_seconds = round(time.time() - started_at, 2)
    rows_written = 0
    rows_failed = 0
    schema_evolutions = 0

    try:
        trace = getattr(pipeline, "last_trace", None)
        if trace:
            if hasattr(trace, "schema_updates"):
                schema_evolutions = len(trace.schema_updates)
            if hasattr(trace, "failed_jobs") and trace.failed_jobs:
                rows_failed = len(trace.failed_jobs)
            load_packages = getattr(trace, "load_packages", None)
            if load_packages and isinstance(load_packages, dict):
                for pkg in load_packages.values():
                    if hasattr(pkg, "schema") and hasattr(pkg.schema, "tables"):
                        for table in pkg.schema.tables.values():
                            if hasattr(table, "row_counts"):
                                rows_written += sum(table.row_counts)
    except Exception:
        pass

    return {
        "rows_written": rows_written,
        "rows_dropped": drop_counter[0] if drop_counter else 0,
        "rows_failed": rows_failed,
        "duration_seconds": duration_seconds,
        "schema_evolutions": schema_evolutions,
    }


def extract_state(pipeline: Any, selected_streams: list[str]) -> dict | None:
    """
    Extracts final cursor values from pipeline.state for INCREMENTAL runs.
    Returns dict: { "table_name": "last_cursor_value_as_string" }
    Returns None for FULL_TABLE runs.
    """
    if not hasattr(pipeline, "state") or not pipeline.state:
        return None
    state = pipeline.state
    sources = state.get("sources", {})
    result = {}
    for source_name, source_data in sources.items():
        if not isinstance(source_data, dict):
            continue
        resources = source_data.get("resources", {})
        for table_name, resource_data in resources.items():
            if table_name not in selected_streams:
                continue
            if not isinstance(resource_data, dict):
                continue
            incremental = resource_data.get("incremental", {})
            if isinstance(incremental, dict):
                for key, val in incremental.items():
                    if key != "last_value":
                        continue
                    if val is not None:
                        result[table_name] = str(val)
                    break
    return result if result else None
