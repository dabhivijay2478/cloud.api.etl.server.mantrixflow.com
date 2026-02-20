"""Pipeline execution via Meltano run.

Runs tap → target jobs with dynamic connection config.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Optional

from .connection_mapper import connection_config_to_meltano_env_for_pipeline
from .meltano_runner import MeltanoRunResult, run_meltano_job

# Map (source_type, dest_type) -> Meltano job name (bidirectional ETL + same-type sync)
DIRECTION_TO_JOB: Dict[tuple[str, str], str] = {
    ("mongodb", "postgresql"): "mongodb-to-postgres",
    ("postgresql", "postgresql"): "postgres-to-postgres",
    ("mysql", "postgresql"): "mysql-to-postgres",
    ("postgresql", "mysql"): "postgres-to-mysql",
    ("postgresql", "mongodb"): "postgres-to-mongodb",
    ("mysql", "mongodb"): "mysql-to-mongodb",
    ("mongodb", "mysql"): "mongodb-to-mysql",
    ("mysql", "mysql"): "mysql-to-mysql",
    ("mongodb", "mongodb"): "mongodb-to-mongodb",
}


@dataclass
class PipelineRunResult:
    """Result of a pipeline job execution."""

    success: bool
    rows_read: int
    rows_written: int
    rows_skipped: int
    rows_failed: int
    checkpoint: Dict[str, Any]
    errors: list[Dict[str, Any]]
    meltano_result: MeltanoRunResult
    user_message: Optional[str] = None


def _parse_row_counts_from_output(stdout: str, stderr: str) -> tuple[int, int]:
    """Attempt to parse record counts from Meltano/tap output. Returns (rows_read, rows_written)."""
    rows_read = 0
    rows_written = 0
    combined = (stdout or "") + "\n" + (stderr or "")
    for line in combined.splitlines():
        line_lower = line.lower()
        if '"type": "record"' in line_lower or '"type":"record"' in line_lower:
            rows_read += 1
        # Tap METRIC: metric_name=record_count metric_value=N
        if "metric_name=record_count" in line_lower and "metric_value=" in line_lower:
            m = re.search(r"metric_value[=:](\d+)", line_lower, re.IGNORECASE)
            if m:
                rows_read = max(rows_read, int(m.group(1)))
        if "records loaded" in line_lower or "rows loaded" in line_lower:
            for part in line.split():
                if part.isdigit():
                    rows_written = max(rows_written, int(part))
                    break
    return rows_read, rows_written


async def run_pipeline_job(
    job_name: str,
    source_type: str,
    source_connection_config: Dict[str, Any],
    dest_type: str,
    dest_connection_config: Dict[str, Any],
    *,
    state_id: Optional[str] = None,
    checkpoint: Optional[Dict[str, Any]] = None,
    sync_mode: Optional[str] = None,
    dbt_models: Optional[list[str]] = None,
    source_table: Optional[str] = None,
    dest_table: Optional[str] = None,
    timeout_seconds: int = 3600,
) -> PipelineRunResult:
    """Run a Meltano pipeline job with dynamic connection config.

    Args:
        job_name: Meltano job name (e.g. 'mongodb-to-postgres').
        source_type: postgresql, mysql, or mongodb.
        source_connection_config: API-style connection config for source.
        dest_type: postgresql, mysql, or mongodb (must have a target plugin).
        dest_connection_config: API-style connection config for destination.
        state_id: Optional Meltano state ID for CDC resume.
        checkpoint: Unused here; Meltano manages state internally.
        timeout_seconds: Max seconds for job.

    Returns:
        PipelineRunResult with counts, checkpoint, errors.
    """
    env = connection_config_to_meltano_env_for_pipeline(
        source_type, source_connection_config, dest_type, dest_connection_config,
        sync_mode=sync_mode,
        dbt_models=dbt_models,
        source_table=source_table,
    )

    run_args: list[str] = []
    if state_id:
        run_args.extend(["--state-id", state_id])

    result = await run_meltano_job(
        job_name,
        env_overrides=env,
        timeout_seconds=timeout_seconds,
        run_args=run_args if run_args else None,
    )

    rows_read, rows_written = _parse_row_counts_from_output(result.stdout, result.stderr)
    errors: list[Dict[str, Any]] = []
    if not result.success:
        errors.append(
            {
                "error": result.user_message or result.stderr or result.stdout or f"Exit code {result.exit_code}",
                "type": result.error_type or "pipeline_error",
            }
        )

    return PipelineRunResult(
        success=result.success,
        rows_read=rows_read,
        rows_written=rows_written,
        rows_skipped=0,
        rows_failed=len(errors) if not result.success else 0,
        checkpoint=checkpoint or {},
        errors=errors,
        meltano_result=result,
        user_message=result.user_message,
    )


def get_job_for_direction(source_type: str, dest_type: str) -> Optional[str]:
    """Return Meltano job name for (source, dest), or None if no job exists."""
    key = (source_type.lower(), dest_type.lower())
    return DIRECTION_TO_JOB.get(key)


def has_meltano_job_for_direction(source_type: str, dest_type: str) -> bool:
    """True if a Meltano job exists for this direction."""
    return get_job_for_direction(source_type, dest_type) is not None
