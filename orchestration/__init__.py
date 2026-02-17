"""Meltano orchestration layer for MANTrixFlow ETL.

Phase 1: Meltano job runner and connection config mapping.
Phase 2: Discovery via meltano invoke.
"""

from .collect import run_collect_via_meltano
from .connection_mapper import connection_config_to_meltano_env
from .discovery import run_discovery
from .meltano_runner import MeltanoRunResult, run_meltano_invoke, run_meltano_job
from .pipeline_runner import get_job_for_direction, run_pipeline_job

__all__ = [
    "connection_config_to_meltano_env",
    "get_job_for_direction",
    "run_collect_via_meltano",
    "run_discovery",
    "run_meltano_job",
    "run_meltano_invoke",
    "run_pipeline_job",
    "MeltanoRunResult",
]
