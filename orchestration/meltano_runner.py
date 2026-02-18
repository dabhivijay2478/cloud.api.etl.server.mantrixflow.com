"""Meltano CLI job runner for MANTrixFlow orchestration.

Executes `meltano run <job>` with dynamic environment overrides.
Uses asyncio for non-blocking subprocess execution.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

# Default timeout for Meltano jobs (e.g. long-running syncs)
DEFAULT_JOB_TIMEOUT_SECONDS = int(os.getenv("MELTANO_JOB_TIMEOUT_SECONDS", "3600"))
# Default timeout for discovery (usually fast)
DEFAULT_INVOKE_TIMEOUT_SECONDS = int(os.getenv("MELTANO_INVOKE_TIMEOUT_SECONDS", "120"))


@dataclass
class MeltanoRunResult:
    """Result of a Meltano job execution."""

    exit_code: int
    stdout: str
    stderr: str
    success: bool
    error_type: Optional[str] = None
    user_message: Optional[str] = None

    @property
    def is_replication_slot_error(self) -> bool:
        """True if stderr indicates a missing replication slot (PostgreSQL CDC)."""
        err = (self.stderr or "").lower()
        return (
            "replication slot" in err
            or "pg_create_logical_replication_slot" in err
            or "wal_level" in err
        )

    @property
    def is_binlog_error(self) -> bool:
        """True if stderr indicates binlog not enabled (MySQL CDC)."""
        err = (self.stderr or "").lower()
        return "binlog" in err or "log_bin" in err

    @property
    def is_oplog_error(self) -> bool:
        """True if stderr indicates oplog issue (MongoDB CDC)."""
        err = (self.stderr or "").lower()
        return "oplog" in err or "replica set" in err


def _infer_user_message(result: MeltanoRunResult) -> Optional[str]:
    """Infer a user-friendly message for common CDC/setup errors."""
    if result.success:
        return None

    if result.is_replication_slot_error:
        return (
            "PostgreSQL logical replication requires a replication slot. "
            "Enable logical replication in your database provider, then create a slot: "
            "SELECT pg_create_logical_replication_slot('stitch_<dbname>', 'wal2json');"
        )
    if result.is_binlog_error:
        return (
            "MySQL CDC requires binlog to be enabled. "
            "Set log_bin=ON and binlog_format=ROW in your MySQL configuration."
        )
    if result.is_oplog_error:
        return (
            "MongoDB CDC requires a replica set with oplog. "
            "Ensure your MongoDB deployment is a replica set."
        )

    return None


async def run_meltano_job(
    job_name: str,
    env_overrides: Optional[Dict[str, str]] = None,
    *,
    project_dir: Optional[str] = None,
    timeout_seconds: int = DEFAULT_JOB_TIMEOUT_SECONDS,
    run_args: Optional[Sequence[str]] = None,
) -> MeltanoRunResult:
    """Execute `meltano run [run_args...] <job_name>` with dynamic env overrides.

    Args:
        job_name: Meltano job name (e.g. 'mongodb-to-postgres').
        env_overrides: Dict of env var name -> value to inject. Merged with os.environ.
        project_dir: Directory containing meltano.yml. Defaults to apps/etl.
        timeout_seconds: Max seconds for job execution.
        run_args: Optional args for meltano run (e.g. ['--state-id', 'xyz']).

    Returns:
        MeltanoRunResult with exit_code, stdout, stderr, and inferred user_message.
    """
    env_overrides = env_overrides or {}
    run_args = run_args or ()
    base_dir = project_dir or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    work_dir = base_dir if os.path.isfile(os.path.join(base_dir, "meltano.yml")) else os.getcwd()

    env = os.environ.copy()
    for key, value in env_overrides.items():
        if value is not None:
            env[str(key)] = str(value)

    cmd = ["meltano", "run", *run_args, job_name]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=work_dir,
        env=env,
    )

    try:
        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            proc.communicate(),
            timeout=timeout_seconds,
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        return MeltanoRunResult(
            exit_code=-1,
            stdout="",
            stderr=f"Job timed out after {timeout_seconds} seconds",
            success=False,
            error_type="timeout",
            user_message=f"The pipeline run exceeded the timeout of {timeout_seconds} seconds.",
        )

    stdout_str = (stdout_bytes or b"").decode("utf-8", errors="replace")
    stderr_str = (stderr_bytes or b"").decode("utf-8", errors="replace")
    exit_code = proc.returncode or -1
    success = exit_code == 0

    result = MeltanoRunResult(
        exit_code=exit_code,
        stdout=stdout_str,
        stderr=stderr_str,
        success=success,
    )
    user_msg = _infer_user_message(result)
    if user_msg:
        result = MeltanoRunResult(
            exit_code=exit_code,
            stdout=stdout_str,
            stderr=stderr_str,
            success=success,
            error_type="cdc_setup",
            user_message=user_msg,
        )
    return result


def run_meltano_job_sync(
    job_name: str,
    env_overrides: Optional[Dict[str, str]] = None,
    *,
    project_dir: Optional[str] = None,
    timeout_seconds: int = DEFAULT_JOB_TIMEOUT_SECONDS,
) -> MeltanoRunResult:
    """Synchronous wrapper for run_meltano_job. Use in sync contexts (e.g. asyncio.to_thread)."""
    return asyncio.run(
        run_meltano_job(
            job_name,
            env_overrides,
            project_dir=project_dir,
            timeout_seconds=timeout_seconds,
        )
    )


def _resolve_project_dir(project_dir: Optional[str]) -> str:
    """Resolve directory containing meltano.yml."""
    base = project_dir or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return base if os.path.isfile(os.path.join(base, "meltano.yml")) else os.getcwd()


def _get_tap_executable_path(plugin_name: str, project_dir: str) -> Optional[str]:
    """Resolve tap executable path. Returns None if not found."""
    # Meltano installs taps at .meltano/extractors/<plugin>/venv/bin/<plugin>
    exe_path = os.path.join(
        project_dir, ".meltano", "extractors", plugin_name, "venv", "bin", plugin_name
    )
    return exe_path if os.path.isfile(exe_path) else None


async def run_tap_direct(
    plugin_name: str,
    config_path: str,
    args: Optional[List[str]] = None,
    *,
    project_dir: Optional[str] = None,
    timeout_seconds: int = DEFAULT_INVOKE_TIMEOUT_SECONDS,
) -> MeltanoRunResult:
    """Run tap executable directly with --config file, bypassing Meltano.

    Meltano may skip env var parsing; this ensures our config is used.
    """
    work_dir = _resolve_project_dir(project_dir)
    exe = _get_tap_executable_path(plugin_name, work_dir)
    if not exe:
        return MeltanoRunResult(
            exit_code=-1,
            stdout="",
            stderr=f"Tap executable not found: {plugin_name}. Run 'meltano install'.",
            success=False,
            error_type="config",
            user_message=f"Tap {plugin_name} is not installed. Run 'meltano install' in the ETL project.",
        )

    cmd = [exe, "--config", config_path, *(args or [])]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=work_dir,
        env=os.environ.copy(),
    )

    try:
        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            proc.communicate(),
            timeout=timeout_seconds,
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        return MeltanoRunResult(
            exit_code=-1,
            stdout="",
            stderr=f"Tap timed out after {timeout_seconds} seconds",
            success=False,
            error_type="timeout",
            user_message=f"Discovery exceeded the timeout of {timeout_seconds} seconds.",
        )

    stdout_str = (stdout_bytes or b"").decode("utf-8", errors="replace")
    stderr_str = (stderr_bytes or b"").decode("utf-8", errors="replace")
    exit_code = proc.returncode or -1
    success = exit_code == 0

    result = MeltanoRunResult(
        exit_code=exit_code,
        stdout=stdout_str,
        stderr=stderr_str,
        success=success,
    )
    user_msg = _infer_user_message(result)
    if user_msg:
        result = MeltanoRunResult(
            exit_code=exit_code,
            stdout=stdout_str,
            stderr=stderr_str,
            success=success,
            error_type="cdc_setup",
            user_message=user_msg,
        )
    return result


async def run_meltano_invoke(
    plugin_name: str,
    args: List[str],
    env_overrides: Optional[Dict[str, str]] = None,
    *,
    project_dir: Optional[str] = None,
    timeout_seconds: int = DEFAULT_INVOKE_TIMEOUT_SECONDS,
) -> MeltanoRunResult:
    """Execute `meltano invoke <plugin> <args...>` with dynamic env overrides.

    Used for discovery: meltano invoke tap-postgres --discover
    """
    env_overrides = env_overrides or {}
    work_dir = _resolve_project_dir(project_dir)

    env = os.environ.copy()
    for key, value in env_overrides.items():
        if value is not None:
            env[str(key)] = str(value)

    proc = await asyncio.create_subprocess_exec(
        "meltano",
        "invoke",
        plugin_name,
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=work_dir,
        env=env,
    )

    try:
        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            proc.communicate(),
            timeout=timeout_seconds,
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        return MeltanoRunResult(
            exit_code=-1,
            stdout="",
            stderr=f"Invoke timed out after {timeout_seconds} seconds",
            success=False,
            error_type="timeout",
            user_message=f"Discovery exceeded the timeout of {timeout_seconds} seconds.",
        )

    stdout_str = (stdout_bytes or b"").decode("utf-8", errors="replace")
    stderr_str = (stderr_bytes or b"").decode("utf-8", errors="replace")
    exit_code = proc.returncode or -1
    success = exit_code == 0

    result = MeltanoRunResult(
        exit_code=exit_code,
        stdout=stdout_str,
        stderr=stderr_str,
        success=success,
    )
    user_msg = _infer_user_message(result)
    if user_msg:
        result = MeltanoRunResult(
            exit_code=exit_code,
            stdout=stdout_str,
            stderr=stderr_str,
            success=success,
            error_type="cdc_setup",
            user_message=user_msg,
        )
    return result
