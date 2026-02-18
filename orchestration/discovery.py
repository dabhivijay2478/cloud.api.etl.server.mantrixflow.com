"""Schema discovery via direct tap invocation.

Runs the tap executable directly with --config file, bypassing Meltano.
Meltano may skip env var parsing; direct invocation ensures our config is used.
"""

from __future__ import annotations

from typing import Any, Dict

from .connection_mapper import SOURCE_TYPE_TO_TAP, connection_config_to_tap_config
from .meltano_runner import run_tap_direct
from utils import ensure_supported_source, parse_discovery_output, temporary_json_file


def _distill_error_message(raw: str) -> str:
    """Extract a user-friendly error from tap stderr/traceback."""
    lines = [l.strip() for l in raw.splitlines() if l.strip()]
    # Prefer the last exception line (actual error)
    for line in reversed(lines):
        if any(
            x in line.lower()
            for x in (
                "error",
                "exception",
                "failed",
                "fatal",
                "authentication",
                "connection refused",
                "ssl",
                "timeout",
            )
        ) and "traceback" not in line.lower() and "file " not in line:
            return line
    # Fallback: last non-empty line
    for line in reversed(lines):
        if line and not line.startswith("File ") and "|" not in line:
            return line
    return raw[:500] if raw else "Connection failed"


async def run_discovery(
    source_type: str,
    connection_config: Dict[str, Any],
    *,
    source_config: Dict[str, Any] | None = None,
    timeout_seconds: int = 120,
) -> Dict[str, Any]:
    """Run schema discovery by invoking the tap directly with --config file.

    Args:
        source_type: One of postgresql, mysql, mongodb.
        connection_config: API-style connection config (host, port, user, password, database).
        source_config: Optional overrides (table, schema) - merged into connection for taps that support it.
        timeout_seconds: Max seconds for discovery.

    Returns:
        Singer catalog dict with "streams" key.

    Raises:
        RuntimeError: If discovery fails (with user_message when available).
    """
    source_type = ensure_supported_source(source_type)
    tap_name = SOURCE_TYPE_TO_TAP.get(source_type)
    if not tap_name:
        raise ValueError(f"No Meltano tap for source_type={source_type}")

    # Build tap config (sqlalchemy_url, etc.) and pass via --config file.
    # Bypass Meltano entirely: it may skip env var parsing and ignore --config.
    tap_config = connection_config_to_tap_config(
        source_type, connection_config, source_config=source_config
    )

    with temporary_json_file(tap_config) as config_path:
        result = await run_tap_direct(
            tap_name,
            config_path,
            args=["--discover"],
            timeout_seconds=timeout_seconds,
        )

    if not result.success:
        raw = result.user_message or result.stderr or result.stdout or f"Exit code {result.exit_code}"
        # Extract user-friendly error; avoid surfacing noisy logs like "Skipping parse of env var"
        msg = _distill_error_message(raw)
        raise RuntimeError(msg)

    return parse_discovery_output(result.stdout)
