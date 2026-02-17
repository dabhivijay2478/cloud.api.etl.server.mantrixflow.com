"""Schema discovery via Meltano invoke.

Runs `meltano invoke <tap> --discover` with dynamic connection config.
"""

from __future__ import annotations

from typing import Any, Dict

from .connection_mapper import SOURCE_TYPE_TO_TAP, connection_config_to_meltano_env
from .meltano_runner import run_meltano_invoke
from utils import ensure_supported_source, parse_discovery_output


async def run_discovery(
    source_type: str,
    connection_config: Dict[str, Any],
    *,
    source_config: Dict[str, Any] | None = None,
    timeout_seconds: int = 120,
) -> Dict[str, Any]:
    """Run schema discovery via Meltano invoke.

    Args:
        source_type: One of postgresql, mysql, mongodb.
        connection_config: API-style connection config (host, port, user, password, database).
        source_config: Optional overrides (table, schema) - merged into connection for taps that support it.
        timeout_seconds: Max seconds for discovery.

    Returns:
        Singer catalog dict with "streams" key.

    Raises:
        RuntimeError: If Meltano invoke fails (with user_message when available).
    """
    source_type = ensure_supported_source(source_type)
    tap_name = SOURCE_TYPE_TO_TAP.get(source_type)
    if not tap_name:
        raise ValueError(f"No Meltano tap for source_type={source_type}")

    env = connection_config_to_meltano_env(source_type, connection_config, role="extractor")
    if source_config:
        for key, value in source_config.items():
            if value is not None:
                env_key = f"MELTANO_EXTRACTOR_{tap_name.upper().replace('-', '_')}_{key.upper()}"
                env[env_key] = str(value)

    result = await run_meltano_invoke(
        tap_name,
        ["--discover"],
        env_overrides=env,
        timeout_seconds=timeout_seconds,
    )

    if not result.success:
        msg = result.user_message or result.stderr or result.stdout or f"Exit code {result.exit_code}"
        raise RuntimeError(msg)

    return parse_discovery_output(result.stdout)
