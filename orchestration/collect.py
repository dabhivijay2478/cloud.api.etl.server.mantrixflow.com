"""Data collection via Meltano invoke.

Runs tap with catalog and state to stream RECORD/STATE messages.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .connection_mapper import SOURCE_TYPE_TO_TAP, connection_config_to_meltano_env
from .meltano_runner import run_meltano_invoke
from utils import ensure_supported_source, parse_singer_stream, temporary_json_file


def _catalog_arg_name(source_type: str) -> str:
    """Postgres uses --properties; MySQL/MongoDB use --catalog."""
    return "--properties" if source_type == "postgresql" else "--catalog"


async def run_collect_via_meltano(
    source_type: str,
    connection_config: Dict[str, Any],
    selected_catalog: Dict[str, Any],
    state: Optional[Dict[str, Any]] = None,
    *,
    tap_config: Optional[Dict[str, Any]] = None,
    timeout_seconds: int = 1200,
) -> Dict[str, Any]:
    """Run tap sync via Meltano invoke; return records and state.

    Args:
        source_type: postgresql, mysql, or mongodb.
        connection_config: API-style connection config.
        selected_catalog: Singer catalog with selected streams.
        state: Optional checkpoint for incremental sync.
        tap_config: Full tap config from connection_config_to_tap_config (table, schema, etc.). If None, uses connection_config.
        timeout_seconds: Max seconds for sync.

    Returns:
        {"records": [...], "state": {...}}

    Raises:
        RuntimeError: If Meltano invoke fails.
    """
    source_type = ensure_supported_source(source_type)
    tap_name = SOURCE_TYPE_TO_TAP.get(source_type)
    if not tap_name:
        raise ValueError(f"No Meltano tap for source_type={source_type}")

    env = connection_config_to_meltano_env(source_type, connection_config, role="extractor")
    catalog_arg = _catalog_arg_name(source_type)
    config_data = tap_config or connection_config

    with temporary_json_file(config_data) as config_path, temporary_json_file(
        selected_catalog
    ) as catalog_path:
        args = ["--config", config_path, catalog_arg, catalog_path]
        if state:
            with temporary_json_file(state) as state_path:
                args.extend(["--state", state_path])
                result = await run_meltano_invoke(
                    tap_name,
                    args,
                    env_overrides=env,
                    timeout_seconds=timeout_seconds,
                )
        else:
            result = await run_meltano_invoke(
                tap_name,
                args,
                env_overrides=env,
                timeout_seconds=timeout_seconds,
            )

    if not result.success:
        msg = result.user_message or result.stderr or result.stdout or f"Exit code {result.exit_code}"
        raise RuntimeError(msg)

    records, new_state = parse_singer_stream(result.stdout)
    return {"records": records, "state": new_state}
