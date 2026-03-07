"""Tap and target executable registry. No hardcoded names — all from env."""

from __future__ import annotations

import json
import logging
import os

logger = logging.getLogger("etl.registry")


def _get_from_registry(env_key: str, source_type: str) -> str | None:
    """Look up executable from TAP_REGISTRY or TARGET_REGISTRY JSON env."""
    raw = os.getenv(env_key)
    if not raw or not raw.strip():
        return None
    try:
        registry = json.loads(raw)
        if isinstance(registry, dict):
            key = source_type.lower()
            return registry.get(key) or registry.get(source_type)
    except json.JSONDecodeError:
        logger.warning("Invalid JSON in %s", env_key)
    return None


def get_tap_executable(source_type: str = "postgres") -> str:
    """Get tap executable name for source type. Reads from env, no hardcoded defaults.

    Env vars (in order of precedence):
    - TAP_REGISTRY: JSON dict e.g. {"postgres":"tap-postgres","mysql":"tap-mysql"}
    - TAP_POSTGRES: for postgres source (when TAP_REGISTRY not used)
    - TAP_<UPPER_TYPE>: e.g. TAP_MYSQL for mysql

    Raises ValueError if not configured.
    """
    # Try registry first
    exe = _get_from_registry("TAP_REGISTRY", source_type)
    if exe:
        return exe

    # Try type-specific env (TAP_POSTGRES, TAP_MYSQL, etc.)
    type_key = f"TAP_{source_type.upper().replace('-', '_')}"
    exe = os.getenv(type_key)
    if exe and exe.strip():
        return exe.strip()

    raise ValueError(
        f"Tap executable not configured for source_type={source_type}. "
        "Set TAP_REGISTRY (JSON) or TAP_POSTGRES (or TAP_<TYPE>) in .env. See .env.example."
    )


def get_target_executable(dest_type: str = "postgres") -> str:
    """Get target executable name for destination type. Reads from env, no hardcoded defaults.

    Env vars (in order of precedence):
    - TARGET_REGISTRY: JSON dict e.g. {"postgres":"target-postgres"}
    - TARGET_POSTGRES: for postgres destination
    - TARGET_<UPPER_TYPE>: e.g. TARGET_SNOWFLAKE

    Raises ValueError if not configured.
    """
    exe = _get_from_registry("TARGET_REGISTRY", dest_type)
    if exe:
        return exe

    type_key = f"TARGET_{dest_type.upper().replace('-', '_')}"
    exe = os.getenv(type_key)
    if exe and exe.strip():
        return exe.strip()

    raise ValueError(
        f"Target executable not configured for dest_type={dest_type}. "
        "Set TARGET_REGISTRY (JSON) or TARGET_POSTGRES (or TARGET_<TYPE>) in .env. See .env.example."
    )
