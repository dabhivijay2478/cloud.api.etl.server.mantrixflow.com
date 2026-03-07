"""Policy guard for source-database mutations such as replication slots."""

from __future__ import annotations

import os

TRUE_VALUES = {"1", "true", "yes", "on"}

SOURCE_DB_MUTATION_POLICY_MESSAGE = (
    "CDC and LOG_BASED sync are disabled by policy because they can alter the "
    "client source database (for example, replication slots)."
)


def are_source_db_mutations_allowed() -> bool:
    raw = (
        os.getenv("ALLOW_SOURCE_DB_MUTATIONS_FOR_CDC")
        or os.getenv("ALLOW_SOURCE_DB_MUTATIONS")
        or "false"
    )
    return raw.strip().lower() in TRUE_VALUES
