"""Environment configuration via Pydantic BaseSettings."""

from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """ETL server configuration from environment variables."""

    SUPABASE_URL: str = ""
    SUPABASE_SERVICE_ROLE_KEY: str = ""
    ENCRYPTION_KEY: str = ""
    CALLBACK_URL: str = ""
    ETL_INTERNAL_TOKEN: str = ""
    CALLBACK_TOKEN: str = ""
    MAX_CONCURRENT_RUNS: int = 20
    DEFAULT_SYNC_TIMEOUT_SECONDS: int = 300
    LOG_LEVEL: str = "INFO"
    PORT: int = 8000

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
