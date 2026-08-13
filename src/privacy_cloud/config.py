from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PRIVACY_CLOUD_", env_file=".env")

    database_url: str = "sqlite+aiosqlite:///./privacy_cloud.db"
    api_key: str = "dev-api-key"
    webhook_secret: str = "dev-webhook-secret"
    artifact_dir: str = "artifacts"
    worker_batch_size: int = 32
    lease_seconds: int = 60


@lru_cache
def get_settings() -> Settings:
    return Settings()
