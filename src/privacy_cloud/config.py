from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PRIVACY_CLOUD_", env_file=".env")

    database_url: str = "sqlite+aiosqlite:///./privacy_cloud.db"
    database_pool_size: int = 10
    database_max_overflow: int = 20
    database_pool_timeout_seconds: int = 10
    redis_url: str = ""
    source_backend: str = "database"
    regional_source_base_url: str = ""
    regional_source_token: str = ""
    source_request_timeout_seconds: float = 10.0
    source_admission_timeout_seconds: float = 30.0
    max_task_attempts: int = 5
    api_key: str = "dev-api-key"
    webhook_secret: str = "dev-webhook-secret"
    artifact_dir: str = "artifacts"
    worker_batch_size: int = 32
    lease_seconds: int = 60
    queue_backend: str = "local"
    artifact_backend: str = "local"
    aws_region: str = "eu-west-1"
    aws_endpoint_url: str = ""
    sqs_queue_url: str = ""
    sqs_visibility_timeout_seconds: int = 60
    sqs_heartbeat_seconds: int = 20
    s3_bucket: str = ""
    s3_prefix: str = "privacy-artifacts"
    s3_kms_key_id: str = ""
    artifact_presign_seconds: int = 300


@lru_cache
def get_settings() -> Settings:
    return Settings()
