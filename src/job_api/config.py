from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    mongo_uri: str = "mongodb://127.0.0.1:27017"
    mongo_database: str = "job_api"
    rabbitmq_url: str = "amqp://guest:guest@127.0.0.1:5672/%2F"
    rabbitmq_management_url: str = "http://127.0.0.1:15672"
    rabbitmq_management_user: str = "guest"
    rabbitmq_management_password: str = "guest"
    rabbitmq_vhost: str = "/"
    work_queue: str = "jobs.work"
    dead_queue: str = "jobs.dead"
    dead_exchange: str = "jobs.dead"
    worker_count: int = Field(default=3, ge=1, le=64)
    poll_interval_seconds: float = Field(default=5, gt=0, le=30)
    backoff_base_seconds: float = Field(default=1, gt=0)
    backoff_cap_seconds: float = Field(default=30, gt=0)
    recovery_grace_seconds: float = Field(default=5, gt=0)
    shutdown_grace_seconds: float = Field(default=10, gt=0)
    worker_stale_seconds: float = Field(default=15, ge=5)
    dispatch_lease_seconds: float = Field(default=30, ge=10)
    enable_test_handlers: bool = False

    @model_validator(mode="after")
    def validate_backoff(self) -> "Settings":
        if self.backoff_cap_seconds < self.backoff_base_seconds:
            raise ValueError("backoff cap must be at least the backoff base")
        return self
