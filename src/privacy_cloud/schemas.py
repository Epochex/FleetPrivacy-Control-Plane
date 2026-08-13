from datetime import datetime

from pydantic import BaseModel, Field, field_validator

from privacy_cloud.models import RequestKind, RequestStatus, TaskStatus

ALLOWED_SOURCES = {"profile", "devices", "telemetry", "jobs", "support_logs"}


class PrivacyRequestCreate(BaseModel):
    subject_key: str = Field(min_length=3, max_length=320)
    kind: RequestKind
    sources: list[str] = Field(default_factory=lambda: sorted(ALLOWED_SOURCES))

    @field_validator("sources")
    @classmethod
    def validate_sources(cls, value: list[str]) -> list[str]:
        unique = list(dict.fromkeys(value))
        unknown = set(unique) - ALLOWED_SOURCES
        if unknown:
            raise ValueError(f"unknown sources: {sorted(unknown)}")
        return unique


class TaskView(BaseModel):
    source: str
    status: TaskStatus
    attempt: int
    result_summary: dict
    error: str | None

    model_config = {"from_attributes": True}


class PrivacyRequestView(BaseModel):
    id: str
    tenant_id: str
    kind: RequestKind
    status: RequestStatus
    requested_sources: list[str]
    artifact_path: str | None
    version: int
    created_at: datetime
    updated_at: datetime
    tasks: list[TaskView] = []

    model_config = {"from_attributes": True}


class SeedRecordsRequest(BaseModel):
    subject_key: str
    records_per_source: int = Field(default=1, ge=1, le=1000)
