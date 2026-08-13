from __future__ import annotations

from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response, status
from fastapi.responses import FileResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from privacy_cloud.config import get_settings
from privacy_cloud.db import get_session
from privacy_cloud.models import PrivacyRequest, RequestStatus
from privacy_cloud.schemas import PrivacyRequestCreate, PrivacyRequestView, SeedRecordsRequest
from privacy_cloud.security import tenant_context
from privacy_cloud.services.events import unpublished_outbox_count, verify_audit_chain
from privacy_cloud.services.records import seed_records
from privacy_cloud.services.requests import (
    IdempotencyConflict,
    create_privacy_request,
    get_privacy_request,
    list_privacy_requests,
)
from privacy_cloud.services.worker import process_once

router = APIRouter()
Session = Annotated[AsyncSession, Depends(get_session)]
Tenant = Annotated[str, Depends(tenant_context)]


@router.post("/privacy-requests", response_model=PrivacyRequestView)
async def create_request(
    command: PrivacyRequestCreate,
    response: Response,
    session: Session,
    tenant_id: Tenant,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=128)],
) -> PrivacyRequest:
    try:
        request, created = await create_privacy_request(
            session,
            tenant_id=tenant_id,
            idempotency_key=idempotency_key,
            command=command,
        )
    except IdempotencyConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    response.status_code = status.HTTP_201_CREATED if created else status.HTTP_200_OK
    return request


@router.get("/privacy-requests", response_model=list[PrivacyRequestView])
async def list_requests(
    session: Session,
    tenant_id: Tenant,
    request_status: Annotated[RequestStatus | None, Query(alias="status")] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[PrivacyRequest]:
    return await list_privacy_requests(
        session,
        tenant_id=tenant_id,
        status=request_status,
        limit=limit,
        offset=offset,
    )


@router.get("/privacy-requests/{request_id}", response_model=PrivacyRequestView)
async def get_request(request_id: str, session: Session, tenant_id: Tenant) -> PrivacyRequest:
    request = await get_privacy_request(session, tenant_id=tenant_id, request_id=request_id)
    if request is None:
        raise HTTPException(status_code=404, detail="privacy request not found")
    return request


@router.get("/privacy-requests/{request_id}/artifact")
async def download_artifact(request_id: str, session: Session, tenant_id: Tenant) -> FileResponse:
    request = await get_privacy_request(session, tenant_id=tenant_id, request_id=request_id)
    if request is None:
        raise HTTPException(status_code=404, detail="privacy request not found")
    if not request.artifact_path:
        raise HTTPException(status_code=409, detail="artifact is not ready")
    path = Path(request.artifact_path).resolve()
    artifact_root = Path(get_settings().artifact_dir).resolve()
    if artifact_root not in path.parents or not path.is_file():
        raise HTTPException(status_code=404, detail="artifact file not found")
    return FileResponse(path, media_type="application/json", filename=f"{request_id}.json")


@router.post("/admin/seed", status_code=201)
async def seed(
    command: SeedRecordsRequest,
    session: Session,
    tenant_id: Tenant,
) -> dict[str, int]:
    return {"created": await seed_records(session, tenant_id=tenant_id, command=command)}


@router.post("/admin/process-once")
async def run_worker_once(
    session: Session,
    tenant_id: Tenant,
    worker_id: Annotated[str, Query(min_length=2, max_length=64)] = "api-worker",
    batch_size: Annotated[int | None, Query(ge=1, le=1000)] = None,
) -> dict[str, int]:
    scoped_worker_id = f"{tenant_id}:{worker_id}"
    return await process_once(
        session,
        worker_id=scoped_worker_id,
        tenant_id=tenant_id,
        batch_size=batch_size,
    )


@router.get("/tenants/audit-chain")
async def audit_chain(session: Session, tenant_id: Tenant) -> dict[str, bool | str]:
    return {"tenant_id": tenant_id, "valid": await verify_audit_chain(session, tenant_id)}


@router.get("/healthz")
async def health(session: Session) -> dict[str, str | int]:
    await session.execute(text("SELECT 1"))
    return {"status": "ok", "unpublished_outbox": await unpublished_outbox_count(session)}


@router.get("/health", include_in_schema=False)
async def health_alias(session: Session) -> dict[str, str | int]:
    return await health(session)


@router.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
