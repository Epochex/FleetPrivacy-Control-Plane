from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from privacy_cloud.metrics import REQUEST_CREATED
from privacy_cloud.models import PrivacyRequest, RequestStatus, RequestTask
from privacy_cloud.schemas import PrivacyRequestCreate
from privacy_cloud.security import hash_subject
from privacy_cloud.services.events import append_audit, enqueue_outbox


class IdempotencyConflict(ValueError):
    pass


def _same_command(request: PrivacyRequest, command: PrivacyRequestCreate) -> bool:
    return (
        request.subject_key_hash == hash_subject(command.subject_key)
        and request.kind == command.kind
        and sorted(request.requested_sources) == sorted(command.sources)
    )


async def _find_by_idempotency(
    session: AsyncSession, tenant_id: str, idempotency_key: str
) -> PrivacyRequest | None:
    return (
        await session.execute(
            select(PrivacyRequest)
            .options(selectinload(PrivacyRequest.tasks))
            .where(
                PrivacyRequest.tenant_id == tenant_id,
                PrivacyRequest.idempotency_key == idempotency_key,
            )
        )
    ).scalar_one_or_none()


async def create_privacy_request(
    session: AsyncSession,
    *,
    tenant_id: str,
    idempotency_key: str,
    command: PrivacyRequestCreate,
) -> tuple[PrivacyRequest, bool]:
    """Create a request and one source task atomically; return (request, created)."""
    existing = await _find_by_idempotency(session, tenant_id, idempotency_key)
    if existing:
        if not _same_command(existing, command):
            raise IdempotencyConflict("idempotency key already represents a different request")
        return existing, False

    request = PrivacyRequest(
        tenant_id=tenant_id,
        idempotency_key=idempotency_key,
        subject_key_hash=hash_subject(command.subject_key),
        kind=command.kind,
        status=RequestStatus.QUEUED,
        requested_sources=command.sources,
    )
    request.tasks = [RequestTask(tenant_id=tenant_id, source=source) for source in command.sources]
    session.add(request)
    try:
        await session.flush()
        enqueue_outbox(
            session,
            tenant_id=tenant_id,
            aggregate_id=request.id,
            event_type="privacy_request.created",
            payload={"kind": request.kind.value, "sources": request.requested_sources},
        )
        await append_audit(
            session,
            tenant_id=tenant_id,
            request_id=request.id,
            action="request.created",
            payload={"kind": request.kind.value, "sources": request.requested_sources},
        )
        await session.commit()
    except IntegrityError:
        await session.rollback()
        existing = await _find_by_idempotency(session, tenant_id, idempotency_key)
        if existing is None:
            raise
        if not _same_command(existing, command):
            raise IdempotencyConflict("idempotency key already represents a different request")
        return existing, False

    REQUEST_CREATED.labels(kind=request.kind.value).inc()
    return await get_privacy_request(session, tenant_id=tenant_id, request_id=request.id), True


async def get_privacy_request(
    session: AsyncSession, *, tenant_id: str, request_id: str
) -> PrivacyRequest | None:
    return (
        await session.execute(
            select(PrivacyRequest)
            .options(selectinload(PrivacyRequest.tasks))
            .where(PrivacyRequest.id == request_id, PrivacyRequest.tenant_id == tenant_id)
        )
    ).scalar_one_or_none()


async def list_privacy_requests(
    session: AsyncSession,
    *,
    tenant_id: str,
    status: RequestStatus | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[PrivacyRequest]:
    statement = (
        select(PrivacyRequest)
        .options(selectinload(PrivacyRequest.tasks))
        .where(PrivacyRequest.tenant_id == tenant_id)
        .order_by(PrivacyRequest.created_at.desc())
        .limit(limit)
        .offset(offset)
    )
    if status is not None:
        statement = statement.where(PrivacyRequest.status == status)
    return list((await session.execute(statement)).scalars().unique())
