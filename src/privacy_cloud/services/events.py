from __future__ import annotations

import hashlib
import json
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from privacy_cloud.models import AuditEvent, OutboxEvent


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True, default=str)


def _postgres_advisory_key(tenant_id: str) -> int:
    """Map a tenant identifier to PostgreSQL's signed 64-bit advisory-lock key."""
    value = int.from_bytes(hashlib.sha256(tenant_id.encode()).digest()[:8], "big")
    return value - (1 << 64) if value >= (1 << 63) else value


async def append_audit(
    session: AsyncSession,
    *,
    tenant_id: str,
    request_id: str,
    action: str,
    payload: dict[str, Any] | None = None,
) -> AuditEvent:
    """Append a tamper-evident tenant audit event in the caller's transaction."""
    payload = payload or {}
    dialect = session.bind.dialect.name if session.bind is not None else ""
    if dialect == "postgresql":
        # Held until commit/rollback, serializing the chain-head read and append per tenant.
        await session.execute(select(func.pg_advisory_xact_lock(_postgres_advisory_key(tenant_id))))
    last = (
        await session.execute(
            select(AuditEvent)
            .where(AuditEvent.tenant_id == tenant_id)
            .order_by(AuditEvent.sequence.desc())
            .limit(1)
            .with_for_update()
        )
    ).scalar_one_or_none()
    sequence = 1 if last is None else last.sequence + 1
    previous_hash = "0" * 64 if last is None else last.event_hash
    payload_hash = hashlib.sha256(_canonical_json(payload).encode()).hexdigest()
    envelope = {
        "tenant_id": tenant_id,
        "request_id": request_id,
        "sequence": sequence,
        "action": action,
        "payload_hash": payload_hash,
        "previous_hash": previous_hash,
    }
    event_hash = hashlib.sha256(_canonical_json(envelope).encode()).hexdigest()
    event = AuditEvent(
        tenant_id=tenant_id,
        request_id=request_id,
        sequence=sequence,
        action=action,
        payload_hash=payload_hash,
        previous_hash=previous_hash,
        event_hash=event_hash,
    )
    session.add(event)
    await session.flush()
    return event


def enqueue_outbox(
    session: AsyncSession,
    *,
    tenant_id: str,
    aggregate_id: str,
    event_type: str,
    payload: dict[str, Any] | None = None,
) -> OutboxEvent:
    event = OutboxEvent(
        tenant_id=tenant_id,
        aggregate_id=aggregate_id,
        event_type=event_type,
        payload=payload or {},
    )
    session.add(event)
    return event


async def verify_audit_chain(session: AsyncSession, tenant_id: str) -> bool:
    events = (
        await session.execute(
            select(AuditEvent)
            .where(AuditEvent.tenant_id == tenant_id)
            .order_by(AuditEvent.sequence.asc())
        )
    ).scalars()
    previous_hash = "0" * 64
    expected_sequence = 1
    for event in events:
        if event.sequence != expected_sequence or event.previous_hash != previous_hash:
            return False
        envelope = {
            "tenant_id": event.tenant_id,
            "request_id": event.request_id,
            "sequence": event.sequence,
            "action": event.action,
            "payload_hash": event.payload_hash,
            "previous_hash": event.previous_hash,
        }
        if hashlib.sha256(_canonical_json(envelope).encode()).hexdigest() != event.event_hash:
            return False
        previous_hash = event.event_hash
        expected_sequence += 1
    return True


async def unpublished_outbox_count(session: AsyncSession) -> int:
    return int(
        await session.scalar(
            select(func.count()).select_from(OutboxEvent).where(OutboxEvent.published_at.is_(None))
        )
        or 0
    )
