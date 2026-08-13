from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from privacy_cloud.models import OutboxEvent, utcnow
from privacy_cloud.services.queue import TaskQueue

logger = logging.getLogger(__name__)


def event_envelope(event: OutboxEvent) -> dict[str, Any]:
    return {
        "event_id": event.id,
        "tenant_id": event.tenant_id,
        "aggregate_id": event.aggregate_id,
        "event_type": event.event_type,
        "payload": event.payload,
        "created_at": event.created_at.isoformat(),
    }


async def relay_outbox_once(
    session: AsyncSession,
    queue: TaskQueue,
    *,
    batch_size: int = 100,
) -> dict[str, int]:
    """Publish committed outbox rows and persist each confirmed publication."""

    statement = (
        select(OutboxEvent)
        .where(OutboxEvent.published_at.is_(None))
        .order_by(OutboxEvent.created_at.asc())
        .limit(batch_size)
    )
    dialect = session.bind.dialect.name if session.bind is not None else ""
    if dialect == "postgresql":
        statement = statement.with_for_update(skip_locked=True)

    events = list((await session.execute(statement)).scalars())
    published = 0
    for event in events:
        try:
            await queue.publish(event_envelope(event))
        except Exception:
            logger.exception("outbox publication failed", extra={"outbox_event_id": event.id})
            continue
        event.published_at = utcnow()
        published += 1
    await session.commit()
    return {"claimed": len(events), "published": published, "failed": len(events) - published}
