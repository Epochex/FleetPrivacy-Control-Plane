from __future__ import annotations

import argparse
import asyncio
import enum
import logging
import signal
import socket
from datetime import timedelta
from typing import Any

from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from privacy_cloud.config import get_settings
from privacy_cloud.models import (
    PrivacyRequest,
    RequestStatus,
    RequestTask,
    TaskStatus,
    utcnow,
)
from privacy_cloud.services.outbox import relay_outbox_once
from privacy_cloud.services.queue import (
    QueueMessage,
    TaskQueue,
    build_task_queue,
    consume_with_heartbeat,
)
from privacy_cloud.services.source_connectors import RegionalSourceExecutor
from privacy_cloud.services.worker import execute_task, refresh_request_status

logger = logging.getLogger(__name__)


class ClaimResult(str, enum.Enum):
    CLAIMED = "claimed"
    TERMINAL_DUPLICATE = "terminal_duplicate"
    BUSY = "busy"
    MISSING = "missing"


async def claim_message_task(
    session: AsyncSession,
    *,
    task_id: str,
    worker_id: str,
    lease_seconds: int,
) -> ClaimResult:
    """Claim one database task identified by an at-least-once queue message."""

    now = utcnow()
    lease_until = now + timedelta(seconds=lease_seconds)
    task = await session.get(RequestTask, task_id)
    if task is None:
        return ClaimResult.MISSING
    if task.status in {TaskStatus.SUCCEEDED, TaskStatus.FAILED}:
        return ClaimResult.TERMINAL_DUPLICATE

    claimable = or_(
        RequestTask.status == TaskStatus.PENDING,
        and_(RequestTask.status == TaskStatus.RUNNING, RequestTask.lease_expires_at < now),
    )
    result = await session.execute(
        update(RequestTask)
        .where(RequestTask.id == task_id, claimable)
        .values(
            status=TaskStatus.RUNNING,
            attempt=RequestTask.attempt + 1,
            lease_owner=worker_id,
            lease_expires_at=lease_until,
            updated_at=now,
        )
        .returning(RequestTask.request_id)
    )
    request_id = result.scalar_one_or_none()
    if request_id is None:
        await session.rollback()
        return ClaimResult.BUSY
    await session.execute(
        update(PrivacyRequest)
        .where(
            PrivacyRequest.id == request_id,
            PrivacyRequest.status == RequestStatus.QUEUED,
        )
        .values(status=RequestStatus.RUNNING, updated_at=now)
    )
    await session.commit()
    return ClaimResult.CLAIMED


async def handle_task_envelope(
    session_factory: async_sessionmaker[AsyncSession],
    body: dict[str, Any],
    *,
    worker_id: str,
    lease_seconds: int,
    source_executor: RegionalSourceExecutor | None = None,
) -> bool:
    """Use the database task row to absorb SQS duplicate and reordered delivery."""

    if body.get("event_type") != "privacy_task.ready":
        return True
    task_id = str(body.get("payload", {}).get("task_id", ""))
    if not task_id:
        return True
    async with session_factory() as session:
        result = await claim_message_task(
            session,
            task_id=task_id,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
        )
    if result == ClaimResult.MISSING:
        return True
    if result == ClaimResult.TERMINAL_DUPLICATE:
        async with session_factory() as session:
            request_id = await session.scalar(
                select(RequestTask.request_id).where(RequestTask.id == task_id)
            )
            if request_id is not None:
                await refresh_request_status(session, request_id)
        return True
    if result == ClaimResult.BUSY:
        return False
    async with session_factory() as session:
        return await execute_task(
            session,
            task_id=task_id,
            worker_id=worker_id,
            source_executor=source_executor,
        )


async def extend_task_lease(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    task_id: str,
    worker_id: str,
    lease_seconds: int,
) -> bool:
    async with session_factory() as session:
        result = await session.execute(
            update(RequestTask)
            .where(
                RequestTask.id == task_id,
                RequestTask.status == TaskStatus.RUNNING,
                RequestTask.lease_owner == worker_id,
            )
            .values(
                lease_expires_at=utcnow() + timedelta(seconds=lease_seconds),
                updated_at=utcnow(),
            )
        )
        await session.commit()
        return bool(result.rowcount)


async def consume_task_message(
    queue: TaskQueue,
    message: QueueMessage,
    session_factory: async_sessionmaker[AsyncSession],
    *,
    worker_id: str,
    source_executor: RegionalSourceExecutor | None = None,
) -> bool:
    settings = get_settings()

    async def handle(body: dict[str, Any]) -> bool:
        return await handle_task_envelope(
            session_factory,
            body,
            worker_id=worker_id,
            lease_seconds=settings.lease_seconds,
            source_executor=source_executor,
        )

    async def heartbeat_database_lease() -> None:
        if message.body.get("event_type") != "privacy_task.ready":
            return
        task_id = str(message.body.get("payload", {}).get("task_id", ""))
        if task_id:
            await extend_task_lease(
                session_factory,
                task_id=task_id,
                worker_id=worker_id,
                lease_seconds=settings.lease_seconds,
            )

    return await consume_with_heartbeat(
        queue,
        message,
        handle,
        heartbeat_seconds=settings.sqs_heartbeat_seconds,
        visibility_seconds=settings.sqs_visibility_timeout_seconds,
        heartbeat_hook=heartbeat_database_lease,
    )


async def run_worker_pass(
    queue: TaskQueue,
    session_factory: async_sessionmaker[AsyncSession],
    *,
    worker_id: str,
    poll_wait_seconds: int = 10,
    source_executor: RegionalSourceExecutor | None = None,
) -> dict[str, int]:
    async with session_factory() as session:
        relay = await relay_outbox_once(session, queue)
    messages = await queue.receive(max_messages=10, wait_seconds=poll_wait_seconds)

    async def consume(message: QueueMessage) -> bool:
        try:
            return await consume_task_message(
                queue,
                message,
                session_factory,
                worker_id=worker_id,
                source_executor=source_executor,
            )
        except Exception:
            logger.exception(
                "queue message processing failed",
                extra={"message_id": message.message_id},
            )
            return False

    results = await asyncio.gather(*(consume(message) for message in messages))
    return {
        "outbox_published": relay["published"],
        "received": len(messages),
        "acknowledged": sum(results),
    }


async def run_worker(*, once: bool = False, poll_wait_seconds: int = 10) -> None:
    from privacy_cloud.db import SessionFactory, engine

    settings = get_settings()
    queue = build_task_queue(settings)
    if settings.source_backend == "database":
        source_executor = None
    elif settings.source_backend == "http":
        source_executor = RegionalSourceExecutor.from_settings(settings)
    else:
        raise ValueError(f"unsupported source backend: {settings.source_backend}")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(name, stop.set)
        except NotImplementedError:
            pass

    worker_id = f"{socket.gethostname()}-{id(stop):x}"
    while not stop.is_set():
        result = await run_worker_pass(
            queue,
            SessionFactory,
            worker_id=worker_id,
            poll_wait_seconds=poll_wait_seconds,
            source_executor=source_executor,
        )
        logger.info("worker pass completed", extra=result)
        if once:
            break
    if source_executor is not None:
        await source_executor.close()
    await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Relay privacy tasks to SQS and consume them")
    parser.add_argument("--once", action="store_true", help="run one relay and receive pass")
    parser.add_argument("--poll-wait", type=int, default=10, choices=range(21))
    arguments = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_worker(once=arguments.once, poll_wait_seconds=arguments.poll_wait))


if __name__ == "__main__":
    main()
