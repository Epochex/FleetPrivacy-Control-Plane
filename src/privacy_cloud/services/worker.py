from __future__ import annotations

import time
from datetime import timedelta
from typing import Any

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from privacy_cloud.config import get_settings
from privacy_cloud.metrics import REQUEST_COMPLETED, TASK_DURATION
from privacy_cloud.models import (
    DeviceCloudRecord,
    PrivacyRequest,
    RequestKind,
    RequestStatus,
    RequestTask,
    TaskStatus,
    utcnow,
)
from privacy_cloud.services.artifacts import ArtifactStore, build_artifact_store
from privacy_cloud.services.events import append_audit, enqueue_outbox
from privacy_cloud.services.source_connectors import RegionalSourceExecutor, SourceOperation


def _claimable(now: Any) -> Any:
    return or_(
        RequestTask.status == TaskStatus.PENDING,
        and_(RequestTask.status == TaskStatus.RUNNING, RequestTask.lease_expires_at < now),
    )


async def claim_tasks(
    session: AsyncSession,
    *,
    worker_id: str,
    tenant_id: str | None = None,
    batch_size: int | None = None,
    lease_seconds: int | None = None,
) -> list[str]:
    """Lease tasks with PostgreSQL SKIP LOCKED and an atomic SQLite fallback."""
    settings = get_settings()
    batch_size = batch_size or settings.worker_batch_size
    lease_seconds = lease_seconds or settings.lease_seconds
    now = utcnow()
    lease_until = now + timedelta(seconds=lease_seconds)
    dialect = session.bind.dialect.name if session.bind is not None else ""

    claim_filter = _claimable(now)
    if tenant_id is not None:
        claim_filter = and_(claim_filter, RequestTask.tenant_id == tenant_id)

    if dialect == "postgresql":
        tasks = list(
            (
                await session.execute(
                    select(RequestTask)
                    .where(claim_filter)
                    .order_by(RequestTask.created_at.asc())
                    .limit(batch_size)
                    .with_for_update(skip_locked=True)
                )
            ).scalars()
        )
        task_ids = [task.id for task in tasks]
        for task in tasks:
            task.status = TaskStatus.RUNNING
            task.attempt += 1
            task.lease_owner = worker_id
            task.lease_expires_at = lease_until
    else:
        candidate_ids = (
            select(RequestTask.id)
            .where(claim_filter)
            .order_by(RequestTask.created_at.asc())
            .limit(batch_size)
            .scalar_subquery()
        )
        result = await session.execute(
            update(RequestTask)
            .where(RequestTask.id.in_(candidate_ids), claim_filter)
            .values(
                status=TaskStatus.RUNNING,
                attempt=RequestTask.attempt + 1,
                lease_owner=worker_id,
                lease_expires_at=lease_until,
                updated_at=now,
            )
            .returning(RequestTask.id)
        )
        task_ids = list(result.scalars())

    if task_ids:
        request_ids = select(RequestTask.request_id).where(RequestTask.id.in_(task_ids))
        await session.execute(
            update(PrivacyRequest)
            .where(
                PrivacyRequest.id.in_(request_ids),
                PrivacyRequest.status == RequestStatus.QUEUED,
            )
            .values(status=RequestStatus.RUNNING, updated_at=now)
        )
    await session.commit()
    return task_ids


async def _active_records(session: AsyncSession, task: RequestTask) -> list[DeviceCloudRecord]:
    request = task.request
    return list(
        (
            await session.execute(
                select(DeviceCloudRecord).where(
                    DeviceCloudRecord.tenant_id == task.tenant_id,
                    DeviceCloudRecord.subject_key_hash == request.subject_key_hash,
                    DeviceCloudRecord.source == task.source,
                    DeviceCloudRecord.deleted_at.is_(None),
                )
            )
        ).scalars()
    )


async def _execute_access(session: AsyncSession, task: RequestTask) -> dict[str, Any]:
    rows = await _active_records(session, task)
    return {
        "record_count": len(rows),
        "records": [{"id": row.id, "payload": row.payload} for row in rows],
        "verified": True,
    }


async def _execute_delete(session: AsyncSession, task: RequestTask) -> dict[str, Any]:
    rows = await _active_records(session, task)
    deleted_at = utcnow()
    for row in rows:
        row.deleted_at = deleted_at
    await session.flush()
    remaining = await session.scalar(
        select(func.count())
        .select_from(DeviceCloudRecord)
        .where(
            DeviceCloudRecord.tenant_id == task.tenant_id,
            DeviceCloudRecord.subject_key_hash == task.request.subject_key_hash,
            DeviceCloudRecord.source == task.source,
            DeviceCloudRecord.deleted_at.is_(None),
        )
    )
    if remaining:
        raise RuntimeError(f"reverse verification found {remaining} active records")
    return {"deleted_count": len(rows), "remaining_count": 0, "verified": True}


async def _write_artifact(
    session: AsyncSession,
    request: PrivacyRequest,
    artifact_store: ArtifactStore | None = None,
) -> str:
    settings = get_settings()
    artifact_store = artifact_store or build_artifact_store(settings)
    document = {
        "request_id": request.id,
        "tenant_id": request.tenant_id,
        "kind": request.kind.value,
        "policy_version": request.policy_version,
        "sources": {task.source: task.result_summary for task in request.tasks},
        "generated_at": utcnow().isoformat(),
    }
    reference = await artifact_store.put_json(
        tenant_id=request.tenant_id,
        request_id=request.id,
        document=document,
    )
    return reference.uri


async def refresh_request_status(
    session: AsyncSession,
    request_id: str,
    *,
    artifact_store: ArtifactStore | None = None,
) -> RequestStatus:
    request = (
        await session.execute(
            select(PrivacyRequest)
            .options(selectinload(PrivacyRequest.tasks))
            .where(PrivacyRequest.id == request_id)
            .with_for_update()
        )
    ).scalar_one()
    statuses = {task.status for task in request.tasks}
    previous_status = request.status
    if not statuses or statuses <= {TaskStatus.PENDING}:
        status = RequestStatus.QUEUED
    elif TaskStatus.PENDING in statuses or TaskStatus.RUNNING in statuses:
        status = RequestStatus.RUNNING
    elif statuses == {TaskStatus.SUCCEEDED}:
        status = RequestStatus.COMPLETED
    elif statuses == {TaskStatus.FAILED}:
        status = RequestStatus.FAILED
    else:
        status = RequestStatus.PARTIAL
    request.status = status
    if status != previous_status:
        request.version += 1

    became_terminal = status in {
        RequestStatus.COMPLETED,
        RequestStatus.PARTIAL,
        RequestStatus.FAILED,
    } and previous_status not in {
        RequestStatus.COMPLETED,
        RequestStatus.PARTIAL,
        RequestStatus.FAILED,
    }
    needs_access_artifact = (
        request.kind == RequestKind.ACCESS
        and status in {RequestStatus.COMPLETED, RequestStatus.PARTIAL}
        and request.artifact_path is None
    )
    if needs_access_artifact:
        request.artifact_path = await _write_artifact(
            session,
            request,
            artifact_store=artifact_store,
        )
    if became_terminal:
        summary = {
            "status": status.value,
            "succeeded": sum(task.status == TaskStatus.SUCCEEDED for task in request.tasks),
            "failed": sum(task.status == TaskStatus.FAILED for task in request.tasks),
        }
        enqueue_outbox(
            session,
            tenant_id=request.tenant_id,
            aggregate_id=request.id,
            event_type=f"privacy_request.{status.value}",
            payload=summary,
        )
        await append_audit(
            session,
            tenant_id=request.tenant_id,
            request_id=request.id,
            action=f"request.{status.value}",
            payload=summary,
        )
        REQUEST_COMPLETED.labels(kind=request.kind.value, status=status.value).inc()
    await session.commit()
    return status


async def execute_task(
    session: AsyncSession,
    *,
    task_id: str,
    worker_id: str,
    source_executor: RegionalSourceExecutor | None = None,
) -> bool:
    task = (
        await session.execute(
            select(RequestTask)
            .options(selectinload(RequestTask.request))
            .where(
                RequestTask.id == task_id,
                RequestTask.status == TaskStatus.RUNNING,
                RequestTask.lease_owner == worker_id,
            )
        )
    ).scalar_one_or_none()
    if task is None:
        return False

    started = time.perf_counter()
    try:
        if source_executor is not None:
            if task.request.kind == RequestKind.DELETE:
                task.request.status = RequestStatus.VERIFYING
            receipt = await source_executor.execute(
                SourceOperation(
                    task_id=task.id,
                    tenant_id=task.tenant_id,
                    source=task.source,
                    subject_key_hash=task.request.subject_key_hash,
                    kind=task.request.kind.value,
                )
            )
            if not receipt.succeeded:
                raise RuntimeError(f"regional source returned HTTP {receipt.status_code}")
            result = receipt.payload
        elif task.request.kind == RequestKind.ACCESS:
            result = await _execute_access(session, task)
        else:
            task.request.status = RequestStatus.VERIFYING
            result = await _execute_delete(session, task)
        task.result_summary = result
        task.status = TaskStatus.SUCCEEDED
        task.error = None
    except Exception as exc:  # noqa: BLE001 - task failures are persisted as workflow state
        settings = get_settings()
        task.status = (
            TaskStatus.PENDING
            if source_executor is not None and task.attempt < settings.max_task_attempts
            else TaskStatus.FAILED
        )
        task.error = str(exc)[:2000]
    finally:
        task.lease_owner = None
        task.lease_expires_at = None
        TASK_DURATION.labels(source=task.source, kind=task.request.kind.value).observe(
            time.perf_counter() - started
        )
        await append_audit(
            session,
            tenant_id=task.tenant_id,
            request_id=task.request_id,
            action=(
                "task.retry_scheduled"
                if task.status == TaskStatus.PENDING
                else f"task.{task.status.value}"
            ),
            payload={"source": task.source, "attempt": task.attempt, "error": task.error},
        )
        await session.commit()
    await refresh_request_status(session, task.request_id)
    return task.status == TaskStatus.SUCCEEDED


async def process_once(
    session: AsyncSession,
    *,
    worker_id: str,
    tenant_id: str | None = None,
    batch_size: int | None = None,
) -> dict[str, int]:
    task_ids = await claim_tasks(
        session,
        worker_id=worker_id,
        tenant_id=tenant_id,
        batch_size=batch_size,
    )
    succeeded = 0
    for task_id in task_ids:
        succeeded += int(await execute_task(session, task_id=task_id, worker_id=worker_id))
    return {"claimed": len(task_ids), "succeeded": succeeded, "failed": len(task_ids) - succeeded}
