from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from itertools import pairwise
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from privacy_cloud.models import (
    AuditEvent,
    DeviceCloudRecord,
    PrivacyRequest,
    RequestStatus,
    RequestTask,
    TaskStatus,
)
from privacy_cloud.security import hash_subject

TERMINAL = {"completed", "partial", "failed"}


async def create_request(
    client: httpx.AsyncClient,
    headers: dict[str, str],
    *,
    subject: str,
    kind: str,
    sources: list[str],
    key: str,
) -> httpx.Response:
    return await client.post(
        "/v1/privacy-requests",
        headers={**headers, "Idempotency-Key": key},
        json={"subject_key": subject, "kind": kind, "sources": sources},
    )


async def process_until_terminal(
    client: httpx.AsyncClient,
    headers: dict[str, str],
    request_id: str,
    *,
    limit: int = 30,
) -> dict[str, Any]:
    for _ in range(limit):
        response = await client.post("/v1/admin/process-once", headers=headers)
        assert response.status_code == 200, response.text
        view = await client.get(f"/v1/privacy-requests/{request_id}", headers=headers)
        assert view.status_code == 200, view.text
        payload = view.json()
        if payload["status"] in TERMINAL:
            return payload
    pytest.fail(f"request {request_id} was not terminal after {limit} processing passes")


async def seed(
    client: httpx.AsyncClient,
    headers: dict[str, str],
    subject: str,
    *,
    records_per_source: int = 2,
) -> httpx.Response:
    return await client.post(
        "/v1/admin/seed",
        headers=headers,
        json={"subject_key": subject, "records_per_source": records_per_source},
    )


def _artifact_path(raw_path: str, artifact_dir: Path) -> Path:
    candidate = Path(raw_path)
    if candidate.is_absolute():
        return candidate
    direct = artifact_dir / candidate
    return direct if direct.exists() else candidate


@pytest.mark.asyncio
async def test_api_key_is_required(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/v1/privacy-requests",
        headers={
            "X-Tenant-Id": "tenant-a",
            "X-Api-Key": "definitely-wrong",
            "Idempotency-Key": "unauthorized",
        },
        json={"subject_key": "no-key@example.com", "kind": "access", "sources": ["profile"]},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_create_is_idempotent_per_tenant(
    client: httpx.AsyncClient, headers_a: dict[str, str]
) -> None:
    arguments = {
        "subject": "idempotent@example.com",
        "kind": "access",
        "sources": ["profile", "devices"],
        "key": "same-command-001",
    }
    first = await create_request(client, headers_a, **arguments)
    second = await create_request(client, headers_a, **arguments)

    assert first.status_code in {200, 201}, first.text
    assert second.status_code in {200, 201}, second.text
    assert first.json()["id"] == second.json()["id"]
    assert len(second.json()["tasks"]) == 2


@pytest.mark.asyncio
async def test_tenant_cannot_read_another_tenants_request(
    client: httpx.AsyncClient, headers_a: dict[str, str], headers_b: dict[str, str]
) -> None:
    created = await create_request(
        client,
        headers_a,
        subject="isolated@example.com",
        kind="access",
        sources=["profile"],
        key="tenant-isolation-001",
    )
    assert created.status_code in {200, 201}, created.text

    hidden = await client.get(f"/v1/privacy-requests/{created.json()['id']}", headers=headers_b)
    assert hidden.status_code == 404


@pytest.mark.asyncio
async def test_tenant_worker_does_not_claim_another_tenants_tasks(
    client: httpx.AsyncClient, headers_a: dict[str, str], headers_b: dict[str, str]
) -> None:
    created = await create_request(
        client,
        headers_b,
        subject="worker-isolation@example.com",
        kind="access",
        sources=["profile"],
        key="worker-isolation-001",
    )
    assert created.status_code in {200, 201}, created.text
    request_id = created.json()["id"]

    processed = await client.post("/v1/admin/process-once?batch_size=1000", headers=headers_a)
    assert processed.status_code == 200
    tenant_b_view = await client.get(f"/v1/privacy-requests/{request_id}", headers=headers_b)
    assert tenant_b_view.status_code == 200
    assert tenant_b_view.json()["status"] == "queued"
    assert tenant_b_view.json()["tasks"][0]["attempt"] == 0


@pytest.mark.asyncio
async def test_access_request_builds_artifact_from_selected_sources(
    client: httpx.AsyncClient,
    headers_a: dict[str, str],
    artifact_dir: Path,
) -> None:
    subject = "access-flow@example.com"
    seeded = await seed(client, headers_a, subject, records_per_source=2)
    assert seeded.status_code in {200, 201}, seeded.text

    created = await create_request(
        client,
        headers_a,
        subject=subject,
        kind="access",
        sources=["profile", "devices", "jobs"],
        key="access-flow-001",
    )
    assert created.status_code in {200, 201}, created.text
    finished = await process_until_terminal(client, headers_a, created.json()["id"])

    assert finished["status"] == "completed"
    assert {task["source"] for task in finished["tasks"]} == {"profile", "devices", "jobs"}
    assert all(task["status"] == "succeeded" for task in finished["tasks"])
    assert finished["artifact_path"]
    artifact = _artifact_path(finished["artifact_path"], artifact_dir)
    assert artifact.exists()
    exported = json.loads(artifact.read_text(encoding="utf-8"))
    assert "access-flow@example.com" not in json.dumps(exported)


@pytest.mark.asyncio
async def test_delete_is_tenant_scoped(
    client: httpx.AsyncClient,
    headers_a: dict[str, str],
    headers_b: dict[str, str],
    database_url: str,
) -> None:
    subject = "shared-subject@example.com"
    assert (await seed(client, headers_a, subject)).status_code in {200, 201}
    assert (await seed(client, headers_b, subject)).status_code in {200, 201}

    created = await create_request(
        client,
        headers_a,
        subject=subject,
        kind="delete",
        sources=["profile", "telemetry"],
        key="delete-scope-001",
    )
    assert created.status_code in {200, 201}, created.text
    finished = await process_until_terminal(client, headers_a, created.json()["id"])
    assert finished["status"] == "completed"

    engine = create_async_engine(database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    subject_hash = hash_subject(subject)
    async with factory() as session:
        rows = (
            (
                await session.execute(
                    select(DeviceCloudRecord).where(
                        DeviceCloudRecord.subject_key_hash == subject_hash,
                        DeviceCloudRecord.source.in_(["profile", "telemetry"]),
                    )
                )
            )
            .scalars()
            .all()
        )
    await engine.dispose()

    tenant_a = [row for row in rows if row.tenant_id == "tenant-a"]
    tenant_b = [row for row in rows if row.tenant_id == "tenant-b"]
    assert tenant_a and all(row.deleted_at is not None for row in tenant_a)
    assert tenant_b and all(row.deleted_at is None for row in tenant_b)


@pytest.mark.asyncio
async def test_audit_events_form_a_per_tenant_hash_chain(
    client: httpx.AsyncClient,
    headers_a: dict[str, str],
    database_url: str,
) -> None:
    created = await create_request(
        client,
        headers_a,
        subject="audit@example.com",
        kind="access",
        sources=["support_logs", "devices"],
        key="audit-chain-001",
    )
    assert created.status_code in {200, 201}, created.text
    await process_until_terminal(client, headers_a, created.json()["id"])

    verified = await client.get("/v1/tenants/audit-chain", headers=headers_a)
    assert verified.status_code == 200
    assert verified.json()["valid"] is True

    engine = create_async_engine(database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        events = (
            (
                await session.execute(
                    select(AuditEvent)
                    .where(AuditEvent.tenant_id == "tenant-a")
                    .order_by(AuditEvent.sequence)
                )
            )
            .scalars()
            .all()
        )
    await engine.dispose()

    assert events
    assert [event.sequence for event in events] == list(range(1, len(events) + 1))
    assert len({event.event_hash for event in events}) == len(events)
    for previous, current in pairwise(events):
        assert current.previous_hash == previous.event_hash
    assert all(len(event.payload_hash) == 64 and len(event.event_hash) == 64 for event in events)


@pytest.mark.asyncio
async def test_reprocessing_terminal_request_has_zero_side_effects(
    client: httpx.AsyncClient,
    headers_a: dict[str, str],
    database_url: str,
) -> None:
    subject = "reprocess@example.com"
    await seed(client, headers_a, subject)
    created = await create_request(
        client,
        headers_a,
        subject=subject,
        kind="delete",
        sources=["jobs"],
        key="reprocess-001",
    )
    assert created.status_code in {200, 201}, created.text
    request_id = created.json()["id"]
    await process_until_terminal(client, headers_a, request_id)

    engine = create_async_engine(database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async def snapshot() -> tuple[int, int, int]:
        async with factory() as session:
            audit_count = await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.request_id == request_id)
            )
            deleted_count = await session.scalar(
                select(func.count())
                .select_from(DeviceCloudRecord)
                .where(
                    DeviceCloudRecord.tenant_id == "tenant-a",
                    DeviceCloudRecord.subject_key_hash == hash_subject(subject),
                    DeviceCloudRecord.deleted_at.is_not(None),
                )
            )
            version = await session.scalar(
                select(PrivacyRequest.version).where(PrivacyRequest.id == request_id)
            )
            return int(audit_count or 0), int(deleted_count or 0), int(version or 0)

    before = await snapshot()
    for _ in range(3):
        response = await client.post("/v1/admin/process-once", headers=headers_a)
        assert response.status_code == 200
    after = await snapshot()
    await engine.dispose()
    assert after == before


@pytest.mark.asyncio
async def test_expired_task_lease_is_recovered_after_worker_interruption(
    client: httpx.AsyncClient,
    headers_a: dict[str, str],
    database_url: str,
) -> None:
    subject = "lease-recovery@example.com"
    await seed(client, headers_a, subject)
    created = await create_request(
        client,
        headers_a,
        subject=subject,
        kind="access",
        sources=["profile"],
        key="lease-recovery-001",
    )
    assert created.status_code in {200, 201}, created.text
    request_id = created.json()["id"]

    engine = create_async_engine(database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        request = await session.get(PrivacyRequest, request_id)
        task = await session.scalar(select(RequestTask).where(RequestTask.request_id == request_id))
        assert request is not None and task is not None
        request.status = RequestStatus.RUNNING
        task.status = TaskStatus.RUNNING
        task.lease_owner = "worker-that-crashed"
        task.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=30)
        task.attempt = 1
        await session.commit()
    await engine.dispose()

    finished = await process_until_terminal(client, headers_a, request_id)
    assert finished["status"] == "completed"
    assert finished["tasks"][0]["status"] == "succeeded"
    assert finished["tasks"][0]["attempt"] >= 2
