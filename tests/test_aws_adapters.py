from __future__ import annotations

import asyncio
import hashlib
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from privacy_cloud.models import (
    Base,
    OutboxEvent,
    PrivacyRequest,
    RequestKind,
    RequestStatus,
    RequestTask,
    TaskStatus,
)
from privacy_cloud.services.artifacts import S3ArtifactStore
from privacy_cloud.services.outbox import relay_outbox_once
from privacy_cloud.services.queue import (
    LocalTaskQueue,
    QueueMessage,
    SqsTaskQueue,
    consume_with_heartbeat,
)
from privacy_cloud.services.queue_worker import handle_task_envelope, run_worker_pass
from privacy_cloud.services.source_connectors import SourceReceipt


class FakeSqsClient:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.visibility_changes: list[dict[str, Any]] = []
        self.deleted: list[dict[str, Any]] = []

    def send_message(self, **kwargs: Any) -> dict[str, str]:
        self.sent.append(kwargs)
        return {"MessageId": "sqs-message-1"}

    def receive_message(self, **kwargs: Any) -> dict[str, Any]:
        return {
            "Messages": [
                {
                    "MessageId": "sqs-message-1",
                    "ReceiptHandle": "receipt-1",
                    "Body": '{"event_id":"event-1","payload":{"task_id":"task-1"}}',
                    "Attributes": {"ApproximateReceiveCount": "3"},
                }
            ]
        }

    def change_message_visibility(self, **kwargs: Any) -> None:
        self.visibility_changes.append(kwargs)

    def delete_message(self, **kwargs: Any) -> None:
        self.deleted.append(kwargs)


class FakeS3Client:
    def __init__(self) -> None:
        self.put_calls: list[dict[str, Any]] = []
        self.presign_calls: list[tuple[str, dict[str, Any]]] = []

    def put_object(self, **kwargs: Any) -> dict[str, str]:
        self.put_calls.append(kwargs)
        return {"ETag": "etag"}

    def generate_presigned_url(self, operation: str, **kwargs: Any) -> str:
        self.presign_calls.append((operation, kwargs))
        return "https://signed.example/artifact"


@pytest.mark.asyncio
async def test_sqs_fifo_publish_receive_visibility_and_ack() -> None:
    client = FakeSqsClient()
    queue = SqsTaskQueue(
        client=client,
        queue_url="https://sqs.eu-west-1.amazonaws.com/123/privacy-tasks.fifo",
        visibility_seconds=75,
    )
    body = {"event_id": "event-1", "tenant_id": "tenant-a", "payload": {"task_id": "task-1"}}

    assert await queue.publish(body) == "sqs-message-1"
    assert client.sent[0]["MessageGroupId"] == "tenant-a"
    assert client.sent[0]["MessageDeduplicationId"] == "event-1"

    messages = await queue.receive(max_messages=5, wait_seconds=20)
    assert len(messages) == 1
    assert messages[0].receive_count == 3
    await queue.heartbeat(messages[0].receipt_handle, 75)
    await queue.ack(messages[0].receipt_handle)
    assert client.visibility_changes[0]["VisibilityTimeout"] == 75
    assert client.deleted[0]["ReceiptHandle"] == "receipt-1"


@pytest.mark.asyncio
async def test_consumer_heartbeats_and_only_acks_success() -> None:
    queue = LocalTaskQueue()
    successful = QueueMessage("message-ok", "receipt-ok", {"event_id": "event-ok"})
    database_heartbeats = 0

    async def heartbeat_database_lease() -> None:
        nonlocal database_heartbeats
        database_heartbeats += 1

    async def slow_success(_: dict[str, Any]) -> bool:
        await asyncio.sleep(0.03)
        return True

    assert await consume_with_heartbeat(
        queue,
        successful,
        slow_success,
        heartbeat_seconds=0.01,
        visibility_seconds=60,
        heartbeat_hook=heartbeat_database_lease,
    )
    assert queue.acked == ["receipt-ok"]
    assert len(queue.heartbeats) >= 2
    assert database_heartbeats >= 2

    failed = QueueMessage("message-failed", "receipt-failed", {"event_id": "event-failed"})

    async def fails(_: dict[str, Any]) -> bool:
        raise ConnectionError("connector failed")

    with pytest.raises(ConnectionError):
        await consume_with_heartbeat(
            queue,
            failed,
            fails,
            heartbeat_seconds=0.01,
            visibility_seconds=60,
        )
    assert "receipt-failed" not in queue.acked


@pytest.mark.asyncio
async def test_s3_artifact_uses_kms_digest_metadata_and_short_lived_download() -> None:
    client = FakeS3Client()
    store = S3ArtifactStore(
        client=client,
        bucket="privacy-artifacts-prod",
        kms_key_id="alias/privacy-artifacts",
        key_prefix="exports",
    )
    document = {"request_id": "request-1", "records": [{"device": "anonymous-1"}]}

    reference = await store.put_json(
        tenant_id="tenant-a",
        request_id="request-1",
        document=document,
    )
    call = client.put_calls[0]
    assert reference.uri == "s3://privacy-artifacts-prod/exports/tenant-a/request-1.json"
    assert call["ServerSideEncryption"] == "aws:kms"
    assert call["SSEKMSKeyId"] == "alias/privacy-artifacts"
    assert call["Metadata"]["sha256"] == hashlib.sha256(call["Body"]).hexdigest()
    assert call["ContentType"] == "application/json"

    url = await store.presign_get(reference.uri, expires_seconds=300)
    assert url == "https://signed.example/artifact"
    operation, kwargs = client.presign_calls[0]
    assert operation == "get_object"
    assert kwargs["ExpiresIn"] == 300
    assert kwargs["Params"]["Key"] == "exports/tenant-a/request-1.json"


@pytest.mark.asyncio
async def test_artifact_endpoint_redirects_to_presigned_s3_get(
    tmp_path,
    monkeypatch,
    database_url: str,
) -> None:
    del database_url
    from privacy_cloud.api.routes import download_artifact

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'artifact-route.db'}")
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    async with factory() as session:
        session.add(
            PrivacyRequest(
                id="request-s3",
                tenant_id="tenant-a",
                idempotency_key="request-s3",
                subject_key_hash="a" * 64,
                kind=RequestKind.ACCESS,
                status=RequestStatus.COMPLETED,
                requested_sources=["profile"],
                artifact_path="s3://privacy-artifacts-prod/exports/tenant-a/request-s3.json",
            )
        )
        await session.commit()

    class SettingsStub:
        artifact_presign_seconds = 300
        artifact_dir = str(tmp_path)

    class StoreStub:
        async def presign_get(self, uri: str, expires_seconds: int) -> str:
            assert uri.endswith("request-s3.json")
            assert expires_seconds == 300
            return "https://signed.example/request-s3"

    monkeypatch.setattr("privacy_cloud.api.routes.get_settings", lambda: SettingsStub())
    monkeypatch.setattr("privacy_cloud.api.routes.build_artifact_store", lambda _: StoreStub())
    async with factory() as session:
        response = await download_artifact("request-s3", session, "tenant-a")
    assert response.status_code == 307
    assert response.headers["location"] == "https://signed.example/request-s3"
    await engine.dispose()


@pytest.mark.asyncio
async def test_outbox_relay_persists_only_confirmed_publications(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'outbox.db'}")
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    async with factory() as session:
        session.add_all(
            [
                OutboxEvent(
                    id="event-ok",
                    tenant_id="tenant-a",
                    aggregate_id="task-a",
                    event_type="privacy_task.ready",
                    payload={"task_id": "task-a"},
                ),
                OutboxEvent(
                    id="event-failed",
                    tenant_id="tenant-a",
                    aggregate_id="task-b",
                    event_type="privacy_task.ready",
                    payload={"task_id": "task-b"},
                ),
            ]
        )
        await session.commit()

    class PartialQueue(LocalTaskQueue):
        async def publish(self, body: dict[str, Any]) -> str:
            if body["event_id"] == "event-failed":
                raise ConnectionError("SQS unavailable")
            return await super().publish(body)

    queue = PartialQueue()
    async with factory() as session:
        result = await relay_outbox_once(session, queue)
    assert result == {"claimed": 2, "published": 1, "failed": 1}

    async with factory() as session:
        rows = list((await session.execute(select(OutboxEvent))).scalars())
    await engine.dispose()
    published = {row.id: row.published_at is not None for row in rows}
    assert published == {"event-ok": True, "event-failed": False}


@pytest.mark.asyncio
async def test_worker_pass_relays_and_acknowledges_missing_task_event(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'worker-pass.db'}")
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    async with factory() as session:
        session.add(
            OutboxEvent(
                id="wake-up-event",
                tenant_id="tenant-a",
                aggregate_id="task-already-removed",
                event_type="privacy_task.ready",
                payload={"task_id": "task-already-removed"},
            )
        )
        await session.commit()

    queue = LocalTaskQueue()
    result = await run_worker_pass(
        queue,
        factory,
        worker_id="worker-pass-test",
        poll_wait_seconds=0,
    )
    assert result == {"outbox_published": 1, "received": 1, "acknowledged": 1}
    assert queue.acked == ["wake-up-event"]
    await engine.dispose()


@pytest.mark.asyncio
async def test_terminal_task_absorbs_duplicate_queue_delivery(
    client,
    headers_a: dict[str, str],
    database_url: str,
) -> None:
    created = await client.post(
        "/v1/privacy-requests",
        headers={**headers_a, "Idempotency-Key": "sqs-duplicate-001"},
        json={
            "subject_key": "queue-duplicate@example.com",
            "kind": "access",
            "sources": ["profile"],
        },
    )
    request_id = created.json()["id"]
    await client.post("/v1/admin/process-once", headers=headers_a)

    engine = create_async_engine(database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        task = await session.scalar(select(RequestTask).where(RequestTask.request_id == request_id))
        assert task is not None
        task_id = task.id
        attempts = task.attempt
        request = await session.get(PrivacyRequest, request_id)
        assert request is not None
        request.artifact_path = None
        await session.commit()

    handled = await handle_task_envelope(
        factory,
        {
            "event_type": "privacy_task.ready",
            "payload": {"task_id": task_id},
        },
        worker_id="sqs-worker-1",
        lease_seconds=60,
    )
    assert handled is True
    async with factory() as session:
        task = await session.get(RequestTask, task_id)
        assert task is not None and task.attempt == attempts
        request = await session.get(PrivacyRequest, request_id)
        assert request is not None and request.artifact_path is not None
    await engine.dispose()


@pytest.mark.asyncio
async def test_regional_failure_retries_five_times_before_terminal_state(
    client,
    headers_a: dict[str, str],
    database_url: str,
) -> None:
    created = await client.post(
        "/v1/privacy-requests",
        headers={**headers_a, "Idempotency-Key": "regional-retry-001"},
        json={
            "subject_key": "regional-retry@example.com",
            "kind": "delete",
            "sources": ["telemetry"],
        },
    )
    request_id = created.json()["id"]
    engine = create_async_engine(database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        task = await session.scalar(select(RequestTask).where(RequestTask.request_id == request_id))
        assert task is not None
        task_id = task.id

    class FailingExecutor:
        calls = 0

        async def execute(self, operation):
            self.calls += 1
            return SourceReceipt(
                task_id=operation.task_id,
                succeeded=False,
                status_code=503,
                latency_seconds=0.01,
                payload={"retryable": True},
            )

    executor = FailingExecutor()
    envelope = {"event_type": "privacy_task.ready", "payload": {"task_id": task_id}}
    for attempt in range(1, 6):
        handled = await handle_task_envelope(
            factory,
            envelope,
            worker_id=f"regional-worker-{attempt}",
            lease_seconds=60,
            source_executor=executor,
        )
        assert handled is False
        async with factory() as session:
            task = await session.get(RequestTask, task_id)
            assert task is not None and task.attempt == attempt
            expected = TaskStatus.FAILED if attempt == 5 else TaskStatus.PENDING
            assert task.status == expected

    assert executor.calls == 5
    assert await handle_task_envelope(
        factory,
        envelope,
        worker_id="regional-worker-redrive",
        lease_seconds=60,
        source_executor=executor,
    )
    assert executor.calls == 5
    await engine.dispose()
