from __future__ import annotations

import httpx
import pytest

from privacy_cloud.services.adaptive_concurrency import AimdConcurrencyController
from privacy_cloud.services.source_connectors import (
    RegionalSourceClient,
    RegionalSourceExecutor,
    SourceOperation,
)


@pytest.mark.asyncio
async def test_source_connector_propagates_tenant_and_task_idempotency() -> None:
    captured: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={"verified": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http_client:
        connector = RegionalSourceClient(
            base_url="https://regional-source.test",
            client=http_client,
            controller=AimdConcurrencyController(limit=2),
            service_token="regional-service-token",
        )
        receipt = await connector.execute(
            SourceOperation(
                task_id="task-42",
                tenant_id="retailer-eu",
                source="devices",
                subject_key_hash="f" * 64,
                kind="delete",
            )
        )

    assert receipt.succeeded is True
    assert captured[0].headers["X-Tenant-ID"] == "retailer-eu"
    assert captured[0].headers["Idempotency-Key"] == "task-42:devices"
    assert captured[0].headers["Authorization"] == "Bearer regional-service-token"
    assert captured[0].headers["X-Request-ID"] == "task-42"
    assert captured[0].url.path == "/privacy/devices/delete"


@pytest.mark.asyncio
async def test_batch_controller_reduces_concurrency_after_source_failures() -> None:
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503 if calls <= 4 else 200, json={"call": calls})

    controller = AimdConcurrencyController(limit=4, decrease_ratio=0.5)
    operations = [
        SourceOperation(
            task_id=f"task-{index}",
            tenant_id="retailer-eu",
            source="telemetry",
            subject_key_hash="f" * 64,
            kind="access",
        )
        for index in range(6)
    ]
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http_client:
        connector = RegionalSourceClient(
            base_url="https://regional-source.test",
            client=http_client,
            controller=controller,
        )
        receipts = await connector.execute_batch(operations)

    assert len(receipts) == 6
    assert controller.limit == 3
    assert [receipt.status_code for receipt in receipts] == [503, 503, 503, 503, 200, 200]


class FakeSharedState:
    def __init__(self) -> None:
        self.acquired: list[tuple[str, str, str]] = []
        self.released: list[tuple[str, str, str]] = []
        self.adjusted = 0

    async def acquire(self, tenant_id: str, source: str, *, holder: str, **kwargs):
        del kwargs
        self.acquired.append((tenant_id, source, holder))
        return True, 4

    async def release(self, tenant_id: str, source: str, *, holder: str) -> None:
        self.released.append((tenant_id, source, holder))

    async def adjust(self, *args, **kwargs):
        del args, kwargs
        self.adjusted += 1
        return 4, 5


@pytest.mark.asyncio
async def test_regional_executor_applies_shared_admission_and_releases_slot() -> None:
    shared_state = FakeSharedState()

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"verified": True, "deleted_count": 3, "remaining_count": 0},
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http_client:
        executor = RegionalSourceExecutor(
            base_url="https://regional-source.test",
            client=http_client,
            shared_state=shared_state,
        )
        receipt = await executor.execute(
            SourceOperation(
                task_id="task-admitted",
                tenant_id="tenant-a",
                source="telemetry",
                subject_key_hash="a" * 64,
                kind="delete",
            )
        )

    assert receipt.succeeded is True
    assert shared_state.acquired == [("tenant-a", "telemetry", "task-admitted")]
    assert shared_state.released == [("tenant-a", "telemetry", "task-admitted")]
    assert shared_state.adjusted == 1


@pytest.mark.asyncio
async def test_regional_delete_rejects_success_without_zero_remaining_readback() -> None:
    shared_state = FakeSharedState()

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"verified": True, "deleted_count": 2, "remaining_count": 1},
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http_client:
        executor = RegionalSourceExecutor(
            base_url="https://regional-source.test",
            client=http_client,
            shared_state=shared_state,
        )
        with pytest.raises(ValueError, match="remaining records"):
            await executor.execute(
                SourceOperation(
                    task_id="task-not-verified",
                    tenant_id="tenant-a",
                    source="support_logs",
                    subject_key_hash="a" * 64,
                    kind="delete",
                )
            )

    assert shared_state.released == [("tenant-a", "support_logs", "task-not-verified")]
    assert shared_state.adjusted == 1
