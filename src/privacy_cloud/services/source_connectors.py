from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

from privacy_cloud.config import Settings
from privacy_cloud.services.adaptive_concurrency import AimdConcurrencyController, RedisAimdState


@dataclass(frozen=True)
class SourceOperation:
    task_id: str
    tenant_id: str
    source: str
    subject_key_hash: str
    kind: str


@dataclass(frozen=True)
class SourceReceipt:
    task_id: str
    succeeded: bool
    status_code: int
    latency_seconds: float
    payload: dict[str, Any]


class SourceResultPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    verified: bool
    record_count: int | None = None
    deleted_count: int | None = None
    remaining_count: int | None = None

    def assert_kind(self, kind: str) -> None:
        if not self.verified:
            raise ValueError("regional source did not verify its result")
        if kind == "delete" and self.remaining_count != 0:
            raise ValueError("regional delete readback found remaining records")


class RegionalSourceClient:
    """Execute typed source operations with task identity and AIMD admission."""

    def __init__(
        self,
        *,
        base_url: str,
        client: httpx.AsyncClient,
        controller: AimdConcurrencyController,
        shared_state: RedisAimdState | None = None,
        service_token: str = "",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.client = client
        self.controller = controller
        self.shared_state = shared_state
        self.service_token = service_token

    async def execute(self, operation: SourceOperation) -> SourceReceipt:
        started = time.perf_counter()
        headers = {
            "X-Tenant-ID": operation.tenant_id,
            "Idempotency-Key": f"{operation.task_id}:{operation.source}",
            "X-Request-ID": operation.task_id,
        }
        if self.service_token:
            headers["Authorization"] = f"Bearer {self.service_token}"
        response = await self.client.post(
            f"{self.base_url}/privacy/{operation.source}/{operation.kind}",
            headers=headers,
            json={"subject_key_hash": operation.subject_key_hash},
        )
        latency = time.perf_counter() - started
        succeeded = response.is_success
        self.controller.observe(latency, succeeded=succeeded)
        payload = response.json() if response.content else {}
        return SourceReceipt(
            task_id=operation.task_id,
            succeeded=succeeded,
            status_code=response.status_code,
            latency_seconds=latency,
            payload=payload,
        )

    async def execute_batch(self, operations: list[SourceOperation]) -> list[SourceReceipt]:
        scopes = {(operation.tenant_id, operation.source) for operation in operations}
        if len(scopes) > 1:
            raise ValueError("one connector batch must target one tenant and data source")
        receipts: list[SourceReceipt] = []
        cursor = 0
        while cursor < len(operations):
            operation = operations[cursor]
            if self.shared_state is not None:
                self.controller.limit = await self.shared_state.get_limit(
                    operation.tenant_id,
                    operation.source,
                    default=self.controller.limit,
                )
            batch = operations[cursor : cursor + self.controller.limit]
            receipts.extend(await asyncio.gather(*(self.execute(operation) for operation in batch)))
            cursor += len(batch)
            decision = self.controller.adjust()
            if self.shared_state is not None:
                _, self.controller.limit = await self.shared_state.adjust(
                    operation.tenant_id,
                    operation.source,
                    default=int(decision["previous_limit"]),
                    p95_seconds=float(decision["p95_seconds"]),
                    failure_rate=float(decision["failure_rate"]),
                    target_p95_seconds=self.controller.target_p95_seconds,
                    minimum=self.controller.minimum,
                    maximum=self.controller.maximum,
                    additive_step=self.controller.additive_step,
                    decrease_ratio=self.controller.decrease_ratio,
                )
        return receipts


class RegionalSourceExecutor:
    """Execute one leased task under a Redis-shared tenant/source admission window."""

    def __init__(
        self,
        *,
        base_url: str,
        client: httpx.AsyncClient,
        shared_state: RedisAimdState,
        admission_timeout_seconds: float = 30.0,
        initial_limit: int = 4,
        maximum_limit: int = 64,
        target_p95_seconds: float = 0.8,
        service_token: str = "",
    ) -> None:
        self.client = RegionalSourceClient(
            base_url=base_url,
            client=client,
            controller=AimdConcurrencyController(
                limit=initial_limit,
                maximum=maximum_limit,
                target_p95_seconds=target_p95_seconds,
            ),
            service_token=service_token,
        )
        self.shared_state = shared_state
        self.admission_timeout_seconds = admission_timeout_seconds

    async def execute(self, operation: SourceOperation) -> SourceReceipt:
        template = self.client.controller
        controller = AimdConcurrencyController(
            limit=template.limit,
            minimum=template.minimum,
            maximum=template.maximum,
            target_p95_seconds=template.target_p95_seconds,
            additive_step=template.additive_step,
            decrease_ratio=template.decrease_ratio,
        )
        source_client = RegionalSourceClient(
            base_url=self.client.base_url,
            client=self.client.client,
            controller=controller,
            service_token=self.client.service_token,
        )
        deadline = time.monotonic() + self.admission_timeout_seconds
        while True:
            admitted, limit = await self.shared_state.acquire(
                operation.tenant_id,
                operation.source,
                holder=operation.task_id,
                default=controller.limit,
                lease_seconds=self.admission_timeout_seconds + 5,
            )
            controller.limit = limit
            if admitted:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("regional source admission timed out")
            await asyncio.sleep(0.02)

        async def publish_decision() -> None:
            decision = controller.adjust()
            await self.shared_state.adjust(
                operation.tenant_id,
                operation.source,
                default=int(decision["previous_limit"]),
                p95_seconds=float(decision["p95_seconds"]),
                failure_rate=float(decision["failure_rate"]),
                target_p95_seconds=controller.target_p95_seconds,
                minimum=controller.minimum,
                maximum=controller.maximum,
                additive_step=controller.additive_step,
                decrease_ratio=controller.decrease_ratio,
            )

        try:
            try:
                receipt = await source_client.execute(operation)
                if receipt.succeeded:
                    payload = SourceResultPayload.model_validate(receipt.payload)
                    payload.assert_kind(operation.kind)
                    receipt = SourceReceipt(
                        task_id=receipt.task_id,
                        succeeded=True,
                        status_code=receipt.status_code,
                        latency_seconds=receipt.latency_seconds,
                        payload=payload.model_dump(exclude_none=True),
                    )
            except Exception:
                controller.observe(0.0, succeeded=False)
                await publish_decision()
                raise
            await publish_decision()
            return receipt
        finally:
            await self.shared_state.release(
                operation.tenant_id,
                operation.source,
                holder=operation.task_id,
            )

    @classmethod
    def from_settings(cls, settings: Settings) -> RegionalSourceExecutor:
        if (
            not settings.regional_source_base_url.startswith("https://")
            or not settings.redis_url
            or not settings.regional_source_token
        ):
            raise ValueError("HTTPS regional source, Redis URL and service token are required")
        return cls(
            base_url=settings.regional_source_base_url,
            client=httpx.AsyncClient(timeout=settings.source_request_timeout_seconds),
            shared_state=RedisAimdState.from_url(settings.redis_url),
            admission_timeout_seconds=settings.source_admission_timeout_seconds,
            service_token=settings.regional_source_token,
        )

    async def close(self) -> None:
        await self.client.client.aclose()
        close = getattr(self.shared_state.client, "aclose", None)
        if close is not None:
            await close()
