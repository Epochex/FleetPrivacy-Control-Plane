from __future__ import annotations

import asyncio
import json
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol

from privacy_cloud.config import Settings


@dataclass(frozen=True)
class QueueMessage:
    message_id: str
    receipt_handle: str
    body: dict[str, Any]
    receive_count: int = 1


class TaskQueue(Protocol):
    async def publish(self, body: dict[str, Any]) -> str: ...

    async def receive(
        self, *, max_messages: int = 1, wait_seconds: int = 10
    ) -> list[QueueMessage]: ...

    async def heartbeat(self, receipt_handle: str, visibility_seconds: int) -> None: ...

    async def ack(self, receipt_handle: str) -> None: ...


class SqsTaskQueue:
    def __init__(
        self,
        *,
        client: Any,
        queue_url: str,
        visibility_seconds: int = 60,
    ) -> None:
        if not queue_url:
            raise ValueError("SQS queue URL is required")
        self.client = client
        self.queue_url = queue_url
        self.visibility_seconds = visibility_seconds
        self.fifo = queue_url.endswith(".fifo")

    async def publish(self, body: dict[str, Any]) -> str:
        event_id = str(body["event_id"])
        arguments: dict[str, Any] = {
            "QueueUrl": self.queue_url,
            "MessageBody": json.dumps(body, separators=(",", ":"), sort_keys=True),
        }
        if self.fifo:
            arguments.update(
                MessageGroupId=str(body.get("tenant_id", "privacy-cloud")),
                MessageDeduplicationId=event_id,
            )
        response = await asyncio.to_thread(self.client.send_message, **arguments)
        return str(response["MessageId"])

    async def receive(self, *, max_messages: int = 1, wait_seconds: int = 10) -> list[QueueMessage]:
        response = await asyncio.to_thread(
            self.client.receive_message,
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=min(max(max_messages, 1), 10),
            WaitTimeSeconds=min(max(wait_seconds, 0), 20),
            VisibilityTimeout=self.visibility_seconds,
            AttributeNames=["ApproximateReceiveCount"],
        )
        messages = []
        for raw in response.get("Messages", []):
            messages.append(
                QueueMessage(
                    message_id=str(raw["MessageId"]),
                    receipt_handle=str(raw["ReceiptHandle"]),
                    body=json.loads(raw["Body"]),
                    receive_count=int(raw.get("Attributes", {}).get("ApproximateReceiveCount", 1)),
                )
            )
        return messages

    async def heartbeat(self, receipt_handle: str, visibility_seconds: int) -> None:
        await asyncio.to_thread(
            self.client.change_message_visibility,
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=visibility_seconds,
        )

    async def ack(self, receipt_handle: str) -> None:
        await asyncio.to_thread(
            self.client.delete_message,
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle,
        )


class LocalTaskQueue:
    """In-process adapter used by development and deterministic tests."""

    def __init__(self) -> None:
        self.messages: deque[QueueMessage] = deque()
        self.acked: list[str] = []
        self.heartbeats: list[tuple[str, int]] = []

    async def publish(self, body: dict[str, Any]) -> str:
        event_id = str(body["event_id"])
        self.messages.append(QueueMessage(event_id, event_id, body))
        return event_id

    async def receive(self, *, max_messages: int = 1, wait_seconds: int = 10) -> list[QueueMessage]:
        del wait_seconds
        result = []
        for _ in range(min(max_messages, len(self.messages))):
            result.append(self.messages.popleft())
        return result

    async def heartbeat(self, receipt_handle: str, visibility_seconds: int) -> None:
        self.heartbeats.append((receipt_handle, visibility_seconds))

    async def ack(self, receipt_handle: str) -> None:
        self.acked.append(receipt_handle)


def build_task_queue(settings: Settings, *, client: Any | None = None) -> TaskQueue:
    if settings.queue_backend == "local":
        return LocalTaskQueue()
    if settings.queue_backend != "sqs":
        raise ValueError(f"unsupported queue backend: {settings.queue_backend}")
    if client is None:
        import boto3

        client = boto3.client(
            "sqs",
            region_name=settings.aws_region,
            endpoint_url=settings.aws_endpoint_url or None,
        )
    return SqsTaskQueue(
        client=client,
        queue_url=settings.sqs_queue_url,
        visibility_seconds=settings.sqs_visibility_timeout_seconds,
    )


async def consume_with_heartbeat(
    queue: TaskQueue,
    message: QueueMessage,
    handler: Callable[[dict[str, Any]], Awaitable[bool]],
    *,
    heartbeat_seconds: float,
    visibility_seconds: int,
    heartbeat_hook: Callable[[], Awaitable[None]] | None = None,
) -> bool:
    """Extend SQS visibility while handling; acknowledge only a successful result."""

    stop = asyncio.Event()

    async def maintain_visibility() -> None:
        while True:
            try:
                await asyncio.wait_for(stop.wait(), timeout=heartbeat_seconds)
                return
            except asyncio.TimeoutError:
                await queue.heartbeat(message.receipt_handle, visibility_seconds)
                if heartbeat_hook is not None:
                    await heartbeat_hook()

    heartbeat_task = asyncio.create_task(maintain_visibility())
    try:
        succeeded = await handler(message.body)
        if succeeded:
            await queue.ack(message.receipt_handle)
        return succeeded
    finally:
        stop.set()
        await heartbeat_task
