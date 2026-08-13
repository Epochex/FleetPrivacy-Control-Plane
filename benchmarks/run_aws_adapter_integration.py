#!/usr/bin/env python3
"""Exercise SQS, S3/KMS and Redis adapters against local cloud services."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from redis.asyncio import Redis

from privacy_cloud.services.adaptive_concurrency import RedisAimdState
from privacy_cloud.services.artifacts import S3ArtifactStore
from privacy_cloud.services.queue import SqsTaskQueue


async def run(args: argparse.Namespace) -> dict[str, object]:
    boto_arguments = {
        "region_name": args.region,
        "endpoint_url": args.endpoint_url,
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
    }
    sqs_client = boto3.client("sqs", **boto_arguments)
    s3_client = boto3.client("s3", **boto_arguments)
    queue_url = sqs_client.get_queue_url(QueueName=args.queue_name)["QueueUrl"]
    queue = SqsTaskQueue(client=sqs_client, queue_url=queue_url, visibility_seconds=60)
    artifact_store = S3ArtifactStore(
        client=s3_client,
        bucket=args.bucket,
        kms_key_id=args.kms_key_id,
        key_prefix="integration",
    )

    publish_started = time.perf_counter()
    publish_ids = await asyncio.gather(
        *(
            queue.publish(
                {
                    "event_id": f"integration-event-{index}",
                    "tenant_id": f"tenant-{index % 20}",
                    "event_type": "privacy_task.ready",
                    "payload": {"task_id": f"task-{index}"},
                }
            )
            for index in range(args.messages)
        )
    )
    publish_elapsed = time.perf_counter() - publish_started

    received = 0
    receive_counts: list[int] = []
    consume_started = time.perf_counter()
    while received < args.messages:
        messages = await queue.receive(max_messages=10, wait_seconds=1)
        if not messages:
            raise RuntimeError(f"queue drained early at {received}/{args.messages}")
        if received == 0:
            await queue.heartbeat(messages[0].receipt_handle, 60)
        await asyncio.gather(*(queue.ack(message.receipt_handle) for message in messages))
        received += len(messages)
        receive_counts.extend(message.receive_count for message in messages)
    consume_elapsed = time.perf_counter() - consume_started

    artifact_started = time.perf_counter()
    references = await asyncio.gather(
        *(
            artifact_store.put_json(
                tenant_id=f"tenant-{index % 20}",
                request_id=f"request-{index}",
                document={"request_id": f"request-{index}", "verified": True},
            )
            for index in range(args.artifacts)
        )
    )
    artifact_elapsed = time.perf_counter() - artifact_started
    heads = await asyncio.gather(
        *(
            asyncio.to_thread(
                s3_client.head_object,
                Bucket=args.bucket,
                Key=reference.uri.removeprefix(f"s3://{args.bucket}/"),
            )
            for reference in references
        )
    )
    signed_url = await artifact_store.presign_get(references[0].uri, expires_seconds=300)

    redis_client = Redis.from_url(args.redis_url, decode_responses=True)
    redis_state = RedisAimdState(redis_client, ttl_seconds=600)
    await redis_client.delete(
        redis_state.key("tenant-a", "telemetry"),
        redis_state.inflight_key("tenant-a", "telemetry"),
    )
    redis_started = time.perf_counter()
    windows = []
    for _ in range(args.redis_updates):
        windows.append(
            await redis_state.adjust(
                "tenant-a",
                "telemetry",
                default=4,
                p95_seconds=0.2,
                failure_rate=0,
                target_p95_seconds=0.8,
                minimum=1,
                maximum=64,
                additive_step=1,
                decrease_ratio=0.5,
            )
        )
    admissions = await asyncio.gather(
        *(
            redis_state.acquire(
                "tenant-a",
                "telemetry",
                holder=f"holder-{index}",
                default=4,
                lease_seconds=30,
            )
            for index in range(64)
        )
    )
    blocked, _ = await redis_state.acquire(
        "tenant-a",
        "telemetry",
        holder="holder-blocked",
        default=4,
        lease_seconds=30,
    )
    await redis_state.release("tenant-a", "telemetry", holder="holder-0")
    readmitted, _ = await redis_state.acquire(
        "tenant-a",
        "telemetry",
        holder="holder-readmitted",
        default=4,
        lease_seconds=30,
    )
    redis_elapsed = time.perf_counter() - redis_started
    await redis_client.aclose()

    queue_attributes = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    return {
        "schema_version": 1,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "aws_endpoint": args.endpoint_url,
            "redis_endpoint": args.redis_url,
            "region": args.region,
        },
        "sqs": {
            "published": len(publish_ids),
            "received_and_acknowledged": received,
            "publish_per_second": round(args.messages / publish_elapsed, 2),
            "consume_per_second": round(args.messages / consume_elapsed, 2),
            "maximum_receive_count": max(receive_counts),
            "visible_after": int(queue_attributes["ApproximateNumberOfMessages"]),
            "in_flight_after": int(queue_attributes["ApproximateNumberOfMessagesNotVisible"]),
            "visibility_heartbeat_exercised": True,
        },
        "s3_kms": {
            "uploaded": len(references),
            "uploads_per_second": round(args.artifacts / artifact_elapsed, 2),
            "all_sse_kms": all(head.get("ServerSideEncryption") == "aws:kms" for head in heads),
            "all_sha256_metadata": all(
                len(head.get("Metadata", {}).get("sha256", "")) == 64 for head in heads
            ),
            "presigned_get_seconds": 300,
            "presigned_url_generated": bool(signed_url),
        },
        "redis_aimd": {
            "atomic_updates": len(windows),
            "first_transition": list(windows[0]),
            "last_transition": list(windows[-1]),
            "updates_per_second": round(args.redis_updates / redis_elapsed, 2),
            "tenant_source_key": redis_state.key("tenant-a", "telemetry"),
            "admitted_at_window_64": sum(admitted for admitted, _ in admissions),
            "sixty_fifth_blocked": not blocked,
            "readmitted_after_release": readmitted,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint-url", default="http://127.0.0.1:4566")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:16379/0")
    parser.add_argument("--region", default="eu-west-1")
    parser.add_argument("--queue-name", default="fleetprivacy-requests")
    parser.add_argument("--bucket", default="fleetprivacy-artifacts")
    parser.add_argument("--kms-key-id", default="alias/fleetprivacy-data")
    parser.add_argument("--messages", type=int, default=500)
    parser.add_argument("--artifacts", type=int, default=100)
    parser.add_argument("--redis-updates", type=int, default=1000)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "benchmark_results",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = asyncio.run(run(args))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    output = args.output_dir / f"aws-adapters-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
    print(f"\n\nresult_file={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
