#!/usr/bin/env python3
"""Compare fixed connector fan-out with AIMD under a deterministic capacity limit."""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

from privacy_cloud.services.adaptive_concurrency import AimdConcurrencyController
from privacy_cloud.services.source_connectors import RegionalSourceClient, SourceOperation


class CapacityTransport(httpx.AsyncBaseTransport):
    """Return 429 when concurrent work exceeds the configured source capacity."""

    def __init__(self, *, capacity: int, service_seconds: float) -> None:
        self.capacity = capacity
        self.service_seconds = service_seconds
        self.in_flight = 0
        self.peak_in_flight = 0
        self.accepted = 0
        self.rejected = 0
        self._lock = asyncio.Lock()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        async with self._lock:
            self.in_flight += 1
            admitted = self.in_flight <= self.capacity
            self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        try:
            await asyncio.sleep(self.service_seconds)
            if admitted:
                self.accepted += 1
                return httpx.Response(200, json={"verified": True}, request=request)
            self.rejected += 1
            return httpx.Response(429, json={"retryable": True}, request=request)
        finally:
            async with self._lock:
                self.in_flight -= 1


def operations(count: int) -> list[SourceOperation]:
    return [
        SourceOperation(
            task_id=f"task-{index}",
            tenant_id="tenant-benchmark",
            source="telemetry",
            subject_key_hash=f"{index:064x}",
            kind="delete",
        )
        for index in range(count)
    ]


async def fixed_run(args: argparse.Namespace) -> dict[str, float | int]:
    transport = CapacityTransport(capacity=args.capacity, service_seconds=args.service_ms / 1000)
    semaphore = asyncio.Semaphore(args.fixed_concurrency)

    async def execute(client: httpx.AsyncClient, operation: SourceOperation) -> float:
        started = time.perf_counter()
        async with semaphore:
            response = await client.post(
                f"https://regional.example/privacy/{operation.source}/{operation.kind}",
                headers={"X-Tenant-ID": operation.tenant_id},
                json={"subject_key_hash": operation.subject_key_hash},
            )
        response.raise_for_status() if response.status_code != 429 else None
        return time.perf_counter() - started

    started = time.perf_counter()
    async with httpx.AsyncClient(transport=transport) as client:
        latencies = await asyncio.gather(
            *(execute(client, operation) for operation in operations(args.operations))
        )
    wall = time.perf_counter() - started
    return {
        "accepted": transport.accepted,
        "rejected_429": transport.rejected,
        "peak_in_flight": transport.peak_in_flight,
        "wall_seconds": round(wall, 4),
        "attempts_per_second": round(args.operations / wall, 2),
        "accepted_per_second": round(transport.accepted / wall, 2),
        "mean_end_to_end_ms": round(statistics.fmean(latencies) * 1000, 3),
    }


async def adaptive_run(args: argparse.Namespace) -> dict[str, float | int]:
    transport = CapacityTransport(capacity=args.capacity, service_seconds=args.service_ms / 1000)
    controller = AimdConcurrencyController(
        limit=args.initial_concurrency,
        minimum=1,
        maximum=args.fixed_concurrency,
        target_p95_seconds=args.target_p95_ms / 1000,
        additive_step=1,
        decrease_ratio=0.5,
    )
    started = time.perf_counter()
    async with httpx.AsyncClient(transport=transport) as client:
        connector = RegionalSourceClient(
            base_url="https://regional.example",
            client=client,
            controller=controller,
        )
        receipts = await connector.execute_batch(operations(args.operations))
    wall = time.perf_counter() - started
    return {
        "accepted": sum(receipt.succeeded for receipt in receipts),
        "rejected_429": sum(receipt.status_code == 429 for receipt in receipts),
        "peak_in_flight": transport.peak_in_flight,
        "final_concurrency": controller.limit,
        "wall_seconds": round(wall, 4),
        "attempts_per_second": round(args.operations / wall, 2),
        "accepted_per_second": round(transport.accepted / wall, 2),
        "mean_request_ms": round(
            statistics.fmean(receipt.latency_seconds for receipt in receipts) * 1000, 3
        ),
    }


async def run(args: argparse.Namespace) -> dict[str, object]:
    fixed, adaptive = await asyncio.gather(fixed_run(args), adaptive_run(args))
    fixed_rejected = int(fixed["rejected_429"])
    adaptive_rejected = int(adaptive["rejected_429"])
    return {
        "schema_version": 1,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "workload": {
            "operations": args.operations,
            "source_capacity": args.capacity,
            "service_ms": args.service_ms,
            "fixed_concurrency": args.fixed_concurrency,
            "adaptive_initial_concurrency": args.initial_concurrency,
            "adaptive_target_p95_ms": args.target_p95_ms,
        },
        "fixed": fixed,
        "aimd": adaptive,
        "result": {
            "fewer_429": fixed_rejected - adaptive_rejected,
            "rejection_reduction_percent": round(
                100 * (fixed_rejected - adaptive_rejected) / fixed_rejected,
                2,
            )
            if fixed_rejected
            else 0.0,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operations", type=int, default=1000)
    parser.add_argument("--capacity", type=int, default=8)
    parser.add_argument("--service-ms", type=float, default=10)
    parser.add_argument("--fixed-concurrency", type=int, default=32)
    parser.add_argument("--initial-concurrency", type=int, default=4)
    parser.add_argument("--target-p95-ms", type=float, default=30)
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
    output = args.output_dir / f"connector-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
    print(f"\n\nresult_file={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
