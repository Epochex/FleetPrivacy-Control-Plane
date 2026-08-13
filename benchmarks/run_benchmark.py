#!/usr/bin/env python3
"""Reproducible in-process ASGI benchmark for the privacy request pipeline."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import statistics
import sys
import tempfile
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

SOURCES = ["profile", "devices", "telemetry", "jobs", "support_logs"]


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def latency_summary(values: list[float]) -> dict[str, float]:
    return {
        "mean_ms": round(statistics.fmean(values) * 1000, 3) if values else 0.0,
        "p50_ms": round(percentile(values, 0.50) * 1000, 3),
        "p95_ms": round(percentile(values, 0.95) * 1000, 3),
        "p99_ms": round(percentile(values, 0.99) * 1000, 3),
        "max_ms": round(max(values) * 1000, 3) if values else 0.0,
    }


async def timed(call: Callable[[], Awaitable[httpx.Response]]) -> tuple[float, httpx.Response]:
    started = time.perf_counter()
    response = await call()
    return time.perf_counter() - started, response


async def run(args: argparse.Namespace) -> dict[str, Any]:
    temporary = tempfile.TemporaryDirectory(prefix="privacy-cloud-benchmark-")
    root = Path(temporary.name)
    api_key = "benchmark-key"
    database_url = args.database_url or f"sqlite+aiosqlite:///{root / 'benchmark.db'}"
    os.environ["PRIVACY_CLOUD_DATABASE_URL"] = database_url
    os.environ["PRIVACY_CLOUD_ARTIFACT_DIR"] = str(root / "artifacts")
    os.environ["PRIVACY_CLOUD_API_KEY"] = api_key
    os.environ["PRIVACY_CLOUD_WORKER_BATCH_SIZE"] = str(args.worker_batch_size)

    from privacy_cloud.config import get_settings

    get_settings.cache_clear()
    from privacy_cloud.main import create_app

    app = create_app()

    def headers_for(index: int) -> dict[str, str]:
        return {
            "X-Tenant-Id": f"benchmark-tenant-{index % args.tenants}",
            "X-Api-Key": api_key,
        }

    warm_headers = headers_for(0)
    transport = httpx.ASGITransport(app=app)
    create_latencies: list[float] = []
    seed_latencies: list[float] = []
    process_latencies: list[float] = []
    request_ids: list[tuple[int, str, dict[str, str]]] = []
    replay_latencies: list[float] = []

    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://benchmark") as client,
    ):
        # Warm imports, SQL compilation and connection setup before measuring.
        warm_subject = "warmup@example.com"
        await client.post(
            "/v1/admin/seed",
            headers=warm_headers,
            json={"subject_key": warm_subject, "records_per_source": 1},
        )
        warm = await client.post(
            "/v1/privacy-requests",
            headers={**warm_headers, "Idempotency-Key": "warmup-001"},
            json={"subject_key": warm_subject, "kind": "access", "sources": ["profile"]},
        )
        warm.raise_for_status()
        for _ in range(10):
            await client.post("/v1/admin/process-once", headers=warm_headers)
            state = await client.get(
                f"/v1/privacy-requests/{warm.json()['id']}", headers=warm_headers
            )
            if state.json()["status"] in {"completed", "partial", "failed"}:
                break

        semaphore = asyncio.Semaphore(args.concurrency)

        async def seed_one(index: int) -> None:
            subject = f"subject-{index}@benchmark.local"
            headers = headers_for(index)
            async with semaphore:
                elapsed, response = await timed(
                    lambda: client.post(
                        "/v1/admin/seed",
                        headers=headers,
                        json={
                            "subject_key": subject,
                            "records_per_source": args.records_per_source,
                        },
                    )
                )
            response.raise_for_status()
            seed_latencies.append(elapsed)

        seed_started = time.perf_counter()
        await asyncio.gather(*(seed_one(index) for index in range(args.requests)))
        seed_elapsed = time.perf_counter() - seed_started

        async def create_one(index: int) -> None:
            subject = f"subject-{index}@benchmark.local"
            kind = "delete" if index % 2 else "access"
            headers = headers_for(index)
            async with semaphore:
                elapsed, response = await timed(
                    lambda: client.post(
                        "/v1/privacy-requests",
                        headers={**headers, "Idempotency-Key": f"request-{index}"},
                        json={"subject_key": subject, "kind": kind, "sources": SOURCES},
                    )
                )
            response.raise_for_status()
            create_latencies.append(elapsed)
            request_ids.append((index, response.json()["id"], headers))

        create_started = time.perf_counter()
        await asyncio.gather(*(create_one(index) for index in range(args.requests)))
        create_elapsed = time.perf_counter() - create_started

        terminal: set[str] = set()
        processing_started = time.perf_counter()
        passes = 0
        worker_semaphore = asyncio.Semaphore(args.worker_concurrency)
        while len(terminal) < len(request_ids):
            passes += 1
            if passes > args.max_processing_passes:
                raise RuntimeError(
                    f"processing did not finish after {args.max_processing_passes} passes; "
                    f"terminal={len(terminal)}/{len(request_ids)}"
                )

            async def process_tenant(tenant_index: int) -> None:
                headers = headers_for(tenant_index)
                async with worker_semaphore:
                    elapsed, response = await timed(
                        lambda: client.post("/v1/admin/process-once", headers=headers)
                    )
                response.raise_for_status()
                process_latencies.append(elapsed)

            await asyncio.gather(
                *(process_tenant(tenant_index) for tenant_index in range(args.tenants))
            )
            if passes % args.poll_every == 0:
                states = await asyncio.gather(
                    *(
                        client.get(f"/v1/privacy-requests/{request_id}", headers=headers)
                        for _, request_id, headers in request_ids
                        if request_id not in terminal
                    )
                )
                for state in states:
                    state.raise_for_status()
                    payload = state.json()
                    if payload["status"] in {"completed", "partial", "failed"}:
                        terminal.add(payload["id"])
        processing_elapsed = time.perf_counter() - processing_started

        states = await asyncio.gather(
            *(
                client.get(f"/v1/privacy-requests/{request_id}", headers=headers)
                for _, request_id, headers in request_ids
            )
        )
        status_counts: dict[str, int] = {}
        task_attempts = 0
        for state in states:
            payload = state.json()
            status_counts[payload["status"]] = status_counts.get(payload["status"], 0) + 1
            task_attempts += sum(task["attempt"] for task in payload["tasks"])

        async def replay_one(index: int, request_id: str, headers: dict[str, str]) -> bool:
            subject = f"subject-{index}@benchmark.local"
            kind = "delete" if index % 2 else "access"
            async with semaphore:
                elapsed, response = await timed(
                    lambda: client.post(
                        "/v1/privacy-requests",
                        headers={**headers, "Idempotency-Key": f"request-{index}"},
                        json={"subject_key": subject, "kind": kind, "sources": SOURCES},
                    )
                )
            response.raise_for_status()
            replay_latencies.append(elapsed)
            return response.json()["id"] == request_id

        replay_started = time.perf_counter()
        replay_matches = await asyncio.gather(
            *(replay_one(index, request_id, headers) for index, request_id, headers in request_ids)
        )
        replay_elapsed = time.perf_counter() - replay_started

        audit_results = await asyncio.gather(
            *(
                client.get("/v1/tenants/audit-chain", headers=headers_for(tenant_index))
                for tenant_index in range(args.tenants)
            )
        )
        for audit_result in audit_results:
            audit_result.raise_for_status()
        valid_audit_chains = sum(result.json()["valid"] for result in audit_results)

        replay_states = await asyncio.gather(
            *(
                client.get(f"/v1/privacy-requests/{request_id}", headers=headers)
                for _, request_id, headers in request_ids
            )
        )
        attempts_after_replay = sum(
            sum(task["attempt"] for task in state.json()["tasks"]) for state in replay_states
        )

    tasks = args.requests * len(SOURCES)
    result = {
        "schema_version": 1,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "processor": platform.processor() or "unknown",
            "logical_cpus": os.cpu_count(),
            "database": (
                "PostgreSQL (asyncpg)"
                if database_url.startswith("postgresql")
                else "SQLite (aiosqlite), isolated temporary file"
            ),
            "transport": "HTTPX ASGITransport (no network or TLS overhead)",
        },
        "workload": {
            "requests": args.requests,
            "sources_per_request": len(SOURCES),
            "source_tasks": tasks,
            "records_per_source": args.records_per_source,
            "create_concurrency": args.concurrency,
            "tenants": args.tenants,
            "worker_batch_size": args.worker_batch_size,
            "worker_concurrency": args.worker_concurrency,
            "request_mix": "50% access, 50% delete",
        },
        "seed": {
            **latency_summary(seed_latencies),
            "wall_seconds": round(seed_elapsed, 4),
            "operations_per_second": round(args.requests / seed_elapsed, 2),
        },
        "create": {
            **latency_summary(create_latencies),
            "wall_seconds": round(create_elapsed, 4),
            "requests_per_second": round(args.requests / create_elapsed, 2),
        },
        "process": {
            **latency_summary(process_latencies),
            "wall_seconds": round(processing_elapsed, 4),
            "source_tasks_per_second": round(tasks / processing_elapsed, 2),
            "processing_passes": passes,
            "total_task_attempts": task_attempts,
        },
        "idempotent_replay": {
            **latency_summary(replay_latencies),
            "wall_seconds": round(replay_elapsed, 4),
            "requests_per_second": round(args.requests / replay_elapsed, 2),
            "matching_request_ids": sum(replay_matches),
            "additional_task_attempts": attempts_after_replay - task_attempts,
        },
        "result": {
            "terminal_requests": len(terminal),
            "status_counts": status_counts,
            "valid_audit_chains": valid_audit_chains,
            "audited_tenants": args.tenants,
        },
    }
    temporary.cleanup()
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--tenants", type=int, default=20)
    parser.add_argument("--records-per-source", type=int, default=4)
    parser.add_argument("--worker-batch-size", type=int, default=64)
    parser.add_argument("--worker-concurrency", type=int, default=10)
    parser.add_argument("--poll-every", type=int, default=1)
    parser.add_argument("--max-processing-passes", type=int, default=2000)
    parser.add_argument(
        "--database-url",
        help="SQLAlchemy async URL; defaults to an isolated SQLite file",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "benchmark_results",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    for name in (
        "requests",
        "concurrency",
        "tenants",
        "records_per_source",
        "worker_batch_size",
        "worker_concurrency",
    ):
        if getattr(args, name) < 1:
            raise SystemExit(f"--{name.replace('_', '-')} must be positive")
    result = asyncio.run(run(args))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    output = args.output_dir / f"benchmark-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
    print(f"\n\nresult_file={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
