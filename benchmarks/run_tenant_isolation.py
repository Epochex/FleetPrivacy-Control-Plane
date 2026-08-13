#!/usr/bin/env python3
"""Probe tenant-scoped resource lookups through the public API."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx


async def run(args: argparse.Namespace) -> dict[str, object]:
    os.environ["PRIVACY_CLOUD_DATABASE_URL"] = args.database_url
    os.environ["PRIVACY_CLOUD_API_KEY"] = args.api_key

    from privacy_cloud.config import get_settings

    get_settings.cache_clear()
    from privacy_cloud.main import create_app

    app = create_app()
    transport = httpx.ASGITransport(app=app)
    request_ids: list[str] = []

    def headers(tenant: int) -> dict[str, str]:
        return {"X-Tenant-Id": f"isolation-tenant-{tenant}", "X-Api-Key": args.api_key}

    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://isolation") as client,
    ):
        for tenant in range(args.tenants):
            response = await client.post(
                "/v1/privacy-requests",
                headers={**headers(tenant), "Idempotency-Key": f"isolation-{tenant}"},
                json={
                    "subject_key": f"subject-{tenant}@isolation.local",
                    "kind": "access",
                    "sources": ["profile"],
                },
            )
            response.raise_for_status()
            request_ids.append(response.json()["id"])

        semaphore = asyncio.Semaphore(args.concurrency)

        async def cross_tenant_probe(index: int) -> int:
            attacker = index % args.tenants
            target = (attacker + 1) % args.tenants
            async with semaphore:
                response = await client.get(
                    f"/v1/privacy-requests/{request_ids[target]}",
                    headers=headers(attacker),
                )
            return response.status_code

        started = time.perf_counter()
        status_codes = await asyncio.gather(
            *(cross_tenant_probe(index) for index in range(args.probes))
        )
        elapsed = time.perf_counter() - started

        own_results = await asyncio.gather(
            *(
                client.get(f"/v1/privacy-requests/{request_ids[tenant]}", headers=headers(tenant))
                for tenant in range(args.tenants)
            )
        )

    return {
        "schema_version": 1,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "workload": {
            "tenants": args.tenants,
            "cross_tenant_probes": args.probes,
            "concurrency": args.concurrency,
            "database": "PostgreSQL (asyncpg)",
            "transport": "HTTPX ASGITransport",
        },
        "result": {
            "cross_tenant_404": sum(code == 404 for code in status_codes),
            "cross_tenant_other": sum(code != 404 for code in status_codes),
            "own_tenant_200": sum(response.status_code == 200 for response in own_results),
            "own_tenant_total": len(own_results),
            "wall_seconds": round(elapsed, 4),
            "probes_per_second": round(args.probes / elapsed, 2),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--api-key", default="isolation-key")
    parser.add_argument("--tenants", type=int, default=100)
    parser.add_argument("--probes", type=int, default=10000)
    parser.add_argument("--concurrency", type=int, default=100)
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
    output = args.output_dir / f"isolation-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
    print(f"\n\nresult_file={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
