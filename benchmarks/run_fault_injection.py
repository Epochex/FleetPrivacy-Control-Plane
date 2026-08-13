"""Deterministic reliability benchmark for task replay and lease recovery."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import select


async def run(args: argparse.Namespace) -> dict[str, Any]:
    temporary = tempfile.TemporaryDirectory(prefix="privacy-cloud-fault-")
    root = Path(temporary.name)
    os.environ["PRIVACY_CLOUD_DATABASE_URL"] = args.database_url
    os.environ["PRIVACY_CLOUD_ARTIFACT_DIR"] = str(root / "artifacts")
    os.environ["PRIVACY_CLOUD_API_KEY"] = "fault-key"

    from privacy_cloud.config import get_settings

    get_settings.cache_clear()
    from privacy_cloud.db import SessionFactory
    from privacy_cloud.main import create_app
    from privacy_cloud.models import RequestTask
    from privacy_cloud.services.worker import claim_tasks, execute_task

    app = create_app()
    headers = {"X-Tenant-ID": "fault-tenant", "X-API-Key": "fault-key"}
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://fault") as client,
    ):
        subject = "fault-subject@example.test"
        seeded = await client.post(
            "/v1/admin/seed",
            headers=headers,
            json={"subject_key": subject, "records_per_source": 3},
        )
        seeded.raise_for_status()
        created = await client.post(
            "/v1/privacy-requests",
            headers={**headers, "Idempotency-Key": "fault-request-001"},
            json={
                "subject_key": subject,
                "kind": "delete",
                "sources": ["profile", "devices", "telemetry", "jobs", "support_logs"],
            },
        )
        created.raise_for_status()
        request_id = created.json()["id"]

        async with SessionFactory() as session:
            claimed = await claim_tasks(
                session,
                worker_id="terminated-worker",
                tenant_id="fault-tenant",
                batch_size=5,
                lease_seconds=60,
            )
            await session.execute(
                RequestTask.__table__.update()
                .where(RequestTask.id.in_(claimed))
                .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
            )
            await session.commit()

        async with SessionFactory() as session:
            recovered = await claim_tasks(
                session,
                worker_id="recovery-worker",
                tenant_id="fault-tenant",
                batch_size=5,
            )
            recovery_success = 0
            for task_id in recovered:
                recovery_success += int(
                    await execute_task(session, task_id=task_id, worker_id="recovery-worker")
                )

        view = await client.get(f"/v1/privacy-requests/{request_id}", headers=headers)
        view.raise_for_status()
        before_replay_attempts = sum(task["attempt"] for task in view.json()["tasks"])
        replay = await client.post(
            "/v1/privacy-requests",
            headers={**headers, "Idempotency-Key": "fault-request-001"},
            json={
                "subject_key": subject,
                "kind": "delete",
                "sources": ["profile", "devices", "telemetry", "jobs", "support_logs"],
            },
        )
        replay.raise_for_status()
        after = await client.get(f"/v1/privacy-requests/{request_id}", headers=headers)
        after.raise_for_status()
        after_replay_attempts = sum(task["attempt"] for task in after.json()["tasks"])
        audit = await client.get("/v1/tenants/audit-chain", headers=headers)
        audit.raise_for_status()

        async with SessionFactory() as session:
            task_states = list(
                (
                    await session.execute(
                        select(RequestTask.status, RequestTask.attempt).where(
                            RequestTask.request_id == request_id
                        )
                    )
                ).all()
            )

    temporary.cleanup()
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "workload": {"requests": 1, "source_tasks": 5, "records_per_source": 3},
        "worker_termination": {
            "initially_claimed": len(claimed),
            "reclaimed_after_expiry": len(recovered),
            "recovered_successfully": recovery_success,
        },
        "result": {
            "request_status": after.json()["status"],
            "task_states": [status.value for status, _ in task_states],
            "task_attempts": [attempt for _, attempt in task_states],
            "idempotent_replay_same_id": replay.json()["id"] == request_id,
            "additional_attempts_after_replay": after_replay_attempts - before_replay_attempts,
            "audit_chain_valid": audit.json()["valid"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
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
    output = args.output_dir / f"fault-injection-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    print(f"result_file={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
