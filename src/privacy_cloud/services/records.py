from __future__ import annotations

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from privacy_cloud.models import DeviceCloudRecord
from privacy_cloud.schemas import ALLOWED_SOURCES, SeedRecordsRequest
from privacy_cloud.security import hash_subject


async def seed_records(
    session: AsyncSession, *, tenant_id: str, command: SeedRecordsRequest
) -> int:
    """Create deterministic synthetic cloud records for demos and benchmarks."""
    subject_hash = hash_subject(command.subject_key)
    rows: list[DeviceCloudRecord] = []
    for source in sorted(ALLOWED_SOURCES):
        for sequence in range(command.records_per_source):
            rows.append(
                DeviceCloudRecord(
                    tenant_id=tenant_id,
                    subject_key_hash=subject_hash,
                    source=source,
                    payload={
                        "source": source,
                        "sequence": sequence,
                        "device_ref": f"device-{sequence % 8:03d}",
                        "region": "eu-central",
                    },
                )
            )
    session.add_all(rows)
    await session.commit()
    return len(rows)


async def clear_seed_records(session: AsyncSession, *, tenant_id: str) -> int:
    result = await session.execute(
        delete(DeviceCloudRecord).where(DeviceCloudRecord.tenant_id == tenant_id)
    )
    await session.commit()
    return int(result.rowcount or 0)
