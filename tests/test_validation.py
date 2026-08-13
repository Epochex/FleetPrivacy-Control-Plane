from __future__ import annotations

import httpx
import pytest


@pytest.mark.asyncio
async def test_unknown_source_is_rejected(
    client: httpx.AsyncClient, headers_a: dict[str, str]
) -> None:
    response = await client.post(
        "/v1/privacy-requests",
        headers={**headers_a, "Idempotency-Key": "invalid-source-001"},
        json={
            "subject_key": "validation@example.com",
            "kind": "access",
            "sources": ["profile", "unregistered_database"],
        },
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_idempotency_key_is_required(
    client: httpx.AsyncClient, headers_a: dict[str, str]
) -> None:
    response = await client.post(
        "/v1/privacy-requests",
        headers=headers_a,
        json={"subject_key": "validation@example.com", "kind": "access"},
    )
    assert response.status_code == 422
