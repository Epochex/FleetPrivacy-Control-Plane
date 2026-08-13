from __future__ import annotations

import os
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

API_KEY = "benchmark-test-key"


@pytest.fixture(scope="session")
def database_url(tmp_path_factory: pytest.TempPathFactory) -> str:
    root = tmp_path_factory.mktemp("privacy-cloud")
    os.environ["PRIVACY_CLOUD_DATABASE_URL"] = f"sqlite+aiosqlite:///{root / 'test.db'}"
    os.environ["PRIVACY_CLOUD_ARTIFACT_DIR"] = str(root / "artifacts")
    os.environ["PRIVACY_CLOUD_API_KEY"] = API_KEY
    os.environ["PRIVACY_CLOUD_WORKER_BATCH_SIZE"] = "128"
    os.environ["PRIVACY_CLOUD_LEASE_SECONDS"] = "1"
    return os.environ["PRIVACY_CLOUD_DATABASE_URL"]


@pytest.fixture(scope="session")
async def app(database_url: str):
    # Import after configuring the process: db.py intentionally builds its engine at import time.
    from privacy_cloud.config import get_settings

    get_settings.cache_clear()
    from privacy_cloud.main import create_app

    application = create_app()
    async with application.router.lifespan_context(application):
        yield application


@pytest.fixture
async def client(app) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as value:
        yield value


@pytest.fixture
def headers_a() -> dict[str, str]:
    return {"X-Tenant-Id": "tenant-a", "X-Api-Key": API_KEY}


@pytest.fixture
def headers_b() -> dict[str, str]:
    return {"X-Tenant-Id": "tenant-b", "X-Api-Key": API_KEY}


@pytest.fixture(scope="session")
def artifact_dir() -> Path:
    return Path(os.environ["PRIVACY_CLOUD_ARTIFACT_DIR"])
