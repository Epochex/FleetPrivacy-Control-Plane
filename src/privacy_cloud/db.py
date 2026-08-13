from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from privacy_cloud.config import get_settings


def build_engine(database_url: str | None = None) -> AsyncEngine:
    settings = get_settings()
    url = database_url or settings.database_url
    arguments: dict[str, object] = {"pool_pre_ping": True}
    if not url.startswith("sqlite"):
        arguments.update(
            pool_size=settings.database_pool_size,
            max_overflow=settings.database_max_overflow,
            pool_timeout=settings.database_pool_timeout_seconds,
        )
    return create_async_engine(url, **arguments)


engine = build_engine()
SessionFactory = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionFactory() as session:
        yield session
