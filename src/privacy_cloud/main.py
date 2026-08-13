from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from privacy_cloud.api import router
from privacy_cloud.db import engine
from privacy_cloud.models import Base


@asynccontextmanager
async def lifespan(_: FastAPI):
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()


def create_app() -> FastAPI:
    application = FastAPI(
        title="Device Privacy Cloud",
        version="0.1.0",
        description="Multi-tenant device-cloud privacy request orchestration",
        lifespan=lifespan,
    )
    sqlite_write_lock = asyncio.Lock()

    @application.middleware("http")
    async def serialize_sqlite_writes(request, call_next):
        if engine.dialect.name == "sqlite" and request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            async with sqlite_write_lock:
                return await call_next(request)
        return await call_next(request)

    @application.exception_handler(RequestValidationError)
    async def authentication_validation_handler(_, exc: RequestValidationError):
        missing_api_key = any(
            error.get("type") == "missing"
            and tuple(error.get("loc", ())) == ("header", "x-api-key")
            for error in exc.errors()
        )
        if missing_api_key:
            return JSONResponse(status_code=401, content={"detail": "invalid API key"})
        serializable_errors = [
            {key: value for key, value in error.items() if key != "ctx"} for error in exc.errors()
        ]
        return JSONResponse(
            status_code=422,
            content={"detail": jsonable_encoder(serializable_errors)},
        )

    application.include_router(router, prefix="/v1")
    return application


app = create_app()
