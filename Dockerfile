FROM python:3.12-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build
COPY pyproject.toml ./
COPY src ./src
RUN python -m pip wheel --wheel-dir /wheels .

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PRIVACY_CLOUD_ARTIFACT_DIR=/var/lib/fleetprivacy/artifacts

RUN groupadd --system fleetprivacy \
    && useradd --system --gid fleetprivacy --home-dir /var/lib/fleetprivacy fleetprivacy \
    && mkdir -p /var/lib/fleetprivacy/artifacts \
    && chown -R fleetprivacy:fleetprivacy /var/lib/fleetprivacy

COPY --from=builder /wheels /wheels
RUN python -m pip install --no-cache-dir /wheels/* \
    && rm -rf /wheels

USER fleetprivacy
WORKDIR /var/lib/fleetprivacy
EXPOSE 8000

CMD ["uvicorn", "privacy_cloud.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
