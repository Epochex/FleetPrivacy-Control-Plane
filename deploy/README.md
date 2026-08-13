# Deployment

## Docker Compose

The development stack runs the API and PostgreSQL:

```bash
export POSTGRES_PASSWORD="$(python -c 'import secrets; print(secrets.token_urlsafe(24))')"
export PRIVACY_CLOUD_API_KEY="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
docker compose up --build -d
docker compose ps
```

The API listens on `http://localhost:8000`. PostgreSQL stays on the private
Compose network. Request artifacts and database files use named volumes.

## Kubernetes

[`kubernetes.yaml`](kubernetes.yaml) deploys the API against an existing
PostgreSQL service. Create the secret first:

```bash
kubectl create namespace fleetprivacy
kubectl -n fleetprivacy create secret generic fleetprivacy-secrets \
  --from-literal=database-url='postgresql+asyncpg://USER:PASSWORD@POSTGRES_HOST:5432/fleetprivacy' \
  --from-literal=api-key="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
kubectl apply -f deploy/kubernetes.yaml
```

Replace `ghcr.io/epochex/fleetprivacy-control-plane:latest` with a pinned image digest
for a release. The Deployment uses rolling updates, readiness checks and a
persistent artifact volume. Database backups and migrations belong in the
PostgreSQL release procedure.

## Configuration

| Variable | Purpose | Default |
| --- | --- | --- |
| `PRIVACY_CLOUD_DATABASE_URL` | SQLAlchemy async database URL | local SQLite |
| `PRIVACY_CLOUD_API_KEY` | API key checked with constant-time comparison | `dev-api-key` |
| `PRIVACY_CLOUD_ARTIFACT_DIR` | generated access-package directory | `artifacts` |
| `PRIVACY_CLOUD_WORKER_BATCH_SIZE` | maximum tasks claimed per worker pass | `32` |
| `PRIVACY_CLOUD_LEASE_SECONDS` | task lease duration | `60` |

Set all secrets through the deployment secret store. Grant the application
identity read/write access only to its database schema and artifact location.
