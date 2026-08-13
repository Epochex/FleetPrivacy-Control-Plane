# FleetPrivacy Control Plane

FleetPrivacy is a Python cloud service for executing privacy access and erasure
requests across connected-device data. A single user can leave records in the
account service, device registry, telemetry stream, print-job history and
support logs. The control plane turns one privacy request into independently
recoverable source tasks, records every transition and returns a verifiable
result.

The project models a practical device-cloud problem: a privacy operation must
finish across several stores even when a worker crashes, a client retries the
same HTTP call or one source is temporarily unavailable.

## What it demonstrates

- **Python cloud API:** FastAPI, Pydantic validation, async SQLAlchemy and
  PostgreSQL-backed request state.
- **Idempotent request intake:** `(tenant_id, idempotency_key)` uniqueness makes
  client retries return the original request.
- **Multi-tenant isolation:** tenant identity is carried from authentication to
  every request, task, record, audit event and query predicate.
- **Recoverable background work:** one task per data source, expiring leases,
  persisted attempts and aggregate request states.
- **Transactional event handoff:** request state and outbox records are
  committed together for a reliable downstream publisher.
- **Tamper-evident audit:** each audit entry hashes the previous entry, action
  and payload so a verifier can detect mutation or deletion.
- **Data minimization:** the service persists a normalized SHA-256 subject key;
  raw account identifiers stay outside the request tables and logs.
- **Operations:** Prometheus counters and latency histograms, container health
  checks, Docker Compose and a Kubernetes deployment manifest.

## Request lifecycle

```mermaid
stateDiagram-v2
    [*] --> queued: POST request
    queued --> running: source tasks claimed
    running --> verifying: deletion readback
    running --> completed: access sources succeeded
    running --> partial: mixed source results
    verifying --> completed: deletion sources verified
    verifying --> partial: mixed source results
    running --> failed: request cannot make progress
    completed --> [*]
    partial --> [*]
    failed --> [*]
```

An access request collects matching records into an artifact. An erasure
request marks matching records deleted and verifies each source result. Task
receipts remain queryable after completion, which makes partial outcomes and
retry history visible to operators.

## Architecture

```mermaid
flowchart LR
    C[Cloud client] -->|tenant, API key, idempotency key| A[FastAPI service]
    A --> V[Pydantic validation]
    V --> P[(PostgreSQL)]
    P --> R[Privacy requests]
    P --> T[Source tasks and leases]
    P --> O[Outbox events]
    P --> U[Hash-chained audit]
    W[Task worker] -->|claim and checkpoint| T
    W --> D[Device-cloud source adapters]
    D -->|receipt and verification| W
    W --> F[Access artifacts]
    A --> M[Prometheus metrics]
```

PostgreSQL is both the system of record and the task coordination primitive.
This keeps the local stack small while still exercising transaction design,
concurrent claims and crash recovery. See [architecture.md](docs/architecture.md)
for the state transitions and invariants.

## Quick start

### Docker Compose

```bash
git clone https://github.com/Epochex/FleetPrivacy-Control-Plane.git
cd FleetPrivacy-Control-Plane
export POSTGRES_PASSWORD="$(python -c 'import secrets; print(secrets.token_urlsafe(24))')"
export PRIVACY_CLOUD_API_KEY="local-demo-key"
docker compose up --build -d
curl http://localhost:8000/v1/healthz
```

Create sample device-cloud records, submit an access request and execute its
source tasks:

```bash
export API=http://localhost:8000
export TENANT=maker-lab
export SUBJECT=owner@example.com

curl -sS -X POST "$API/v1/admin/seed" \
  -H "X-Tenant-ID: $TENANT" \
  -H "X-API-Key: $PRIVACY_CLOUD_API_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"subject_key\":\"$SUBJECT\",\"records_per_source\":3}"

curl -sS -X POST "$API/v1/privacy-requests" \
  -H "X-Tenant-ID: $TENANT" \
  -H "X-API-Key: $PRIVACY_CLOUD_API_KEY" \
  -H "Idempotency-Key: access-owner-001" \
  -H "Content-Type: application/json" \
  -d "{\"subject_key\":\"$SUBJECT\",\"kind\":\"access\",\"sources\":[\"profile\",\"devices\",\"telemetry\",\"jobs\",\"support_logs\"]}"

curl -sS -X POST "$API/v1/admin/process-once?worker_id=demo-worker&batch_size=32" \
  -H "X-Tenant-ID: $TENANT" \
  -H "X-API-Key: $PRIVACY_CLOUD_API_KEY"
```

OpenAPI documentation is available at `http://localhost:8000/docs`. The full
route guide is in [api.md](docs/api.md).

### Local Python

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
uvicorn privacy_cloud.main:app --reload
pytest -q
```

SQLite is the local default. PostgreSQL is used for the container stack and the
concurrent-worker benchmark.

## Benchmark

The committed PostgreSQL benchmark admitted **200 requests at 65.47 req/s** and
processed **1,000 source tasks at 67.47 tasks/s**. All 200 requests completed.
A second pass replayed every idempotency key: all 200 request IDs matched and
task attempts stayed at 1,000. Audit verification passed for all 20 tenants.

Run the same workload with:

```bash
python benchmarks/run_benchmark.py \
  --database-url postgresql+asyncpg://privacy:privacy@127.0.0.1:55432/privacy \
  --requests 200 --concurrency 20 --tenants 20 \
  --records-per-source 4 --worker-batch-size 64
```

Reproduction parameters, percentiles and measurement scope are published in
[benchmark.md](docs/benchmark.md). The machine-readable result is committed at
[`benchmark-20260813T220057Z.json`](benchmarks/benchmark_results/benchmark-20260813T220057Z.json).

## Repository guide

```text
src/privacy_cloud/   API, database models, security and task execution
tests/               state, idempotency, tenant and failure-recovery tests
benchmarks/          repeatable load generator and result artifacts
deploy/              Compose and Kubernetes deployment notes
docs/                architecture, API, benchmark and interview deep dives
```

## Engineering references

FleetPrivacy combines patterns proven in several public systems:

- [Full Stack FastAPI Template](https://github.com/fastapi/full-stack-fastapi-template)
  for Python API, PostgreSQL, container and CI conventions.
- [Fides](https://github.com/ethyca/fides) for data-subject request orchestration
  and privacy-request operational workflows.
- [Amazon S3 Find and Forget](https://github.com/awslabs/amazon-s3-find-and-forget)
  for queued discovery, deletion jobs and auditable action logs.
Attribution and license details are recorded in [NOTICE](NOTICE). FleetPrivacy
is released under the [MIT License](LICENSE).
