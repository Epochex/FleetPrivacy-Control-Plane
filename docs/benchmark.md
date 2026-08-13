# Benchmark evidence

## PostgreSQL 16 request and task workload

Benchmark timestamp: 2026-08-13 22:30:35 UTC.

| Workload or result | Value |
| --- | ---: |
| tenants | 100 |
| privacy requests | 1,000 |
| source tasks | 5,000 |
| request mix | 50% Access, 50% Delete |
| records per source | 4 |
| create concurrency | 50 |
| worker concurrency | 10 |
| completed requests | 1,000 / 1,000 |
| request admission throughput | 63.03 requests/s |
| create P50 / P95 / P99 | 715.5 / 1,272.3 / 1,655.1 ms |
| source-task throughput | 31.78 tasks/s |
| total task attempts | 5,000 |
| replayed IDs matching | 1,000 / 1,000 |
| additional attempts after replay | 0 |
| valid audit chains | 100 / 100 |

The result is stored in [`benchmark-20260813T223035Z.json`](../benchmarks/benchmark_results/benchmark-20260813T223035Z.json).

Request admission includes FastAPI routing, Pydantic validation and the PostgreSQL transaction that inserts one request, five tasks, six Outbox rows and the first audit event. Source-task processing includes row-locked claim, data read or deletion, Delete readback, task receipt, parent aggregation, audit append and Access artifact generation. HTTPX ASGITransport removes socket, reverse-proxy and TLS overhead; the result isolates the Python and PostgreSQL path on 24 logical CPUs.

The first 100-tenant run executed all tenant workers simultaneously and exhausted the default SQLAlchemy pool of 5 persistent plus 10 overflow connections. The corrected run sets a 10-connection pool, 20 overflow slots, a 10-second acquisition timeout and worker concurrency 10. It completed all 5,000 tasks with one attempt per task. This failure became the capacity rule used by the Helm worker configuration: task concurrency remains below the available database connection budget after reserving health, relay and status-update sessions.

Reproduce:

```bash
python benchmarks/run_benchmark.py \
  --database-url postgresql+asyncpg://privacy:privacy@127.0.0.1:55433/privacy \
  --requests 1000 --concurrency 50 --tenants 100 \
  --records-per-source 4 --worker-batch-size 64 --worker-concurrency 10
```

## Worker termination and lease recovery

[`fault-injection-20260813T223347Z.json`](../benchmarks/benchmark_results/fault-injection-20260813T223347Z.json) claims five tasks under one worker, terminates the owner, expires the leases and lets a second worker reclaim them.

| Result | Value |
| --- | ---: |
| initially claimed | 5 |
| reclaimed after expiry | 5 |
| recovered successfully | 5 |
| final attempts per task | 2, 2, 2, 2, 2 |
| request status | completed |
| replay additional attempts | 0 |
| audit-chain recomputation | passed |

## Cross-tenant isolation

[`isolation-20260813T223338Z.json`](../benchmarks/benchmark_results/isolation-20260813T223338Z.json) creates one request for each of 100 tenants, performs 10,000 cross-tenant lookups at concurrency 25 and then executes one same-tenant control lookup per tenant.

| Result | Value |
| --- | ---: |
| cross-tenant HTTP 404 | 10,000 / 10,000 |
| other cross-tenant responses | 0 |
| same-tenant HTTP 200 | 100 / 100 |
| probe throughput | 250.19 requests/s |

The lookup predicate combines request ID and tenant ID. The API returns the same absence response for an unknown resource and a resource owned by another tenant.

## SQS, S3/KMS and Redis integration

[`aws-adapters-20260813T224322Z.json`](../benchmarks/benchmark_results/aws-adapters-20260813T224322Z.json) uses boto3 and redis-py against LocalStack 3.8.1 and Redis 7.

| Result | Value |
| --- | ---: |
| SQS published / received / acknowledged | 500 / 500 / 500 |
| SQS publish / consume throughput | 202.94 / 146.46 messages/s |
| visible and in-flight messages after run | 0 / 0 |
| S3 artifacts uploaded | 100 |
| objects carrying SSE-KMS | 100 / 100 |
| objects carrying SHA-256 metadata | 100 / 100 |
| presigned GET | generated with 300-second expiry |
| Redis atomic AIMD updates | 1,000 |
| Redis update throughput | 960.32 updates/s |
| Redis distributed admission | 64 admitted, 65th blocked, one release allowed one new holder |

## Regional connector overload

[`connector-20260813T223245Z.json`](../benchmarks/benchmark_results/connector-20260813T223245Z.json) models a regional source accepting eight concurrent operations with 10 ms service time. Both policies receive the same 1,000 operations.

| Policy | Accepted | HTTP 429 | Accepted throughput | Peak in flight |
| --- | ---: | ---: | ---: | ---: |
| fixed concurrency 32 | 12 | 988 | 28.53 operations/s | 32 |
| AIMD, initial 4 | 975 | 25 | 432.11 operations/s | 9 |

AIMD adds one slot after a successful latency window and halves the tenant/source window after an HTTP failure or P95 breach. The controlled overload run reduced first-attempt HTTP 429 responses by 97.47%. Redis Lua updates make the window decision atomic across worker Pods and expire inactive tenant/source keys after the configured TTL.
