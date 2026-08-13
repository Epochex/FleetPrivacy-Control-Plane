# PostgreSQL benchmark

## Result

Benchmark timestamp: 2026-08-13 22:00:57 UTC.

| Metric | Result |
| --- | ---: |
| privacy requests | 200 |
| source tasks | 1,000 |
| tenants | 20 |
| request mix | 50% access, 50% deletion |
| completed requests | 200 / 200 |
| total task attempts | 1,000 |
| request admission throughput | 65.47 requests/s |
| request create p50 | 234.587 ms |
| request create p95 | 692.618 ms |
| request create p99 | 754.279 ms |
| source-task throughput | 67.47 tasks/s |
| source-task processing wall time | 14.8211 s |
| idempotent replay throughput | 121.54 requests/s |
| replayed request IDs matching | 200 / 200 |
| additional attempts after replay | 0 |
| valid tenant audit chains | 20 / 20 |

The result is stored as
[`benchmark-20260813T220057Z.json`](../benchmarks/benchmark_results/benchmark-20260813T220057Z.json).

## Environment

| Component | Value |
| --- | --- |
| operating system | Linux 5.15 x86_64 |
| logical CPUs | 24 |
| Python | 3.10.12 |
| database | PostgreSQL 16 through asyncpg |
| HTTP transport | HTTPX ASGITransport |
| records per source | 4 |
| request-create concurrency | 20 |
| worker batch size | 64 |

ASGITransport sends HTTP requests directly into the application process. The
measurements include FastAPI routing, Pydantic validation, application logic,
SQLAlchemy and PostgreSQL. Network, reverse-proxy and TLS costs are outside the
measured path.

## Reproduce

Start PostgreSQL and create an empty database. The benchmark creates its own
schema and synthetic device-cloud rows.

```bash
python benchmarks/run_benchmark.py \
  --database-url postgresql+asyncpg://privacy:privacy@127.0.0.1:55432/privacy \
  --requests 200 \
  --concurrency 20 \
  --tenants 20 \
  --records-per-source 4 \
  --worker-batch-size 64
```

The workload performs these phases:

1. Warm one request to load imports, compile SQL and establish connections.
2. Seed each benchmark subject across five device-cloud sources.
3. Create 200 tenant-scoped requests through the HTTP API.
4. Alternate access and deletion request types.
5. Run one worker pass per tenant in parallel. Each worker claims only that
   tenant's tasks.
6. Replay every creation command under its original idempotency key and compare
   request IDs and task-attempt totals.
7. Verify the audit chain for all 20 tenants.
8. Query every parent request and write latency percentiles and outcomes to JSON.

## Reading the numbers

Request admission measures validation plus the transaction that inserts the
request aggregate, five tasks, one Outbox event and the first audit event. The
65.47 requests/s value represents that complete write path.

Task throughput covers claim, source query or deletion, deletion readback,
receipt persistence, parent-state aggregation, audit append and access-artifact
generation. Exactly 1,000 attempts for 1,000 tasks confirms that this run did
not trigger lease recovery or task retry. The replay phase returned the same
200 aggregate IDs without increasing task attempts, exercising the database
idempotency constraint through the HTTP API.

The processing call lasts much longer than request creation because one worker
pass executes its claimed tasks sequentially inside each tenant. The next
optimization experiment is bounded per-tenant task concurrency with separate
database sessions, followed by comparison of lock waits, connection-pool wait
and p99 task duration.

## Additional experiments

Use the same JSON schema for follow-up runs:

- increase `--concurrency` while holding request count constant to locate the
  database connection-pool knee;
- increase tenants to measure parallel `SKIP LOCKED` claims across partitions;
- force a worker exit after claim, wait for lease expiry and record recovered
  task attempts;
- run through Uvicorn and a reverse proxy to add socket, serialization and TLS
  costs;
- grow completed task history and compare claim-query plans with the active
  status/lease/creation index.
