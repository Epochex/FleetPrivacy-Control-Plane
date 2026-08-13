# Reproducible benchmark

The benchmark drives the FastAPI application in-process through HTTPX ASGITransport. It measures
application, validation, SQLAlchemy and SQLite work while excluding network and TLS overhead.

```bash
python benchmarks/run_benchmark.py \
  --requests 200 \
  --concurrency 20 \
  --tenants 20 \
  --records-per-source 4 \
  --worker-batch-size 64
```

For the production-path measurement, pass an empty PostgreSQL database:

```bash
python benchmarks/run_benchmark.py \
  --database-url postgresql+asyncpg://privacy:privacy@127.0.0.1:55432/privacy \
  --requests 200 --concurrency 20 --tenants 20
```

Each request fans out to all five data sources. The workload alternates access and deletion requests,
waits until every request reaches a terminal state, and writes a machine-readable result under
`benchmarks/benchmark_results/`.

The JSON records the exact workload and environment together with create throughput, source-task
processing throughput, latency percentiles, final states and task attempts. Compare results only when
the workload, database and transport fields match.
