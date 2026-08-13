# Architecture

## Business flow

Connected-device platforms usually distribute one person's information across
several domains:

| Source | Example records | Access action | Erasure action |
| --- | --- | --- | --- |
| `profile` | account and locale | export matching fields | remove profile payload |
| `devices` | ownership and pairing | export owned devices | remove ownership records |
| `telemetry` | health and usage events | export subject events | remove subject events |
| `jobs` | print or device jobs | export job history | remove job records |
| `support_logs` | support diagnostics | export matching logs | remove subject logs |

The API accepts one access or deletion command and expands it into one task for
each requested source. Each task stores its own attempt counter, lease, result
summary and error. The parent request derives its state from those receipts.

## Write path

Request creation uses one database transaction:

1. Authenticate `X-API-Key` and validate `X-Tenant-ID`.
2. Normalize and hash the subject key.
3. insert the privacy request under the tenant-scoped idempotency key.
4. Insert one task for each selected source.
5. Append the first audit event.
6. Insert the outbox event.
7. Commit all records together.

The unique constraint on `(tenant_id, idempotency_key)` is the final arbiter for
concurrent duplicate submissions. After a conflict, the API reads and returns
the existing aggregate.

## Task claiming and recovery

Workers claim pending tasks in batches. A claim writes `lease_owner`,
`lease_expires_at`, increments `attempt` and moves the task to `running`.
PostgreSQL row locking prevents two workers from owning the same task at the
same time. A task whose lease expires becomes claimable again, so process loss
does not strand the parent request.

The source action is idempotent for a `(request_id, source)` pair. The worker
stores a compact receipt after each source call and then recalculates the parent
request state:

- pending or active tasks produce `running`;
- successful access tasks produce `completed`;
- deletion execution sets `verifying` while it performs the source readback,
  then successful tasks produce `completed`;
- mixed success and failure produce `partial`;
- terminal failure across the request produces `failed`.

## Data model

### `privacy_requests`

The aggregate stores tenant, idempotency key, hashed subject, request kind,
policy version, selected sources, artifact location, status and an optimistic
version. The tenant/status/creation index supports dashboards and polling.

### `request_tasks`

One row represents one source action. `(request_id, source)` is unique. The
claim index covers status, lease expiry and creation time so workers can find
available work without scanning completed history.

### `outbox_events`

An outbox event is inserted beside the state change it describes. A publisher
can deliver unpublished events and then set `published_at`. This closes the
gap between committing request state and notifying another service.

### `audit_events`

Audit entries form a per-request hash chain:

```text
payload_hash = SHA256(canonical_json(payload))
event_hash = SHA256(canonical_json(
  tenant_id, request_id, sequence, action, payload_hash, previous_hash
))
```

Sequence and previous-hash validation detects edited, reordered and interior
deleted entries. An externally stored chain head also detects tail deletion.
Audit payloads contain identifiers and receipts needed for review; raw subject
keys stay out of the chain.

### `device_cloud_records`

This table provides reproducible device-cloud source adapters for the demo and
benchmark. Every record carries tenant, hashed subject and source. Erasure sets
`deleted_at`; access collects active payloads into the request artifact.

## Multi-tenant isolation

Tenant identity is established from the request header after API-key checking.
Every aggregate and source row stores `tenant_id`, and every read or mutation
uses it as a predicate. Idempotency keys are tenant-local, so two customers can
use the same client-generated key without collision.

Tests exercise cross-tenant request lookup, repeated idempotency keys and data
records that share the same subject hash in different tenants.

## Failure matrix

| Failure | Persisted evidence | Recovery action |
| --- | --- | --- |
| client times out after commit | request and idempotency row | retry returns the same request |
| process exits after task claim | owner and lease expiry | another worker reclaims after expiry |
| one source fails | attempt, error and sibling receipts | return partial or failed with the successful receipts intact |
| callback delivery fails | unpublished outbox row | publisher retries from database |
| audit record is changed | broken hash or sequence | verifier identifies first invalid event |

## Operations

The service exposes created/completed counters and a source/kind task-duration
histogram. Operators can derive request completion rate, source error rate and
latency distributions from these metrics. Health probes cover API process
availability; deployment monitoring should add database connectivity and lease
backlog alerts.

The first scaling step is multiple API processes sharing PostgreSQL. Worker
throughput scales through batch size and concurrent claimers. Artifact storage
can move from the mounted volume to an object-store adapter while keeping the
request state and signed location in PostgreSQL.
