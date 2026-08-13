# Architecture

## Business model

A connected-device vendor serves retailers as isolated tenants. A retailer submits a
privacy request when an account is closed, a former operator requests a data export,
or its compliance team must delete a subject across regional services. A store is the
resource domain and the subject's device-cloud footprint spans five source families:

| Source | Connected-device records | Access output | Delete verification |
| --- | --- | --- | --- |
| `profile` | account, locale, consent state | selected profile fields | active profile count |
| `devices` | device ownership, assignment and pairing actor | subject-linked device list | remaining subject bindings |
| `telemetry` | heartbeat and health events annotated with an operator or account | subject-linked events | remaining subject events |
| `jobs` | refresh, firmware and configuration jobs with creator identity | subject-created job history | remaining subject job rows |
| `support_logs` | diagnostics and support cases | matched records | remaining matched logs |

The API accepts one Access or Delete command from the customer's privacy portal and
creates one independently recoverable task for each selected source. Each task
persists tenant, source, owner, lease expiry, attempt, receipt and error. The parent
request exposes partial progress and derives the final export or deletion receipt
from those source results.

## State ownership

| Component | Input | Maintained state | Output |
| --- | --- | --- | --- |
| FastAPI | tenant, API key, idempotency key, request command | validated request context | stable request ID and query API |
| RDS PostgreSQL | request command, claims and receipts | request state, leases, attempts, Outbox, source records and audit chain | atomic state transitions |
| SQS | committed Outbox envelopes | visibility deadline, receive count and DLQ status | worker wake-up event |
| Worker | SQS event and database task | in-progress lease heartbeat | source receipt and parent-state update |
| Redis | source latency, HTTP 429 and failures | tenant/source AIMD window with TTL | next connector concurrency limit |
| S3/KMS | Access JSON bytes | encrypted versioned object and SHA-256 metadata | five-minute presigned download |

## Transactional request intake

One PostgreSQL transaction performs this sequence:

1. Hash the normalized subject identity.
2. Insert the request under `(tenant_id, idempotency_key)`.
3. Insert one task per selected source.
4. Insert one `privacy_task.ready` Outbox row per task and one request-created event.
5. Append the first tenant audit event.
6. Commit all rows.

Concurrent requests using the same tenant and idempotency key converge through the unique constraint. A replay with the same normalized command returns the original request. A key reused for a different command returns HTTP 409.

## Outbox and SQS delivery

Relays scan committed Outbox rows with `FOR UPDATE SKIP LOCKED`. The relay sends an envelope containing event ID, tenant ID, aggregate ID, event type and payload, then records `published_at`. A process exit between send confirmation and the database update creates a duplicate SQS event. The consumer uses the database task row as the execution gate:

```text
pending task + valid message       -> claim, attempt + 1, execute
running task + active lease        -> leave message unacknowledged
running task + expired lease       -> reclaim, attempt + 1, execute
succeeded or failed task           -> refresh aggregate/artifact, acknowledge
unknown task                       -> acknowledge
```

During source work, a heartbeat extends SQS visibility and the PostgreSQL task lease. Successful processing deletes the SQS message. An exception leaves the message available for redelivery; the fifth receive moves it to the DLQ.

## Regional source control

Each connector operation uses HTTPS with a Secrets Manager service token and carries `X-Tenant-ID`, `X-Request-ID`, a task/source idempotency key and the hashed subject. The AIMD controller records response latency and success for a homogeneous tenant/source batch:

```text
healthy window and P95 <= target: window = min(maximum, window + 1)
HTTP failure or P95 > target:      window = max(minimum, floor(window * ratio))
```

Redis stores the window under `privacy:aimd:{tenant}:{source}` and active holders in a sorted set whose score is the admission-lease expiry. One Lua script removes expired holders, compares cardinality with the current window and admits a new task atomically. A second Lua script reads the current window, computes the next value and refreshes the TTL atomically, so all worker Pods observe and enforce the same upstream-pressure decision.

## Access and Delete completion

Access aggregates source receipts into canonical JSON. S3 stores the object under `artifacts/{tenant_id}/{request_id}.json` with SSE-KMS and SHA-256 metadata. The API returns a presigned GET URL valid for 300 seconds.

Delete scopes every mutation by tenant, subject hash and source, then queries the same predicate for active rows. A nonzero remaining count records a failed receipt. Completed and failed source receipts remain attached to the parent request for repair and replay.

An Access task can commit immediately before S3 upload or request aggregation. A duplicate SQS message sees the terminal task, rebuilds a missing artifact at the deterministic S3 key, refreshes the parent request and acknowledges the event without increasing the task attempt.

## Audit chain

Each tenant owns a monotonic sequence. PostgreSQL advisory transaction locks serialize chain-head updates. An event stores the previous hash, canonical payload hash and current event hash:

```text
payload_hash = SHA256(canonical_json(payload))
event_hash = SHA256(canonical_json(
  tenant_id, request_id, sequence, action, payload_hash, previous_hash
))
```

The unique `(tenant_id, sequence)` constraint blocks concurrent forks. Full recomputation detects payload mutation, reordering and interior deletion.

## AWS topology

Terraform provisions three availability zones, private EKS nodes, RDS PostgreSQL Multi-AZ, ElastiCache Redis with automatic failover, SQS/DLQ, S3/KMS, Secrets Manager and CloudWatch. API and worker Pods use separate Pod Identity roles. The Helm chart configures rolling deployments, health probes, topology spread, HPA and PDB.

The exact resource graph, IAM actions, alarms and failure drills are defined in [aws-production.md](aws-production.md).
