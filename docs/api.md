# API

The OpenAPI UI is served at `/docs`; machine-readable OpenAPI is available at
`/openapi.json`. All `/v1/privacy-requests`, `/v1/admin` and tenant audit routes
require these headers:

| Header | Meaning |
| --- | --- |
| `X-Tenant-ID` | tenant partition used in all resource queries |
| `X-API-Key` | service API key |
| `Idempotency-Key` | required on request creation, 8 to 128 characters |

## Create a privacy request

`POST /v1/privacy-requests`

```json
{
  "subject_key": "owner@example.com",
  "kind": "access",
  "sources": ["profile", "devices", "telemetry", "jobs", "support_logs"]
}
```

`kind` accepts `access` and `delete`. Unknown sources fail validation with
HTTP 422. The first successful submission returns HTTP 201. Repeating the same
command under the same tenant and idempotency key returns HTTP 200 with the
original request ID. Reusing that key for another subject, kind or source set
returns HTTP 409.

The response includes parent state and per-source receipts:

```json
{
  "id": "28b004c5-79f4-44c8-b3ae-af0fe94fbb13",
  "tenant_id": "maker-lab",
  "kind": "access",
  "status": "queued",
  "requested_sources": ["profile", "devices"],
  "artifact_path": null,
  "version": 1,
  "created_at": "2026-08-13T10:30:00Z",
  "updated_at": "2026-08-13T10:30:00Z",
  "tasks": [
    {
      "source": "profile",
      "status": "pending",
      "attempt": 0,
      "result_summary": {},
      "error": null
    }
  ]
}
```

## Query requests

| Method and path | Operation |
| --- | --- |
| `GET /v1/privacy-requests/{request_id}` | fetch one tenant-scoped request |
| `GET /v1/privacy-requests?status=running&limit=50&offset=0` | list and filter requests |
| `GET /v1/privacy-requests/{request_id}/artifact` | download a completed access artifact |

The artifact route returns HTTP 409 while packaging is pending. The resolved
file path must remain under the configured artifact root.

## Execute queued work

`POST /v1/admin/process-once?worker_id=worker-a&batch_size=32`

```json
{"claimed": 10, "succeeded": 10, "failed": 0}
```

This endpoint runs one worker pass for demos, tests and benchmark automation.
PostgreSQL workers claim rows using `FOR UPDATE SKIP LOCKED`; the SQLite local
mode uses one atomic update with returning. Worker IDs are prefixed with the
authenticated tenant.

## Seed the reproducible device-cloud adapters

`POST /v1/admin/seed`

```json
{"subject_key": "owner@example.com", "records_per_source": 4}
```

The endpoint inserts deterministic records into all five source domains and is
used by tests and benchmarks.

## Verify tenant audit history

`GET /v1/tenants/audit-chain`

```json
{"tenant_id": "maker-lab", "valid": true}
```

The verifier checks sequence numbers, previous hashes and recalculated event
hashes for the authenticated tenant.

## Service endpoints

| Method and path | Operation |
| --- | --- |
| `GET /v1/healthz` | database probe and unpublished Outbox count |
| `GET /v1/health` | health alias hidden from OpenAPI |
| `GET /v1/metrics` | Prometheus text exposition |

## Status model

| Request status | Meaning |
| --- | --- |
| `queued` | all selected source tasks are pending |
| `running` | at least one task is active or pending after work began |
| `verifying` | a deletion task is checking remaining active records |
| `completed` | every selected source task succeeded |
| `partial` | terminal tasks contain both success and failure |
| `failed` | every selected source task failed |

| Task status | Meaning |
| --- | --- |
| `pending` | available for claim |
| `running` | owned until `lease_expires_at` |
| `succeeded` | verified receipt saved |
| `failed` | failure string and attempt saved |
