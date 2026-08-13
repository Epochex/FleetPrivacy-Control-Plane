# Connected-device privacy service requirements

## Business context

Retail IoT platforms distribute customer and operator data across store accounts,
access-point configuration, electronic shelf label ownership, device telemetry,
firmware jobs and support diagnostics. Enterprise privacy requests must locate,
export or erase those records across regions while preserving task receipts and a
reviewable action history.

## Service objectives

| Objective | Target | Mechanism |
| --- | ---: | --- |
| request acceptance availability | 99.95% monthly | EKS multi-zone API replicas and RDS Multi-AZ |
| API admission latency | p95 below 800 ms at 20 concurrent clients | bounded transaction, indexed tenant queries and pooled connections |
| source-task completion | 99% within 15 minutes | SQS wake-up, autoscaled workers and per-source receipts |
| duplicate side effects | 0 in replay and redelivery tests | tenant idempotency key and `(request_id, source)` task identity |
| deletion evidence | readback result for every selected source | delete followed by source query and stored receipt |
| export confidentiality | encrypted object and time-limited download | S3 SSE-KMS and presigned GET URL |
| unrecoverable task visibility | alarm within 5 minutes | SQS DLQ and CloudWatch backlog alarms |
| audit chain integrity | every tenant chain verifies | tenant-serialized hash-chain append and scheduled verification |

## Workload model

- 55,000 stores form the public company-scale capacity-planning envelope.
- A privacy command targets five logical data domains: profile, devices,
  telemetry, jobs and support logs.
- API bursts come from enterprise compliance portals and batch account closure.
- Source actions have uneven latency because store and regional services can be
  temporarily unreachable.
- Repeated client submissions and repeated queue delivery are normal inputs.

## Processing contract

1. The API authenticates the caller, resolves the tenant and validates the source set.
2. One RDS transaction writes the parent request, five source tasks, the first audit
   event, one request event and five task-ready Outbox records.
3. The Outbox relay publishes task wake-up messages to SQS and marks each event
   published after the AWS API confirms acceptance.
4. EKS workers receive messages, claim pending database tasks and extend SQS
   visibility while a source action is active.
5. Access tasks collect records. Delete tasks update the source and query it again.
6. The worker stores the receipt, recalculates the parent state and acknowledges the
   message after the database commit.
7. Access results are written to S3 with SSE-KMS, SHA-256 metadata and a tenant-prefixed
   object key. The API issues a short-lived presigned download URL.
8. SQS routes repeatedly failing messages to the DLQ. CloudWatch alarms on queue age,
   DLQ depth, API error rate and RDS capacity.

## Failure acceptance tests

| Injection | Expected state |
| --- | --- |
| repeat the same HTTP request 200 times | one request ID and no extra source-task attempt |
| deliver the same SQS message twice | second delivery finds no claimable task and performs no source action |
| terminate a worker after task claim | visibility and database lease expire; another worker resumes the task |
| fail S3 upload after source completion | source receipts remain committed; artifact construction retries |
| fail queue publication after database commit | Outbox stays unpublished and the relay retries |
| make one source unavailable | sibling receipts remain; parent exposes the failed source and attempt count |
| alter or reorder an audit row | chain verification returns the first invalid sequence |

The repository benchmark, replay test and fault tests publish the measured result for
each implemented acceptance condition.
