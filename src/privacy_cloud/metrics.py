from prometheus_client import Counter, Histogram

REQUEST_CREATED = Counter("privacy_requests_created_total", "Created privacy requests", ["kind"])
REQUEST_COMPLETED = Counter(
    "privacy_requests_completed_total", "Completed privacy requests", ["kind", "status"]
)
TASK_DURATION = Histogram(
    "privacy_task_duration_seconds",
    "Privacy source task duration",
    ["source", "kind"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)
