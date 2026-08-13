import pytest

from privacy_cloud.services.adaptive_concurrency import AimdConcurrencyController, RedisAimdState


def test_aimd_additively_increases_below_tail_latency_target() -> None:
    controller = AimdConcurrencyController(limit=4, target_p95_seconds=0.5)
    for latency in (0.10, 0.12, 0.11, 0.13):
        controller.observe(latency, succeeded=True)

    window = controller.adjust()

    assert window["previous_limit"] == 4
    assert window["next_limit"] == 5


def test_aimd_multiplicatively_decreases_on_failure() -> None:
    controller = AimdConcurrencyController(limit=10, decrease_ratio=0.6)
    for succeeded in (True, True, False, True):
        controller.observe(0.1, succeeded=succeeded)

    window = controller.adjust()

    assert window["failure_rate"] == 0.25
    assert window["next_limit"] == 6


def test_aimd_multiplicatively_decreases_on_tail_latency() -> None:
    controller = AimdConcurrencyController(limit=8, target_p95_seconds=0.5)
    for latency in (0.1, 0.2, 0.3, 0.9):
        controller.observe(latency, succeeded=True)

    window = controller.adjust()

    assert window["p95_seconds"] == 0.9
    assert window["next_limit"] == 5


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, int] = {}
        self.eval_calls: list[tuple] = []

    async def get(self, key: str) -> int | None:
        return self.values.get(key)

    async def eval(self, script: str, key_count: int, key: str, *arguments):
        self.eval_calls.append((script, key_count, key, *arguments))
        current = self.values.get(key, int(arguments[0]))
        p95, failure_rate, target = map(float, arguments[1:4])
        minimum, maximum, additive = map(int, arguments[4:7])
        decrease_ratio = float(arguments[7])
        if failure_rate > 0 or p95 > target:
            next_limit = max(minimum, int(current * decrease_ratio))
        else:
            next_limit = min(maximum, current + additive)
        self.values[key] = next_limit
        return [current, next_limit]


@pytest.mark.asyncio
async def test_redis_aimd_state_is_scoped_and_updates_atomically() -> None:
    client = FakeRedis()
    state = RedisAimdState(client, ttl_seconds=600)

    assert await state.get_limit("tenant-a", "telemetry", default=4) == 4
    previous, current = await state.adjust(
        "tenant-a",
        "telemetry",
        default=4,
        p95_seconds=0.2,
        failure_rate=0,
        target_p95_seconds=0.8,
        minimum=1,
        maximum=32,
        additive_step=1,
        decrease_ratio=0.5,
    )
    assert (previous, current) == (4, 5)
    assert await state.get_limit("tenant-a", "telemetry", default=4) == 5
    assert await state.get_limit("tenant-b", "telemetry", default=4) == 4
    assert "redis.call('SET'" in client.eval_calls[0][0]
