from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any

REDIS_AIMD_LUA = """
local current = tonumber(redis.call('GET', KEYS[1]) or ARGV[1])
local p95 = tonumber(ARGV[2])
local failure_rate = tonumber(ARGV[3])
local target = tonumber(ARGV[4])
local minimum = tonumber(ARGV[5])
local maximum = tonumber(ARGV[6])
local additive = tonumber(ARGV[7])
local decrease_ratio = tonumber(ARGV[8])
local ttl_seconds = tonumber(ARGV[9])
local next_limit
if failure_rate > 0 or p95 > target then
    next_limit = math.max(minimum, math.floor(current * decrease_ratio))
else
    next_limit = math.min(maximum, current + additive)
end
redis.call('SET', KEYS[1], next_limit, 'EX', ttl_seconds)
return {current, next_limit}
"""

REDIS_ADMISSION_LUA = """
local window_key = KEYS[1]
local inflight_key = KEYS[2]
local now_ms = tonumber(ARGV[1])
local expires_ms = tonumber(ARGV[2])
local holder = ARGV[3]
local default_limit = tonumber(ARGV[4])
local ttl_seconds = tonumber(ARGV[5])
local limit = tonumber(redis.call('GET', window_key) or default_limit)
redis.call('ZREMRANGEBYSCORE', inflight_key, '-inf', now_ms)
if redis.call('ZCARD', inflight_key) >= limit then
    return {0, limit}
end
redis.call('ZADD', inflight_key, expires_ms, holder)
redis.call('EXPIRE', inflight_key, ttl_seconds)
return {1, limit}
"""


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(math.ceil(len(ordered) * fraction) - 1, len(ordered) - 1)
    return ordered[max(index, 0)]


@dataclass
class AimdConcurrencyController:
    """Adjust connector concurrency from measured tail latency and failures."""

    limit: int = 4
    minimum: int = 1
    maximum: int = 64
    target_p95_seconds: float = 0.8
    additive_step: int = 1
    decrease_ratio: float = 0.65
    _latencies: list[float] = field(default_factory=list, repr=False)
    _failures: int = field(default=0, repr=False)

    def observe(self, latency_seconds: float, *, succeeded: bool) -> None:
        self._latencies.append(latency_seconds)
        self._failures += int(not succeeded)

    def adjust(self) -> dict[str, float | int]:
        samples = len(self._latencies)
        p95 = percentile(self._latencies, 0.95)
        failure_rate = self._failures / samples if samples else 0.0
        previous = self.limit
        if samples and (failure_rate > 0 or p95 > self.target_p95_seconds):
            self.limit = max(self.minimum, math.floor(self.limit * self.decrease_ratio))
        elif samples:
            self.limit = min(self.maximum, self.limit + self.additive_step)
        self._latencies.clear()
        self._failures = 0
        return {
            "previous_limit": previous,
            "next_limit": self.limit,
            "samples": samples,
            "p95_seconds": p95,
            "failure_rate": failure_rate,
        }


class RedisAimdState:
    """Maintain one atomic AIMD window per tenant and regional data source."""

    def __init__(
        self, client: Any, *, prefix: str = "privacy:aimd", ttl_seconds: int = 900
    ) -> None:
        self.client = client
        self.prefix = prefix.rstrip(":")
        self.ttl_seconds = ttl_seconds

    def key(self, tenant_id: str, source: str) -> str:
        return f"{self.prefix}:{tenant_id}:{source}"

    def inflight_key(self, tenant_id: str, source: str) -> str:
        return f"{self.key(tenant_id, source)}:inflight"

    async def get_limit(self, tenant_id: str, source: str, *, default: int) -> int:
        value = await self.client.get(self.key(tenant_id, source))
        return int(value) if value is not None else default

    async def adjust(
        self,
        tenant_id: str,
        source: str,
        *,
        default: int,
        p95_seconds: float,
        failure_rate: float,
        target_p95_seconds: float,
        minimum: int,
        maximum: int,
        additive_step: int,
        decrease_ratio: float,
    ) -> tuple[int, int]:
        result = await self.client.eval(
            REDIS_AIMD_LUA,
            1,
            self.key(tenant_id, source),
            default,
            p95_seconds,
            failure_rate,
            target_p95_seconds,
            minimum,
            maximum,
            additive_step,
            decrease_ratio,
            self.ttl_seconds,
        )
        return int(result[0]), int(result[1])

    async def acquire(
        self,
        tenant_id: str,
        source: str,
        *,
        holder: str,
        default: int,
        lease_seconds: float,
    ) -> tuple[bool, int]:
        now_ms = int(time.time() * 1000)
        result = await self.client.eval(
            REDIS_ADMISSION_LUA,
            2,
            self.key(tenant_id, source),
            self.inflight_key(tenant_id, source),
            now_ms,
            now_ms + int(lease_seconds * 1000),
            holder,
            default,
            self.ttl_seconds,
        )
        return bool(result[0]), int(result[1])

    async def release(self, tenant_id: str, source: str, *, holder: str) -> None:
        await self.client.zrem(self.inflight_key(tenant_id, source), holder)

    @classmethod
    def from_url(cls, redis_url: str, *, ttl_seconds: int = 900) -> RedisAimdState:
        from redis.asyncio import Redis

        return cls(Redis.from_url(redis_url, decode_responses=True), ttl_seconds=ttl_seconds)
