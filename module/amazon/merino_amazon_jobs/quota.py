"""Shared SP-API quota lock used by Amazon ingestion pods.

Airflow can still schedule overlapping Amazon DAGs; this lock keeps only one
SP-API job in the Reports/Orders quota at a time. createReport is also paced:

- all report types: 1 request / 60s (createReport 0.0167 rps)
- GET_SALES_AND_TRAFFIC_REPORT: 3 requests / 300s
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from typing import Any

logger = logging.getLogger(__name__)

SALES_TRAFFIC_REPORT_TYPE = "GET_SALES_AND_TRAFFIC_REPORT"
JOB_LOCK_KEY = "sp_api_job_lock"
JOB_LOCK_HELD_ENV = "SP_API_JOB_LOCK_HELD"
CREATE_REPORT_KEY = "sp_api_create_report"
SALES_TRAFFIC_KEY = "sp_api_sales_traffic_report"
COOLDOWN_KEY = "sp_api_create_report_cooldown"

CREATE_REPORT_INTERVAL_SEC = 60
SALES_TRAFFIC_WINDOW_SEC = 300
SALES_TRAFFIC_MAX_IN_WINDOW = 3

_TAKE_SLOT_LUA = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local max_count = tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
if redis.call('ZCARD', key) < max_count then
  redis.call('ZADD', key, now, ARGV[4])
  redis.call('EXPIRE', key, window)
  return 0
end
local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
if oldest == nil or #oldest < 2 then
  return window
end
return window - (now - tonumber(oldest[2]))
"""

_local_create_at = 0.0
_local_sales_at: deque[float] = deque()
_local_lock = threading.Lock()


def redis_url_from_env(environ: dict[str, str] | None = None) -> str:
    environ = environ or os.environ
    explicit = (environ.get("REDIS_URL") or "").strip()
    if explicit:
        return explicit
    host = (
        (environ.get("MCP_REDIS_HOST") or "").strip()
        or (environ.get("MERINO_REDIS_HOST") or "").strip()
    )
    if not host:
        return ""
    port = (
        (environ.get("MCP_REDIS_PORT") or "").strip()
        or (environ.get("MERINO_REDIS_PORT") or "6379").strip()
    )
    database = (
        (environ.get("MCP_REDIS_DB") or "").strip()
        or (environ.get("MERINO_REDIS_DB") or "0").strip()
    )
    password = environ.get("MCP_REDIS_PASSWORD") or environ.get(
        "MERINO_REDIS_PASSWORD", ""
    )
    credentials = f":{password}@" if password else ""
    return f"redis://{credentials}{host}:{port}/{database}"


def redis_client(environ: dict[str, str] | None = None) -> Any | None:
    url = redis_url_from_env(environ)
    if not url:
        return None
    try:
        from redis import Redis

        return Redis.from_url(url, decode_responses=True, socket_timeout=5)
    except Exception as exc:
        logger.warning("sp_api_quota_redis_unavailable error=%s", exc)
        return None


def reset_local_quota() -> None:
    """Clear in-process pacing so tests do not inherit 60s waits."""
    global _local_create_at
    with _local_lock:
        _local_create_at = 0.0
        _local_sales_at.clear()


def retry_backoff_seconds(attempt: int, status: int | None) -> float:
    if status == 429:
        return float(60 * (2**attempt))
    return float(2**attempt)


def mark_create_report_throttled(
    seconds: float,
    *,
    client: Any | None = None,
) -> None:
    ttl = max(1, int(seconds))
    active = redis_client() if client is None else client
    if active is None:
        return
    try:
        current = int(active.ttl(COOLDOWN_KEY) or 0)
        if current >= ttl:
            return
        active.set(COOLDOWN_KEY, "1", ex=ttl)
        logger.warning("sp_api_quota_cooldown ttl=%s", ttl)
    except Exception as exc:
        logger.warning("sp_api_quota_redis_unavailable operation=cooldown error=%s", exc)


def wait_for_create_report(
    report_type: str,
    *,
    client: Any | None = None,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.time,
) -> None:
    """Block until createReport is within Amazon's documented quotas."""
    active = redis_client() if client is None else client
    _wait_cooldown(active, sleep=sleep)
    _take_slot(
        active,
        CREATE_REPORT_KEY,
        window=CREATE_REPORT_INTERVAL_SEC,
        max_count=1,
        sleep=sleep,
        now=now,
        local=_wait_local_create,
    )
    if report_type == SALES_TRAFFIC_REPORT_TYPE:
        _take_slot(
            active,
            SALES_TRAFFIC_KEY,
            window=SALES_TRAFFIC_WINDOW_SEC,
            max_count=SALES_TRAFFIC_MAX_IN_WINDOW,
            sleep=sleep,
            now=now,
            local=_wait_local_sales_traffic,
        )


@contextmanager
def sp_api_job_lock(
    *,
    owner: str,
    client: Any | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> Iterator[None]:
    """Hold an exclusive Redis lock for one SP-API ingestion process."""
    if os.environ.get(JOB_LOCK_HELD_ENV):
        yield
        return

    active = redis_client() if client is None else client
    if active is None:
        logger.warning("sp_api_job_lock skipped owner=%s reason=no_redis", owner)
        yield
        return

    key = os.environ.get("SP_API_JOB_LOCK_KEY", JOB_LOCK_KEY).strip() or JOB_LOCK_KEY
    ttl = max(60, int(os.environ.get("SP_API_JOB_LOCK_TTL_SEC", "10800")))
    token = f"{owner}:{uuid.uuid4()}"
    while True:
        try:
            acquired = bool(active.set(key, token, nx=True, ex=ttl))
        except Exception as exc:
            logger.warning("sp_api_job_lock skipped owner=%s error=%s", owner, exc)
            yield
            return
        if acquired:
            break
        wait = 15
        try:
            remaining = int(active.ttl(key) or 0)
            if remaining > 0:
                wait = min(15, remaining)
        except Exception:
            pass
        logger.info("sp_api_job_lock waiting owner=%s sleep=%s", owner, wait)
        sleep(wait)

    stop = threading.Event()

    def _heartbeat() -> None:
        while not stop.wait(30):
            try:
                if active.get(key) == token:
                    active.expire(key, ttl)
            except Exception:
                return

    thread = threading.Thread(target=_heartbeat, daemon=True)
    thread.start()
    os.environ[JOB_LOCK_HELD_ENV] = token
    logger.info("sp_api_job_lock acquired owner=%s", owner)
    try:
        yield
    finally:
        stop.set()
        os.environ.pop(JOB_LOCK_HELD_ENV, None)
        try:
            if active.get(key) == token:
                active.delete(key)
        except Exception as exc:
            logger.warning("sp_api_job_lock release failed owner=%s error=%s", owner, exc)
        logger.info("sp_api_job_lock released owner=%s", owner)


def locked_cli(argv: Sequence[str] | None = None) -> int:
    """Hold the SP-API job lock for one pod command, including bash loops."""
    command = list(sys.argv[1:] if argv is None else argv)
    if not command:
        raise SystemExit("usage: merino-amazon-with-lock COMMAND [ARGS...]")
    with sp_api_job_lock(owner=command[0]):
        return int(subprocess.run(command, check=False).returncode)


def _wait_cooldown(
    client: Any | None,
    *,
    sleep: Callable[[float], None],
) -> None:
    if client is None:
        return
    try:
        while (ttl := int(client.ttl(COOLDOWN_KEY) or 0)) > 0:
            logger.warning("sp_api_quota_wait cooldown=%s", ttl)
            sleep(ttl)
    except Exception as exc:
        logger.warning("sp_api_quota_redis_unavailable operation=wait_cooldown error=%s", exc)


def _take_slot(
    client: Any | None,
    key: str,
    *,
    window: int,
    max_count: int,
    sleep: Callable[[float], None],
    now: Callable[[], float],
    local: Callable[[Callable[[], float], Callable[[float], None]], None],
) -> None:
    if client is None:
        local(now, sleep)
        return
    while True:
        try:
            wait = float(
                client.eval(
                    _TAKE_SLOT_LUA,
                    1,
                    key,
                    now(),
                    window,
                    max_count,
                    str(uuid.uuid4()),
                )
            )
        except Exception as exc:
            logger.warning("sp_api_quota_redis_unavailable operation=slot error=%s", exc)
            local(now, sleep)
            return
        if wait <= 0:
            return
        logger.info("sp_api_quota_wait key=%s sleep=%.1f", key, wait)
        sleep(wait)


def _wait_local_create(
    now: Callable[[], float],
    sleep: Callable[[float], None],
) -> None:
    global _local_create_at
    with _local_lock:
        wait = CREATE_REPORT_INTERVAL_SEC - (now() - _local_create_at)
        if wait > 0:
            sleep(wait)
        _local_create_at = now()


def _wait_local_sales_traffic(
    now: Callable[[], float],
    sleep: Callable[[float], None],
) -> None:
    with _local_lock:
        current = now()
        while (
            _local_sales_at
            and current - _local_sales_at[0] >= SALES_TRAFFIC_WINDOW_SEC
        ):
            _local_sales_at.popleft()
        if len(_local_sales_at) >= SALES_TRAFFIC_MAX_IN_WINDOW:
            wait = SALES_TRAFFIC_WINDOW_SEC - (current - _local_sales_at[0])
            if wait > 0:
                sleep(wait)
            current = now()
            while (
                _local_sales_at
                and current - _local_sales_at[0] >= SALES_TRAFFIC_WINDOW_SEC
            ):
                _local_sales_at.popleft()
        _local_sales_at.append(current)
