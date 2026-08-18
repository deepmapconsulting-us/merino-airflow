from __future__ import annotations

import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

MODULE_ROOT = Path(__file__).resolve().parents[1] / "module" / "amazon"
sys.path.insert(0, str(MODULE_ROOT))

from merino_amazon_jobs.quota import (
    JOB_LOCK_HELD_ENV,
    SALES_TRAFFIC_REPORT_TYPE,
    locked_cli,
    redis_url_from_env,
    reset_local_quota,
    retry_backoff_seconds,
    sp_api_job_lock,
    wait_for_create_report,
)


class DictRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.ttls: dict[str, int] = {}

    def set(self, key: str, value: str, nx: bool = False, ex: int | None = None) -> bool:
        if nx and key in self.values:
            return False
        self.values[key] = value
        if ex is not None:
            self.ttls[key] = ex
        return True

    def get(self, key: str) -> str | None:
        return self.values.get(key)

    def delete(self, key: str) -> int:
        return 1 if self.values.pop(key, None) is not None else 0

    def ttl(self, key: str) -> int:
        if key not in self.values:
            return -2
        return self.ttls.get(key, -1)

    def expire(self, key: str, ttl: int) -> bool:
        if key not in self.values:
            return False
        self.ttls[key] = ttl
        return True

    def eval(self, _script: str, _numkeys: int, *args: object) -> float:
        return 0


def test_redis_url_from_env_prefers_explicit_url() -> None:
    assert redis_url_from_env({"REDIS_URL": "redis://example:6379/2"}) == (
        "redis://example:6379/2"
    )


def test_redis_url_from_env_builds_merino_credentials() -> None:
    assert (
        redis_url_from_env(
            {
                "MERINO_REDIS_HOST": "merino-mcp-redis.merino-mcp.svc.cluster.local",
                "MERINO_REDIS_PASSWORD": "secret",
            }
        )
        == "redis://:secret@merino-mcp-redis.merino-mcp.svc.cluster.local:6379/0"
    )


def test_retry_backoff_seconds_uses_minute_scale_for_429() -> None:
    assert retry_backoff_seconds(0, 429) == 60
    assert retry_backoff_seconds(1, 429) == 120
    assert retry_backoff_seconds(0, 500) == 1


def test_wait_for_create_report_paces_sales_traffic_locally() -> None:
    reset_local_quota()
    sleeps: list[float] = []
    clock = {"t": 1_000.0}

    def now() -> float:
        return clock["t"]

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock["t"] += seconds

    with patch("merino_amazon_jobs.quota.redis_client", return_value=None):
        for _ in range(3):
            wait_for_create_report(
                SALES_TRAFFIC_REPORT_TYPE,
                sleep=sleep,
                now=now,
            )
        assert sleeps == [60, 60]
        wait_for_create_report(
            SALES_TRAFFIC_REPORT_TYPE,
            sleep=sleep,
            now=now,
        )

    assert sleeps == [60, 60, 60, 120]


def test_job_lock_waits_then_acquires_and_releases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(JOB_LOCK_HELD_ENV, raising=False)
    client = DictRedis()
    client.values["sp_api_job_lock"] = "other"
    client.ttls["sp_api_job_lock"] = 4
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        client.values.clear()

    with sp_api_job_lock(owner="sales_traffic", client=client, sleep=sleep):
        assert JOB_LOCK_HELD_ENV in __import__("os").environ
        assert client.get("sp_api_job_lock").startswith("sales_traffic:")

    assert sleeps == [4]
    assert "sp_api_job_lock" not in client.values
    assert JOB_LOCK_HELD_ENV not in __import__("os").environ


def test_job_lock_is_reentrant_when_parent_already_holds_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(JOB_LOCK_HELD_ENV, "parent-token")
    client = MagicMock()
    with sp_api_job_lock(owner="listings", client=client, sleep=lambda _s: None):
        pass
    client.set.assert_not_called()


def test_locked_cli_runs_command_under_lock() -> None:
    with (
        patch(
            "merino_amazon_jobs.quota.sp_api_job_lock",
            return_value=nullcontext(),
        ),
        patch(
            "merino_amazon_jobs.quota.subprocess.run",
            return_value=SimpleNamespace(returncode=7),
        ) as run,
    ):
        assert locked_cli(["merino-amazon-jobs", "--dry-run"]) == 7
    run.assert_called_once_with(["merino-amazon-jobs", "--dry-run"], check=False)
