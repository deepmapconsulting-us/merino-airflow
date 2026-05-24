"""Campaign config status cache and lookup helpers for Meta traffic DAGs."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

REDIS_CONN_ID = "merino_redis"
SNAPSHOT_BUCKET = "airflow-run-us-west2"
REPORT_TIMEZONE = os.environ.get("META_REPORT_TIMEZONE", "America/Los_Angeles")
REPORT_PARTITION_HOURS = 4
CONFIG_CACHE_TTL_SECONDS = 2 * 24 * 60 * 60
CONFIG_BUCKET_HOURS = tuple(range(0, 24, REPORT_PARTITION_HOURS))
ACTIVE_STATUS = "active"
NOT_ACTIVE_STATUS = "not_active"
HYBRID_STATUS = "hybrid"


def get_redis():
    from airflow.providers.redis.hooks.redis import RedisHook  # type: ignore[import-not-found]

    return RedisHook(redis_conn_id=REDIS_CONN_ID).get_conn()


def config_cache_key(run_date: str, hour: int) -> str:
    return f"meta:campaign_config:{run_date}:{hour:02d}"


def cache_config_snapshot(snapshot: dict[str, Any], run_date: str, run_datetime: str, redis_client=None) -> str:
    redis_client = redis_client or get_redis()
    hour = int(run_datetime[9:11])
    key = config_cache_key(run_date, hour)
    redis_client.set(
        key,
        json.dumps(snapshot, separators=(",", ":"), sort_keys=True),
        ex=CONFIG_CACHE_TTL_SECONDS,
    )
    return key


def config_snapshot_uri(prefix: str, run_date: str, hour: int) -> str:
    local = datetime.fromisoformat(run_date).replace(
        hour=hour,
        minute=0,
        second=0,
        microsecond=0,
        tzinfo=ZoneInfo(REPORT_TIMEZONE),
    )
    return f"gs://{SNAPSHOT_BUCKET}/{prefix}/{run_date}/{local.strftime('%Y%m%dT%H%M%S%z')}/snapshot.json"


def load_config_snapshot(storage_client, prefix: str, run_date: str, hour: int, redis_client=None) -> dict[str, Any] | None:
    redis_client = redis_client or get_redis()
    key = config_cache_key(run_date, hour)
    cached = redis_client.get(key) if redis_client is not None else None
    if cached:
        if isinstance(cached, bytes):
            cached = cached.decode("utf-8")
        return json.loads(cached)

    if storage_client is None:
        return None

    try:
        snapshot = _read_json_from_gcs(storage_client, config_snapshot_uri(prefix, run_date, hour))
    except Exception as exc:
        print(f"meta_status: no campaign config for {run_date} {hour:02d}:00: {exc}")
        return None

    redis_client.set(
        key,
        json.dumps(snapshot, separators=(",", ":"), sort_keys=True),
        ex=CONFIG_CACHE_TTL_SECONDS,
    )
    return snapshot


def _read_json_from_gcs(storage_client, uri: str) -> dict[str, Any]:
    bucket_name, _, object_name = uri[5:].partition("/")
    return json.loads(storage_client.bucket(bucket_name).blob(object_name).download_as_text())


@dataclass(frozen=True)
class ConfigStatusMap:
    campaign_active: dict[str, bool]
    adset_active: dict[tuple[str, str], bool]
    ad_active: dict[tuple[str, str, str], bool]

    @classmethod
    def from_snapshot(cls, snapshot: dict[str, Any]) -> "ConfigStatusMap":
        campaign_active: dict[str, bool] = {}
        adset_active: dict[tuple[str, str], bool] = {}
        ad_active: dict[tuple[str, str, str], bool] = {}

        for account in snapshot.get("accounts", {}).values():
            for campaign in account.get("campaigns", []):
                campaign_id = str(campaign.get("id") or "")
                if not campaign_id:
                    continue
                campaign_is_active = _is_active(campaign.get("status"))
                campaign_active[campaign_id] = campaign_is_active

                for adset in campaign.get("adsets", []):
                    adset_id = str(adset.get("id") or "")
                    if not adset_id:
                        continue
                    adset_is_active = campaign_is_active and _is_active(adset.get("status"))
                    adset_active[(campaign_id, adset_id)] = adset_is_active

                    for ad in adset.get("ads", []):
                        ad_id = str(ad.get("id") or "")
                        if not ad_id:
                            continue
                        ad_active[(campaign_id, adset_id, ad_id)] = adset_is_active and _is_active(ad.get("status"))

        return cls(campaign_active=campaign_active, adset_active=adset_active, ad_active=ad_active)

    def campaign_status(self, campaign_id: str) -> bool:
        return self.campaign_active.get(str(campaign_id), False)

    def adset_status(self, campaign_id: str, adset_id: str) -> bool:
        return self.adset_active.get((str(campaign_id), str(adset_id)), False)

    def ad_status(self, campaign_id: str, adset_id: str, ad_id: str) -> bool:
        return self.ad_active.get((str(campaign_id), str(adset_id), str(ad_id)), False)


class DailyStatusResolver:
    def __init__(self, storage_client, prefix: str, redis_client=None) -> None:
        self.storage_client = storage_client
        self.prefix = prefix
        self.redis_client = redis_client or get_redis()
        self.maps_by_date: dict[str, list[ConfigStatusMap]] = {}

    def campaign_status(self, report_date: str, campaign_id: str) -> str:
        return self._status(report_date, lambda status_map: status_map.campaign_status(campaign_id))

    def adset_status(self, report_date: str, campaign_id: str, adset_id: str) -> str:
        return self._status(report_date, lambda status_map: status_map.adset_status(campaign_id, adset_id))

    def ad_status(self, report_date: str, campaign_id: str, adset_id: str, ad_id: str) -> str:
        return self._status(report_date, lambda status_map: status_map.ad_status(campaign_id, adset_id, ad_id))

    def _status(self, report_date: str, is_active) -> str:
        maps = self._maps_for_date(report_date)
        return status_from_checks([is_active(status_map) for status_map in maps])

    def _maps_for_date(self, report_date: str) -> list[ConfigStatusMap]:
        report_date = str(report_date)
        if report_date not in self.maps_by_date:
            maps = []
            for hour in CONFIG_BUCKET_HOURS:
                snapshot = load_config_snapshot(self.storage_client, self.prefix, report_date, hour, self.redis_client)
                if snapshot:
                    maps.append(ConfigStatusMap.from_snapshot(snapshot))
            if not maps:
                print(f"meta_status: no config snapshots available for daily report_date={report_date}")
            self.maps_by_date[report_date] = maps
        return self.maps_by_date[report_date]


class HourlyStatusResolver:
    def __init__(self, storage_client, prefix: str, redis_client=None) -> None:
        self.storage_client = storage_client
        self.prefix = prefix
        self.redis_client = redis_client or get_redis()
        self.maps_by_bucket: dict[tuple[str, int], ConfigStatusMap | None] = {}

    def ad_status(self, metric_hour: Any, campaign_id: str, adset_id: str, ad_id: str) -> str:
        status_map = self._map_for_metric_hour(metric_hour)
        if not status_map:
            return NOT_ACTIVE_STATUS
        return ACTIVE_STATUS if status_map.ad_status(campaign_id, adset_id, ad_id) else NOT_ACTIVE_STATUS

    def _map_for_metric_hour(self, metric_hour: Any) -> ConfigStatusMap | None:
        local = metric_hour if hasattr(metric_hour, "astimezone") else datetime.fromisoformat(str(metric_hour))
        if local.tzinfo is None:
            local = local.replace(tzinfo=ZoneInfo(REPORT_TIMEZONE))
        local = local.astimezone(ZoneInfo(REPORT_TIMEZONE))
        bucket_hour = (local.hour // REPORT_PARTITION_HOURS) * REPORT_PARTITION_HOURS
        key = (local.strftime("%Y-%m-%d"), bucket_hour)
        if key not in self.maps_by_bucket:
            snapshot = load_config_snapshot(self.storage_client, self.prefix, key[0], key[1], self.redis_client)
            self.maps_by_bucket[key] = ConfigStatusMap.from_snapshot(snapshot) if snapshot else None
        return self.maps_by_bucket[key]


def status_from_checks(values: list[bool]) -> str:
    if not values:
        return NOT_ACTIVE_STATUS
    if all(values):
        return ACTIVE_STATUS
    if not any(values):
        return NOT_ACTIVE_STATUS
    return HYBRID_STATUS


def _is_active(status: Any) -> bool:
    return str(status or "").upper() == "ACTIVE"
