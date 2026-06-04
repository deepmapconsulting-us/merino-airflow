"""Daily creative media download + analysis via media-analysis-mcp FastAPI.

Reads active ads from the latest ``facebook_campaign_config_update`` GCS snapshot,
groups work by adset, and for each eligible ad POSTs to media-analysis-mcp:

1. ``/api/v1/download-ad-creative-assets`` — cache media on the MCP server (Redis + disk)
2. ``/api/v1/creative-media-analysis`` — LLM video/audio analysis (cached in MCP Redis)

Airflow does not port-forward Redis; the MCP pod connects to cluster Redis directly.

## Required Airflow Variables (GSM secrets)

| Variable | Env fallback | Purpose |
|----------|--------------|---------|
| ``meta_access_token`` | ``META_ACCESS_TOKEN`` | Meta Graph Bearer (download) |
| ``meta_mcp_gateway_token`` | ``META_MCP_GATEWAY_TOKEN`` | ``X-MCP-Gateway-Token`` header |
| ``media_analysis_url`` | ``MEDIA_ANALYSIS_URL`` | MCP base URL (default in-cluster: ``http://media-analysis-mcp.merino-mcp.svc.cluster.local:8080``; override with public URL for local runs) |

OpenAI credentials are configured server-side on media-analysis-mcp (``OPENAI_API_KEY`` env from Kubernetes secret). Do not pass API keys from Airflow.

## Optional tuning Variables

| Variable | Env fallback | Default |
|----------|--------------|---------|
| ``facebook_active_accounts`` | ``FACEBOOK_ACTIVE_ACCOUNTS`` | all accounts in snapshot |
| ``FACEBOOK_TRAFFIC_LOOKUP_WINDOWS`` | ``FACEBOOK_TRAFFIC_LOOKUP_WINDOWS`` | ``3`` days |
| ``media_analysis_sample_sec`` | ``TEST_VIDEO_SAMPLE_SEC`` | ``3`` (``get_video_frame_in_sec``) |
| ``media_analysis_frame_interval_sec`` | ``TEST_SPLIT_FRAME_BY_SEC`` | ``1`` (``split_frame_by_sec``) |
| ``media_analysis_force_refresh`` | — | ``false`` (download cache) |
| ``media_analysis_analysis_force_refresh`` | — | ``false`` (analysis cache) |
| ``media_analysis_save_to_gcs`` | — | ``true`` (upload downloads to GCS) |
| ``MEDIA_ANALYSIS_CONFIG`` | ``MEDIA_ANALYSIS_CONFIG`` | optional per-ad analysis config overrides |
| ``media_analysis_max_active_tasks`` | — | ``8`` (max concurrent tasks per DAG run) |
"""

from __future__ import annotations

import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag, task  # type: ignore[import-not-found]
from airflow.utils.task_group import TaskGroup  # type: ignore[import-not-found]

try:
    from airflow.providers.standard.sensors.external_task import ExternalTaskSensor  # type: ignore[import-not-found]
except ImportError:
    from airflow.sensors.external_task import ExternalTaskSensor  # type: ignore[import-not-found,no-redef]

from meta_gcs import (
    REPORT_TIMEZONE,
    campaign_config_logical_date,
    gcs_console_link,
    meta_access_token,
    read_json_from_gcs,
    read_latest_snapshot_pointer,
    report_partition_datetime,
    resolve_logical_date_from_context,
    variable_get,
)

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
if MODULE_PATH.exists():
    sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs.media_analysis import (  # noqa: E402  # type: ignore[import-not-found]
    analysis_targets_from_download,
    creative_media_analysis,
    download_ad_creative_assets,
    mcp_gateway_token,
    media_analysis_config_by_ad,
    media_analysis_config_for_ad,
    media_analysis_base_url,
    upsert_creative_media_analysis,
)
from merino_meta_jobs.traffic import (  # noqa: E402  # type: ignore[import-not-found]
    DEFAULT_TRAFFIC_LOOKUP_WINDOW_DAYS,
    media_analysis_ads_from_adset,
    traffic_accounts_from_config,
)

DAG_ID = "meta_creative_media_analysis"
CAMPAIGN_CONFIG_DAG_ID = "facebook_campaign_config_update"
CONFIG_GCS_PREFIX = "facebook_campaign_config_update"
ACTIVE_ACCOUNTS_VARIABLE_NAME = "facebook_active_accounts"
ACTIVE_ACCOUNTS_ENV = "FACEBOOK_ACTIVE_ACCOUNTS"
LOOKUP_WINDOW_VARIABLE_NAME = "FACEBOOK_TRAFFIC_LOOKUP_WINDOWS"
LOOKUP_WINDOW_ENV = "FACEBOOK_TRAFFIC_LOOKUP_WINDOWS"
SAMPLE_SEC_VARIABLE = "media_analysis_sample_sec"
SAMPLE_SEC_ENV = "TEST_VIDEO_SAMPLE_SEC"
FRAME_INTERVAL_VARIABLE = "media_analysis_frame_interval_sec"
FRAME_INTERVAL_ENV = "TEST_SPLIT_FRAME_BY_SEC"
DOWNLOAD_FORCE_REFRESH_VARIABLE = "media_analysis_force_refresh"
ANALYSIS_FORCE_REFRESH_VARIABLE = "media_analysis_analysis_force_refresh"
SAVE_TO_GCS_VARIABLE = "media_analysis_save_to_gcs"
MAX_ACTIVE_TASKS_VARIABLE = "media_analysis_max_active_tasks"
DEFAULT_MAX_ACTIVE_TASKS = 8
DEFAULT_MAX_FRAMES = 20
POSTGRES_CONN_ID = "merino_analytics"


def _max_active_tasks() -> int:
    raw = variable_get(MAX_ACTIVE_TASKS_VARIABLE, str(DEFAULT_MAX_ACTIVE_TASKS)).strip()
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_MAX_ACTIVE_TASKS
    return max(1, value)


@dag(
    dag_id=DAG_ID,
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 1, 1, 0, 0, tz=REPORT_TIMEZONE),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=_max_active_tasks(),
    tags=["meta", "creative", "media-analysis"],
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    doc_md=__doc__,
)
def meta_creative_media_analysis():
    config_source = _campaign_config_for_display()
    config_log = _config_log_payload(config_source)
    dag_params = _dag_run_params()

    @task
    def log_campaign_config_source(source: dict[str, Any]) -> None:
        print(f"{DAG_ID}: config snapshot: {source.get('snapshot_uri') or '<none>'}")
        print(f"{DAG_ID}: config snapshot link: {source.get('snapshot_link') or '<none>'}")
        if source.get("error"):
            print(f"{DAG_ID}: campaign config unavailable during DAG parse: {source['error']}")
        else:
            print(
                f"{DAG_ID}: {source['account_count']} accounts, "
                f"{source.get('campaign_count', 0)} campaigns, "
                f"{source['adset_count']} adsets, "
                f"{source.get('ad_count', 0)} ads for media analysis"
            )

    @task
    def no_campaigns_from_campaign_config(source: dict[str, Any]) -> None:
        if source.get("error"):
            raise RuntimeError(source["error"])
        print(f"{DAG_ID}: no creative media analysis tasks were created")

    @task
    def download_ad_creative(ad: dict[str, Any], run_config: dict[str, Any]) -> dict[str, Any]:
        ad_id = str(ad["id"])
        base_url = media_analysis_base_url()
        ad_config = media_analysis_config_for_ad(
            ad_id,
            run_config.get("media_analysis_config_by_ad", {}),
        )
        is_frame_config = str(ad_config.get("image_type") or "frames") == "frames"
        frame_sample_end_sec = ad_config.get("frame_sample_end_sec") if is_frame_config else None
        frame_sample_interval_sec = ad_config.get("frame_sample_interval_sec") if is_frame_config else None
        payload = download_ad_creative_assets(
            ad_id,
            meta_token=meta_access_token(),
            gateway_token=mcp_gateway_token(),
            base_url=base_url,
            get_video_frame_in_sec=(
                int(float(frame_sample_end_sec))
                if frame_sample_end_sec is not None
                else run_config["get_video_frame_in_sec"]
            ),
            split_frame_by_sec=(
                float(frame_sample_interval_sec)
                if frame_sample_interval_sec is not None
                else run_config["split_frame_by_sec"]
            ),
            force_refresh=run_config["download_force_refresh"],
            bucket_location=run_config["bucket_location"],
            save_to_gcs=run_config["save_to_gcs"],
        )
        cache_hits = payload.get("cache_hits") if isinstance(payload.get("cache_hits"), list) else []
        video_ids = payload.get("video_ids") if isinstance(payload.get("video_ids"), list) else []
        print(
            f"{DAG_ID}: downloaded ad_id={ad_id} videos={len(video_ids)} "
            f"cache_hits={cache_hits} config={ad_config or '{}'}"
        )
        return {
            "ad_id": ad_id,
            "download": payload,
            "video_ids": [str(v) for v in video_ids],
            "cache_hits": [str(v) for v in cache_hits],
        }

    @task
    def analyze_ad_creative(
        download_result: dict[str, Any], run_config: dict[str, Any]
    ) -> dict[str, Any]:
        from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-not-found]
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        ad_id = str(download_result.get("ad_id") or "")
        ad_config = media_analysis_config_for_ad(
            ad_id,
            run_config.get("media_analysis_config_by_ad", {}),
        )
        download_payload = download_result.get("download")
        if not isinstance(download_payload, dict):
            raise RuntimeError(f"download_ad_creative missing download payload for ad_id={ad_id}")

        campaign_id = str(download_payload.get("campaign_id") or "")
        adset_id = str(download_payload.get("adset_id") or "")
        partition_datetime = report_partition_datetime(
            resolve_logical_date_from_context(get_current_context())
        ).isoformat()
        targets = analysis_targets_from_download(download_payload)
        if not targets:
            video_ids = download_payload.get("video_ids") if isinstance(download_payload.get("video_ids"), list) else []
            warnings = download_payload.get("warnings") if isinstance(download_payload.get("warnings"), list) else []
            print(
                f"{DAG_ID}: skipping analysis for ad_id={ad_id}: "
                f"no analyzable videos (video_ids={video_ids}, warnings={warnings})"
            )
            return {
                "ad_id": ad_id,
                "skipped": True,
                "reason": "no_analyzable_videos",
                "video_ids": [],
                "analysis_from_cache": [],
                "results": [],
                "errors": [],
            }

        meta_token = meta_access_token()
        gateway = mcp_gateway_token()
        base_url = media_analysis_base_url()
        results: list[dict[str, Any]] = []
        for target in targets:
            video_id = str(target["video_id"])
            storage = target["storage"]
            if not isinstance(storage, dict):
                continue
            analysis = creative_media_analysis(
                storage,
                ad_id=ad_id,
                video_id=video_id,
                meta_token=meta_token,
                gateway_token=gateway,
                base_url=base_url,
                force_refresh=run_config["analysis_force_refresh"],
                max_frames=run_config["max_frames"],
                audio_analysis=run_config["audio_analysis"],
                config=ad_config,
            )
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = hook.get_conn()
            try:
                snapshot_id = upsert_creative_media_analysis(
                    conn,
                    campaign_id=campaign_id,
                    adset_id=adset_id,
                    ad_id=ad_id,
                    video_id=video_id,
                    partition_datetime=partition_datetime,
                    analysis=analysis,
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

            from_cache = bool(analysis.get("from_cache"))
            print(
                f"{DAG_ID}: analyzed ad_id={ad_id} video_id={video_id} "
                f"from_cache={from_cache} snapshot_id={snapshot_id} config={ad_config or '{}'}"
            )
            results.append(
                {
                    "video_id": video_id,
                    "from_cache": from_cache,
                    "redis_key": analysis.get("redis_key"),
                    "snapshot_id": snapshot_id,
                }
            )

        return {
            "ad_id": ad_id,
            "video_ids": [row["video_id"] for row in results],
            "analysis_from_cache": [row["video_id"] for row in results if row["from_cache"]],
            "results": results,
            "errors": [],
        }

    wait_for_campaign_config = ExternalTaskSensor(
        task_id="wait_for_facebook_campaign_config_update",
        external_dag_id=CAMPAIGN_CONFIG_DAG_ID,
        external_task_id=None,
        execution_date_fn=campaign_config_logical_date,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=3 * 60 * 60,
    )
    wait_for_object_property = ExternalTaskSensor(
        task_id="wait_for_meta_object_property_sync",
        external_dag_id="meta_object_property_sync",
        external_task_id="sync_object_properties",
        execution_date_fn=campaign_config_logical_date,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=3 * 60 * 60,
    )
    config_task = log_campaign_config_source(config_log)
    wait_for_campaign_config >> wait_for_object_property >> config_task

    accounts = config_source.get("accounts", [])
    if not accounts:
        config_task >> no_campaigns_from_campaign_config(config_log)
        return

    for account in accounts:
        account_id = _airflow_id(account["id"])
        with TaskGroup(group_id=f"account_{account_id}") as account_group:
            for campaign in account.get("campaigns", []):
                campaign_id = _airflow_id(campaign["id"])
                with TaskGroup(group_id=f"campaign_{campaign_id}") as campaign_group:
                    for adset in campaign.get("adsets", []):
                        adset_id = _airflow_id(adset["id"])
                        ads = config_source.get("ads_by_adset", {}).get(str(adset.get("id")), [])
                        if not ads:
                            continue
                        with TaskGroup(group_id=f"adset_{adset_id}") as adset_group:
                            for ad in ads:
                                ad_task_id = _airflow_id(str(ad["id"]))
                                downloaded = download_ad_creative.override(
                                    task_id=f"download_ad_{ad_task_id}"
                                )(ad, dag_params)
                                analyzed = analyze_ad_creative.override(
                                    task_id=f"analyze_ad_{ad_task_id}"
                                )(downloaded, dag_params)
                                downloaded >> analyzed
                        config_task >> adset_group
                config_task >> campaign_group
        config_task >> account_group


def _dag_run_params() -> dict[str, Any]:
    return {
        "get_video_frame_in_sec": int(
            variable_get(SAMPLE_SEC_VARIABLE, os.environ.get(SAMPLE_SEC_ENV, "3"))
        ),
        "split_frame_by_sec": float(
            variable_get(FRAME_INTERVAL_VARIABLE, os.environ.get(FRAME_INTERVAL_ENV, "1"))
        ),
        "download_force_refresh": _bool_variable(DOWNLOAD_FORCE_REFRESH_VARIABLE, False),
        "analysis_force_refresh": _bool_variable(ANALYSIS_FORCE_REFRESH_VARIABLE, False),
        "save_to_gcs": _bool_variable(SAVE_TO_GCS_VARIABLE, True),
        "bucket_location": "meta_analysis",
        "max_frames": DEFAULT_MAX_FRAMES,
        "audio_analysis": True,
        "media_analysis_config_by_ad": media_analysis_config_by_ad(),
    }


def _bool_variable(name: str, default: bool) -> bool:
    raw = variable_get(name, "true" if default else "false").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _campaign_config_for_display() -> dict[str, Any]:
    source: dict[str, Any] = {
        "snapshot_uri": "",
        "snapshot_link": "",
        "accounts": [],
        "ads_by_adset": {},
        "account_count": 0,
        "campaign_count": 0,
        "adset_count": 0,
        "ad_count": 0,
        "error": "",
    }
    try:
        import google.auth  # type: ignore[import-not-found]
        from google.cloud import storage  # type: ignore[import-not-found]

        credentials, _project_id = google.auth.default()
        storage_client = storage.Client(credentials=credentials)
        pointer_uri, pointer = read_latest_snapshot_pointer(storage_client, CONFIG_GCS_PREFIX)
        snapshot_uri = str(pointer["final_output"])
        snapshot = read_json_from_gcs(storage_client, snapshot_uri)
        lookup_window_days = int(
            variable_get(
                LOOKUP_WINDOW_VARIABLE_NAME,
                os.environ.get(LOOKUP_WINDOW_ENV, str(DEFAULT_TRAFFIC_LOOKUP_WINDOW_DAYS)),
            )
        )
        active_accounts = variable_get(
            ACTIVE_ACCOUNTS_VARIABLE_NAME,
            os.environ.get(ACTIVE_ACCOUNTS_ENV, ""),
        )
        cutoff = datetime.now(timezone.utc) - timedelta(days=lookup_window_days)
        accounts = traffic_accounts_from_config(
            snapshot,
            active_accounts_value=active_accounts,
            lookup_window_days=lookup_window_days,
        )
        ads_by_adset: dict[str, list[dict[str, Any]]] = {}
        ad_count = 0
        for account in accounts:
            for campaign in account.get("campaigns", []):
                for adset in campaign.get("adsets", []):
                    adset_key = str(adset.get("id") or "")
                    if not adset_key:
                        continue
                    ads = media_analysis_ads_from_adset(adset, cutoff=cutoff)
                    if ads:
                        ads_by_adset[adset_key] = ads
                        ad_count += len(ads)

        source.update(
            {
                "snapshot_uri": snapshot_uri,
                "snapshot_link": gcs_console_link(snapshot_uri),
                "accounts": accounts,
                "ads_by_adset": ads_by_adset,
                "account_count": len(accounts),
                "campaign_count": _campaign_count(accounts),
                "adset_count": _adset_count(accounts),
                "ad_count": ad_count,
            }
        )
    except Exception as exc:
        source["error"] = str(exc)
    return source


def _config_log_payload(source: dict[str, Any]) -> dict[str, Any]:
    return {
        "snapshot_uri": source.get("snapshot_uri", ""),
        "snapshot_link": source.get("snapshot_link", ""),
        "account_count": source.get("account_count", 0),
        "campaign_count": source.get("campaign_count", 0),
        "adset_count": source.get("adset_count", 0),
        "ad_count": source.get("ad_count", 0),
        "error": source.get("error", ""),
    }


def _campaign_count(accounts: list[dict[str, Any]]) -> int:
    return sum(len(account.get("campaigns", [])) for account in accounts)


def _adset_count(accounts: list[dict[str, Any]]) -> int:
    return sum(
        len(campaign.get("adsets", []))
        for account in accounts
        for campaign in account.get("campaigns", [])
    )


def _airflow_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_") or "unknown"


meta_creative_media_analysis()
