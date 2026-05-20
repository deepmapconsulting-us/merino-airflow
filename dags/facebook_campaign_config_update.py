"""Sync current Facebook (Meta) campaign/ad config to GCS and publish the latest pointer.

Meta access token is read from Airflow Variable `meta_access_token` (GSM:
`airflow-variables-meta_access_token`), with fallback to env `META_ACCESS_TOKEN`.
Each campaign, adset, and ad includes `created_at` and `updated_at` when Meta
returns `created_time` / `updated_time`. Snapshots are written to GCS at
`gs://airflow-run-us-west2/facebook_campaign_config_update/<date>/<4-hour-bucket>/snapshot.json`.
Latest pointer: `gs://airflow-run-us-west2/facebook_campaign_config_update/latest_success.json`.
Airflow Variable `META_CAMPAIGN_CONFIG_SNAPSHOT` mirrors the full snapshot JSON (not the pointer).
Airflow Variable `FACEBOOK_AD_ACCOUNT_TIMEZONE` caches account timezone names by ad account id.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import timedelta
from pathlib import Path

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import Variable, dag, task  # type: ignore[import-not-found]

from meta_gcs import (
    REPORT_TIMEZONE,
    meta_access_token,
    publish_latest_pointer,
    run_partition,
    variable_get,
    write_snapshot_to_gcs,
)

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
if MODULE_PATH.exists():
    sys.path.insert(0, str(MODULE_PATH))

DAG_ID = "facebook_campaign_config_update"
GCS_PREFIX = "facebook_campaign_config_update"
SNAPSHOT_VARIABLE_NAME = "META_CAMPAIGN_CONFIG_SNAPSHOT"
TIMEZONE_VARIABLE_NAME = "FACEBOOK_AD_ACCOUNT_TIMEZONE"
DEFAULT_META_PAGE_LIMIT = 500


def _account_timezone_cache() -> dict[str, str]:
    value = variable_get(TIMEZONE_VARIABLE_NAME).strip()
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        print(f"{DAG_ID}: ignoring invalid JSON in Airflow Variable {TIMEZONE_VARIABLE_NAME!r}")
        return {}
    if not isinstance(payload, dict):
        print(f"{DAG_ID}: ignoring non-dict Airflow Variable {TIMEZONE_VARIABLE_NAME!r}")
        return {}
    return {str(account_id): str(timezone_name) for account_id, timezone_name in payload.items() if timezone_name}


def _save_account_timezone_cache(account_timezone_by_id: dict[str, str]) -> None:
    try:
        Variable.set(
            TIMEZONE_VARIABLE_NAME,
            json.dumps(account_timezone_by_id, separators=(",", ":"), sort_keys=True),
        )
    except Exception as exc:
        print(
            f"{DAG_ID}: skipped Variable.set({TIMEZONE_VARIABLE_NAME!r}) "
            f"because GSM Variable write failed: {exc}"
        )


@dag(
    dag_id=DAG_ID,
    schedule="0 0,4,8,12,16,20 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=REPORT_TIMEZONE),
    catchup=False,
    tags=["facebook", "campaign", "config", "meta"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    doc_md=__doc__,
)
def facebook_campaign_config_update():
    @task
    def sync_campaign_config() -> None:
        access_token = meta_access_token()
        import google.auth  # type: ignore[import-not-found]
        from google.cloud import storage  # type: ignore[import-not-found]
        from merino_meta_jobs.account_snapshot import current_ad_object_snapshot  # type: ignore[import-not-found]

        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        account_timezone_by_id = _account_timezone_cache()
        snapshot = current_ad_object_snapshot(
            access_token,
            account_timezone_by_id=account_timezone_by_id,
            page_limit=page_limit,
        )
        run_date, run_datetime = run_partition()
        credentials, _project_id = google.auth.default()
        storage_client = storage.Client(credentials=credentials)
        snapshot_uri = write_snapshot_to_gcs(
            storage_client,
            GCS_PREFIX,
            snapshot,
            run_date,
            run_datetime,
        )
        latest_pointer_uri = publish_latest_pointer(
            storage_client,
            prefix=GCS_PREFIX,
            dag_id=DAG_ID,
            snapshot_uri=snapshot_uri,
            run_date=run_date,
            run_datetime=run_datetime,
            variable_name=SNAPSHOT_VARIABLE_NAME,
            variable_snapshot=snapshot,
        )
        _save_account_timezone_cache(account_timezone_by_id)
        print(
            f"{DAG_ID}: synced "
            f"{len(snapshot['accounts'])} Meta ad account config snapshots to {snapshot_uri}; "
            f"latest pointer at {latest_pointer_uri}"
        )

    sync_campaign_config()


facebook_campaign_config_update()
