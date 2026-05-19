"""Sync current Facebook (Meta) campaign/ad config to GCS and publish the latest pointer.

Meta access token is read from Airflow Variable `meta_access_token` (GSM:
`airflow-variables-meta_access_token`), with fallback to env `META_ACCESS_TOKEN`.
Each campaign and adset in the snapshot includes `created_at` and `updated_at`
(from Meta `created_time` / `updated_time`). Snapshots are written to GCS at
`gs://airflow-run-us-west2/facebook_campaign_config_update/<date>/<datetime>/snapshot.json`.
Latest pointer: `gs://airflow-run-us-west2/facebook_campaign_config_update/latest_success.json`
(optional Variable mirror when GSM allows).
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag, task  # type: ignore[import-not-found]

from meta_gcs import (
    meta_access_token,
    publish_latest_pointer,
    run_partition,
    write_snapshot_to_gcs,
)

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
if MODULE_PATH.exists():
    sys.path.insert(0, str(MODULE_PATH))

DAG_ID = "facebook_campaign_config_update"
GCS_PREFIX = "facebook_campaign_config_update"
SNAPSHOT_VARIABLE_NAME = "META_CAMPAIGN_CONFIG_SNAPSHOT"
DEFAULT_META_PAGE_LIMIT = 500


@dag(
    dag_id=DAG_ID,
    schedule=timedelta(hours=4),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
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
        snapshot = current_ad_object_snapshot(access_token, page_limit=page_limit)
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
        )
        print(
            f"{DAG_ID}: synced "
            f"{len(snapshot['accounts'])} Meta ad account config snapshots to {snapshot_uri}; "
            f"latest pointer at {latest_pointer_uri}"
        )

    sync_campaign_config()


facebook_campaign_config_update()
