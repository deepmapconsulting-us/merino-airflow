"""Sync hourly Meta ad traffic (insights) to GCS and publish the latest pointer.

Meta access token is read from Airflow Variable `meta_access_token` (GSM:
`airflow-variables-meta_access_token`), with fallback to env `META_ACCESS_TOKEN`.
Snapshots are written to GCS at
`gs://airflow-run-us-west2/meta_traffic_hourly/<date>/<datetime>/snapshot.json`.
Latest pointer: `gs://airflow-run-us-west2/meta_traffic_hourly/latest_success.json`
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
    metric_date,
    publish_latest_pointer,
    run_partition,
    write_snapshot_to_gcs,
)

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
if MODULE_PATH.exists():
    sys.path.insert(0, str(MODULE_PATH))

DAG_ID = "meta_traffic_hourly"
GCS_PREFIX = "meta_traffic_hourly"
SNAPSHOT_VARIABLE_NAME = "META_TRAFFIC_HOURLY_SNAPSHOT"
DEFAULT_META_PAGE_LIMIT = 500


@dag(
    dag_id=DAG_ID,
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["meta", "traffic", "hourly"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    doc_md=__doc__,
)
def meta_traffic_hourly():
    @task
    def sync_traffic_snapshot() -> None:
        access_token = meta_access_token()
        import google.auth  # type: ignore[import-not-found]
        from google.cloud import storage  # type: ignore[import-not-found]
        from merino_meta_jobs.traffic import traffic_hourly_snapshot  # type: ignore[import-not-found]

        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        snapshot = traffic_hourly_snapshot(
            access_token,
            metric_date(),
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
        )
        insight_rows = sum(
            len(account.get("insights", []))
            for account in snapshot.get("accounts", {}).values()
        )
        print(
            f"{DAG_ID}: synced {insight_rows} ad insight rows for {snapshot['metric_date']} "
            f"to {snapshot_uri}; latest pointer at {latest_pointer_uri}"
        )

    sync_traffic_snapshot()


# meta_traffic_hourly()
