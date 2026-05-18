"""Sync current Facebook (Meta) ad objects to GCS and publish the latest pointer.

Credentials are injected from the Airflow secrets backend / Kubernetes secret as
environment variables. The snapshot payload intentionally excludes credentials.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pendulum  # type: ignore[import-not-found]
from airflow.decorators import dag, task  # type: ignore[import-not-found]

try:
    from airflow.sdk import Variable  # type: ignore[import-not-found]
except ImportError:  # Airflow 2 compatibility for local tooling.
    from airflow.models import Variable  # type: ignore[import-not-found,no-redef]

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
if MODULE_PATH.exists():
    sys.path.insert(0, str(MODULE_PATH))

SNAPSHOT_VARIABLE_NAME = "META_CURRENT_AD_OBJECT_SNAPSHOT"
SNAPSHOT_BUCKET = "airflow-run-us-west2"
DEFAULT_META_PAGE_LIMIT = 500


@dag(
    dag_id="facebook_traffic_ingestion",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["facebook", "traffic", "ingestion", "meta"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    doc_md=__doc__,
)
def facebook_traffic_ingestion():
    @task
    def sync_current_ad_objects() -> None:
        access_token = os.environ.get("META_ACCESS_TOKEN", "").strip()
        if not access_token:
            raise RuntimeError("META_ACCESS_TOKEN is required to sync Meta ad objects")

        from google.cloud import storage  # type: ignore[import-not-found]
        from merino_meta_jobs.account_snapshot import current_ad_object_snapshot  # type: ignore[import-not-found]

        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        snapshot = current_ad_object_snapshot(access_token, page_limit=page_limit)
        run_datetime = _run_datetime()
        snapshot_uri = _write_snapshot_to_gcs(snapshot, run_datetime, storage.Client())

        Variable.set(
            SNAPSHOT_VARIABLE_NAME,
            json.dumps(
                {
                    "final_output": snapshot_uri,
                    "last_success_datetime": run_datetime,
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        )
        print(
            "facebook_traffic_ingestion: synced "
            f"{len(snapshot['accounts'])} Meta ad account snapshots to {snapshot_uri}"
        )

    sync_current_ad_objects()


facebook_traffic_ingestion()


def _run_datetime() -> str:
    try:
        from airflow.operators.python import get_current_context  # type: ignore[import-not-found]

        logical_date = get_current_context()["logical_date"]
    except Exception:
        logical_date = datetime.now(timezone.utc)

    if hasattr(logical_date, "to_datetime_string"):
        return logical_date.strftime("%Y%m%dT%H%M%SZ")
    return logical_date.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_snapshot_to_gcs(snapshot: dict, run_datetime: str, storage_client) -> str:
    object_name = f"facebook_traffic_ingestion/{run_datetime}/snapshot.json"
    payload = json.dumps(snapshot, separators=(",", ":"), sort_keys=True)
    storage_client.bucket(SNAPSHOT_BUCKET).blob(object_name).upload_from_string(
        payload,
        content_type="application/json",
    )
    return f"gs://{SNAPSHOT_BUCKET}/{object_name}"
