"""Sync current Facebook (Meta) ad objects to GCS and publish the latest pointer.

Meta access token is read from Airflow Variable `meta_access_token` (GSM:
`airflow-variables-meta_access_token`), with fallback to env `META_ACCESS_TOKEN`.
Latest snapshot pointer is written to GCS at
`gs://airflow-run-us-west2/facebook_traffic_ingestion/latest_success.json`
(and mirrored to Variable `META_CURRENT_AD_OBJECT_SNAPSHOT` when GSM allows).
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import Variable, dag, task  # type: ignore[import-not-found]

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
if MODULE_PATH.exists():
    sys.path.insert(0, str(MODULE_PATH))

META_ACCESS_TOKEN_VARIABLE = "meta_access_token"
SNAPSHOT_VARIABLE_NAME = "META_CURRENT_AD_OBJECT_SNAPSHOT"
SNAPSHOT_LATEST_OBJECT = "facebook_traffic_ingestion/latest_success.json"
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
        access_token = _meta_access_token()
        import google.auth  # type: ignore[import-not-found]
        from google.cloud import storage  # type: ignore[import-not-found]
        from merino_meta_jobs.account_snapshot import current_ad_object_snapshot  # type: ignore[import-not-found]

        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        snapshot = current_ad_object_snapshot(access_token, page_limit=page_limit)
        run_datetime = _run_datetime()
        credentials, _project_id = google.auth.default()
        storage_client = storage.Client(credentials=credentials)
        snapshot_uri = _write_snapshot_to_gcs(snapshot, run_datetime, storage_client)
        latest_pointer_uri = _publish_latest_pointer(
            storage_client,
            snapshot_uri,
            run_datetime,
        )
        print(
            "facebook_traffic_ingestion: synced "
            f"{len(snapshot['accounts'])} Meta ad account snapshots to {snapshot_uri}; "
            f"latest pointer at {latest_pointer_uri}"
        )

    sync_current_ad_objects()


facebook_traffic_ingestion()


def _variable_get(key: str, fallback: str = "") -> str:
    """Read a Variable without Airflow 2 `default_var` (not valid in Airflow 3 SDK)."""
    try:
        return str(Variable.get(key))
    except Exception:
        return fallback


def _meta_access_token() -> str:
    token = _variable_get(META_ACCESS_TOKEN_VARIABLE).strip()
    if not token:
        token = os.environ.get("META_ACCESS_TOKEN", "").strip()
    if not token:
        raise RuntimeError(
            f"Meta access token missing. Set Airflow Variable {META_ACCESS_TOKEN_VARIABLE!r} "
            f"(GSM secret airflow-variables-{META_ACCESS_TOKEN_VARIABLE}) "
            "or env META_ACCESS_TOKEN."
        )
    return token


def _run_datetime() -> str:
    try:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        logical_date = get_current_context()["logical_date"]
    except Exception:
        try:
            from airflow.operators.python import get_current_context  # type: ignore[import-not-found]

            logical_date = get_current_context()["logical_date"]
        except Exception:
            logical_date = datetime.now(timezone.utc)

    if hasattr(logical_date, "strftime"):
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


def _publish_latest_pointer(storage_client, snapshot_uri: str, run_datetime: str) -> str:
    """Write latest-success pointer to GCS; optionally mirror to Airflow Variable for UI."""
    pointer = {
        "final_output": snapshot_uri,
        "last_success_datetime": run_datetime,
    }
    payload = json.dumps(pointer, separators=(",", ":"), sort_keys=True)
    storage_client.bucket(SNAPSHOT_BUCKET).blob(SNAPSHOT_LATEST_OBJECT).upload_from_string(
        payload,
        content_type="application/json",
    )
    latest_uri = f"gs://{SNAPSHOT_BUCKET}/{SNAPSHOT_LATEST_OBJECT}"

    try:
        Variable.set(SNAPSHOT_VARIABLE_NAME, payload)
    except Exception as exc:
        print(
            f"facebook_traffic_ingestion: skipped Variable.set({SNAPSHOT_VARIABLE_NAME!r}) "
            f"because GSM Variable write failed: {exc}"
        )

    return latest_uri
