"""Shared GCS snapshot helpers for Meta Airflow DAGs."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from urllib.parse import quote

from airflow.sdk import Variable  # type: ignore[import-not-found]

META_ACCESS_TOKEN_VARIABLE = "meta_access_token"
SNAPSHOT_BUCKET = "airflow-run-us-west2"


def variable_get(key: str, fallback: str = "") -> str:
    try:
        return str(Variable.get(key))
    except Exception:
        return fallback


def meta_access_token() -> str:
    import os

    token = variable_get(META_ACCESS_TOKEN_VARIABLE).strip()
    if not token:
        token = os.environ.get("META_ACCESS_TOKEN", "").strip()
    if not token:
        raise RuntimeError(
            f"Meta access token missing. Set Airflow Variable {META_ACCESS_TOKEN_VARIABLE!r} "
            f"(GSM secret airflow-variables-{META_ACCESS_TOKEN_VARIABLE}) "
            "or env META_ACCESS_TOKEN."
        )
    return token


def logical_date_utc():
    try:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        return get_current_context()["logical_date"]
    except Exception:
        try:
            from airflow.operators.python import get_current_context  # type: ignore[import-not-found]

            return get_current_context()["logical_date"]
        except Exception:
            return datetime.now(timezone.utc)


def run_partition() -> tuple[str, str]:
    logical_date = logical_date_utc()
    if hasattr(logical_date, "astimezone"):
        logical_date = logical_date.astimezone(timezone.utc)
    run_date = logical_date.strftime("%Y-%m-%d")
    run_datetime = logical_date.strftime("%Y%m%dT%H%M%SZ")
    return run_date, run_datetime


def metric_date() -> str:
    return run_partition()[0]


def snapshot_object_name(prefix: str, run_date: str, run_datetime: str) -> str:
    return f"{prefix}/{run_date}/{run_datetime}/snapshot.json"


def latest_object_name(prefix: str) -> str:
    return f"{prefix}/latest_success.json"


def gcs_uri(bucket_name: str, object_name: str) -> str:
    return f"gs://{bucket_name}/{object_name}"


def gcs_console_link(uri: str) -> str:
    bucket_name, object_name = parse_gcs_uri(uri)
    return (
        "https://console.cloud.google.com/storage/browser/_details/"
        f"{bucket_name}/{quote(object_name)}"
    )


def parse_gcs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"Expected gs:// URI, got {uri!r}")
    bucket_name, _, object_name = uri[5:].partition("/")
    if not bucket_name or not object_name:
        raise ValueError(f"Expected gs://bucket/object URI, got {uri!r}")
    return bucket_name, object_name


def read_json_from_gcs(storage_client, uri: str) -> dict:
    bucket_name, object_name = parse_gcs_uri(uri)
    payload = storage_client.bucket(bucket_name).blob(object_name).download_as_text()
    return json.loads(payload)


def read_latest_snapshot_pointer(storage_client, prefix: str) -> tuple[str, dict]:
    pointer_uri = gcs_uri(SNAPSHOT_BUCKET, latest_object_name(prefix))
    return pointer_uri, read_json_from_gcs(storage_client, pointer_uri)


def write_snapshot_to_gcs(
    storage_client,
    prefix: str,
    snapshot: dict,
    run_date: str,
    run_datetime: str,
) -> str:
    object_name = snapshot_object_name(prefix, run_date, run_datetime)
    payload = json.dumps(snapshot, separators=(",", ":"), sort_keys=True)
    storage_client.bucket(SNAPSHOT_BUCKET).blob(object_name).upload_from_string(
        payload,
        content_type="application/json",
    )
    return gcs_uri(SNAPSHOT_BUCKET, object_name)


def publish_latest_pointer(
    storage_client,
    *,
    prefix: str,
    dag_id: str,
    snapshot_uri: str,
    run_date: str,
    run_datetime: str,
    variable_name: str,
    variable_snapshot: dict | None = None,
) -> str:
    pointer = {
        "final_output": snapshot_uri,
        "last_success_date": run_date,
        "last_success_datetime": run_datetime,
    }
    pointer_payload = json.dumps(pointer, separators=(",", ":"), sort_keys=True)
    latest_object = latest_object_name(prefix)
    storage_client.bucket(SNAPSHOT_BUCKET).blob(latest_object).upload_from_string(
        pointer_payload,
        content_type="application/json",
    )
    latest_uri = gcs_uri(SNAPSHOT_BUCKET, latest_object)

    if variable_snapshot is not None:
        variable_payload = json.dumps(variable_snapshot, separators=(",", ":"), sort_keys=True)
    else:
        variable_payload = pointer_payload

    try:
        Variable.set(variable_name, variable_payload)
    except Exception as exc:
        print(
            f"{dag_id}: skipped Variable.set({variable_name!r}) "
            f"because GSM Variable write failed: {exc}"
        )

    return latest_uri
