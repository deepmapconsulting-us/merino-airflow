"""Shared GCS snapshot helpers for Meta Airflow DAGs."""

from __future__ import annotations

import json
from datetime import datetime, timezone

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
    return f"gs://{SNAPSHOT_BUCKET}/{object_name}"


def publish_latest_pointer(
    storage_client,
    *,
    prefix: str,
    dag_id: str,
    snapshot_uri: str,
    run_date: str,
    run_datetime: str,
    variable_name: str,
) -> str:
    pointer = {
        "final_output": snapshot_uri,
        "last_success_date": run_date,
        "last_success_datetime": run_datetime,
    }
    payload = json.dumps(pointer, separators=(",", ":"), sort_keys=True)
    latest_object = latest_object_name(prefix)
    storage_client.bucket(SNAPSHOT_BUCKET).blob(latest_object).upload_from_string(
        payload,
        content_type="application/json",
    )
    latest_uri = f"gs://{SNAPSHOT_BUCKET}/{latest_object}"

    try:
        Variable.set(variable_name, payload)
    except Exception as exc:
        print(
            f"{dag_id}: skipped Variable.set({variable_name!r}) "
            f"because GSM Variable write failed: {exc}"
        )

    return latest_uri
