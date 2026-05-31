"""Shared GCS snapshot helpers for Meta Airflow DAGs."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import Variable  # type: ignore[import-not-found]

META_ACCESS_TOKEN_VARIABLE = "meta_access_token"
SNAPSHOT_BUCKET = "airflow-run-us-west2"
# Meta ad accounts report insights by calendar day in account TZ; default matches Merino US accounts.
REPORT_TIMEZONE = os.environ.get("META_REPORT_TIMEZONE", "America/Los_Angeles")
REPORT_PARTITION_HOURS = 2
REPORT_SCHEDULE_DELAY_MINUTES = 10


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


def logical_date():
    """Airflow logical date for the current run, or now when called outside a task."""
    try:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        return get_current_context()["logical_date"]
    except Exception:
        try:
            from airflow.operators.python import get_current_context  # type: ignore[import-not-found]

            return get_current_context()["logical_date"]
        except Exception:
            return datetime.now(timezone.utc)


def _report_timezone():
    return pendulum.timezone(REPORT_TIMEZONE)


def report_datetime(value: Any | None = None) -> pendulum.DateTime:
    """Convert a run timestamp to REPORT_TIMEZONE (default America/Los_Angeles)."""
    if value is None:
        value = logical_date()
    if hasattr(value, "astimezone"):
        return pendulum.instance(value).in_timezone(_report_timezone())
    return pendulum.now(_report_timezone())


def report_partition_datetime(value: Any | None = None) -> pendulum.DateTime:
    """Return the report-time 2-hour bucket for a run timestamp."""
    local = report_datetime(value)
    partition_hour = (local.hour // REPORT_PARTITION_HOURS) * REPORT_PARTITION_HOURS
    return local.set(hour=partition_hour, minute=0, second=0, microsecond=0)


def report_schedule_datetime(value: Any | None = None) -> pendulum.DateTime:
    """Return the scheduled run timestamp for a report bucket."""
    return report_partition_datetime(value).add(minutes=REPORT_SCHEDULE_DELAY_MINUTES)


def run_partition() -> tuple[str, str]:
    """GCS path partition keys using the Meta 2-hour report bucket."""
    local = report_partition_datetime()
    run_date = local.format("YYYY-MM-DD")
    run_datetime = local.format("YYYYMMDDTHHmmssZZ")
    return run_date, run_datetime


def metric_date() -> str:
    """Calendar day sent to Meta insights time_range (account-local reporting day)."""
    local = report_partition_datetime()
    if local.hour == 0:
        return local.subtract(days=1).format("YYYY-MM-DD")
    return local.format("YYYY-MM-DD")


def partition_hour(value: Any | None = None) -> str:
    """Two-hour bucket for snapshot/hourly tables in REPORT_TIMEZONE."""
    return report_partition_datetime(value).isoformat()


def resolve_logical_date_from_context(
    context: dict[str, Any] | None = None,
    run_logical_date: Any | None = None,
) -> Any:
    """Best-effort Airflow logical date from sensor/task context."""
    if run_logical_date is not None:
        return run_logical_date
    if not context:
        return logical_date()

    if context.get("logical_date"):
        return context["logical_date"]
    for key in ("data_interval_start", "execution_date", "ts"):
        if context.get(key):
            return context[key]
    for source in (context.get("dag_run"), context.get("task_instance"), context.get("ti")):
        if source is None:
            continue
        for attr in ("logical_date", "data_interval_start", "execution_date", "run_after", "start_date"):
            value = getattr(source, attr, None)
            if value:
                return value
    run_id = str(context.get("run_id") or getattr(context.get("dag_run"), "run_id", ""))
    if "__" in run_id:
        try:
            return pendulum.parse(run_id.split("__", 1)[1])
        except Exception:
            pass
    raise KeyError(f"No logical date found in Airflow context keys: {sorted(context)}")


def campaign_config_logical_date(logical_date: Any | None = None, **context: Any) -> pendulum.DateTime:
    """Map a downstream DAG run to the matching facebook_campaign_config_update logical date."""
    return report_partition_datetime(
        resolve_logical_date_from_context(context, run_logical_date=logical_date)
    )


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
    variable_name: str | None = None,
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

    if variable_name:
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
