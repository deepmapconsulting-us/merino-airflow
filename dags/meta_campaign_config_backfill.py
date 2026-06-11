"""Scan historical facebook_campaign_config_update GCS snapshots for backfill planning.

Daily schedule enables Airflow UI backfill (one run per logical date). Keep this DAG
paused for normal ops; trigger manually or backfill when needed.

- **Backfill** (UI): one inventory report per day in the selected date range.
- **Single Run** with no conf: scans every snapshot (full union inventory).
- **Single Run** conf ``{"start_date": "...", "end_date": "..."}``: scans that range only.

Dimension tables ``marketing.meta_campaign`` / ``meta_adset`` / ``meta_ad`` hold
only the latest object properties from ``meta_object_property_sync``. They do
not store historical status timelines; use this scan output when planning
metric backfills.
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag, get_current_context, task  # type: ignore[import-not-found]

from meta_gcs import REPORT_TIMEZONE, read_json_from_gcs, run_partition, write_snapshot_to_gcs

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
if MODULE_PATH.exists():
    sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs.campaign_config_backfill import (  # noqa: E402  # type: ignore[import-not-found]
    DEFAULT_CONFIG_GCS_PREFIX,
    scan_config_snapshots,
)

DAG_ID = "meta_campaign_config_backfill"
OUTPUT_GCS_PREFIX = "meta_campaign_config_backfill"
MAX_SNAPSHOTS_ENV = "CAMPAIGN_CONFIG_BACKFILL_MAX_SNAPSHOTS"


def _scan_date_range(context: dict[str, Any]) -> tuple[str | None, str | None]:
    dag_run = context.get("dag_run")
    conf = dict(getattr(dag_run, "conf", None) or {})

    if conf.get("scan_all"):
        return None, None

    start_date = str(conf.get("start_date") or "").strip() or None
    end_date = str(conf.get("end_date") or "").strip() or None
    if start_date or end_date:
        return start_date or end_date, end_date or start_date

    run_type = str(getattr(dag_run, "run_type", "") or "")
    if run_type in {"backfill", "scheduled"}:
        logical_date = context.get("logical_date") or getattr(dag_run, "logical_date", None)
        if logical_date is not None:
            day = pendulum.instance(logical_date).in_timezone(REPORT_TIMEZONE).format("YYYY-MM-DD")
            return day, day

    return None, None


@dag(
    dag_id=DAG_ID,
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=REPORT_TIMEZONE),
    catchup=False,
    max_active_runs=1,
    tags=["meta", "config", "backfill"],
    default_args={
        "owner": "data-platform",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    doc_md=__doc__,
)
def meta_campaign_config_backfill():
    @task
    def scan_historical_config_snapshots() -> dict[str, Any]:
        import google.auth  # type: ignore[import-not-found]
        from google.cloud import storage  # type: ignore[import-not-found]

        context = get_current_context()
        start_date, end_date = _scan_date_range(context)

        max_snapshots_raw = os.environ.get(MAX_SNAPSHOTS_ENV, "").strip()
        max_snapshots = int(max_snapshots_raw) if max_snapshots_raw else None

        credentials, _project_id = google.auth.default()
        storage_client = storage.Client(credentials=credentials)
        report = scan_config_snapshots(
            storage_client,
            read_json_from_gcs,
            prefix=DEFAULT_CONFIG_GCS_PREFIX,
            max_snapshots=max_snapshots,
            start_date=start_date,
            end_date=end_date,
        )

        run_date, run_datetime = run_partition()
        output_uri = write_snapshot_to_gcs(
            storage_client,
            OUTPUT_GCS_PREFIX,
            report,
            run_date,
            run_datetime,
        )

        range_label = f"{start_date}..{end_date}" if start_date else "all"
        print(
            f"{DAG_ID}: scanned {report['snapshot_count']} config snapshots ({range_label}); "
            f"unique campaigns={report['unique_campaigns']} "
            f"adsets={report['unique_adsets']} ads={report['unique_ads']}"
        )
        if report.get("snapshot_summaries"):
            first = report["snapshot_summaries"][0]
            last = report["snapshot_summaries"][-1]
            print(
                f"{DAG_ID}: first snapshot campaigns={first['campaign_count']} "
                f"adsets={first['adset_count']} at {first['observed_at']}"
            )
            print(
                f"{DAG_ID}: last snapshot campaigns={last['campaign_count']} "
                f"adsets={last['adset_count']} at {last['observed_at']}"
            )
        print(f"{DAG_ID}: inventory report written to {output_uri}")
        report["output_uri"] = output_uri
        return {
            "output_uri": output_uri,
            "snapshot_count": report["snapshot_count"],
            "unique_campaigns": report["unique_campaigns"],
            "unique_adsets": report["unique_adsets"],
            "unique_ads": report["unique_ads"],
            "scan_start_date": start_date,
            "scan_end_date": end_date,
        }

    scan_historical_config_snapshots()


meta_campaign_config_backfill()
