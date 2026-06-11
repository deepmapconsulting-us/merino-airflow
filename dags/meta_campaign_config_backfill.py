"""Scan historical facebook_campaign_config_update GCS snapshots for backfill planning.

Manual DAG. Reads every ``snapshot.json`` under the campaign config GCS prefix,
prints per-snapshot counts, and writes a union inventory (first/last seen,
active observation windows) to GCS.

Dimension tables ``marketing.meta_campaign`` / ``meta_adset`` / ``meta_ad`` hold
only the latest object properties from ``meta_object_property_sync``. They do
not store historical status timelines; use this scan output when planning
metric backfills.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag, task  # type: ignore[import-not-found]

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


@dag(
    dag_id=DAG_ID,
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz=REPORT_TIMEZONE),
    catchup=False,
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

        max_snapshots_raw = os.environ.get(MAX_SNAPSHOTS_ENV, "").strip()
        max_snapshots = int(max_snapshots_raw) if max_snapshots_raw else None

        credentials, _project_id = google.auth.default()
        storage_client = storage.Client(credentials=credentials)
        report = scan_config_snapshots(
            storage_client,
            read_json_from_gcs,
            prefix=DEFAULT_CONFIG_GCS_PREFIX,
            max_snapshots=max_snapshots,
        )

        run_date, run_datetime = run_partition()
        output_uri = write_snapshot_to_gcs(
            storage_client,
            OUTPUT_GCS_PREFIX,
            report,
            run_date,
            run_datetime,
        )

        print(
            f"{DAG_ID}: scanned {report['snapshot_count']} config snapshots; "
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
        }

    scan_historical_config_snapshots()


meta_campaign_config_backfill()
