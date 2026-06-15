"""Write provisional and finalized GA4 daily session analysis to Cloud SQL.

Every two hours, today and yesterday refresh from intraday tables when those
tables exist and the report date is not already finalized in Cloud SQL.

Finalized `events_YYYYMMDD` tables are loaded once per report date: the first
time the finalized BigQuery table exists and Cloud SQL still has provisional
rows (or no rows) for that date.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag, task  # type: ignore[import-not-found]

from meta_gcs import REPORT_TIMEZONE

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "ga4"
if MODULE_PATH.exists():
    sys.path.insert(0, str(MODULE_PATH))

from merino_ga4_jobs.daily_analysis import (  # noqa: E402  # type: ignore[import-not-found]
    PROJECT_ID,
    daily_analysis_query,
    ga4_source_table,
    landing_page_daily_analysis_query,
    merge_daily_analysis,
    merge_landing_page_daily_analysis,
    report_date_is_finalized,
)
from merino_ga4_jobs.daily_purchaser_behavior import (  # noqa: E402  # type: ignore[import-not-found]
    daily_purchaser_behavior_query,
    merge_daily_purchaser_behavior,
)

DAG_ID = "ga4_daily_analysis"
GCP_CONN_ID = "google_cloud_default"


def ga4_refresh_candidates(logical_date: Any) -> list[dict[str, Any]]:
    run_date = logical_date.date() if hasattr(logical_date, "date") else logical_date
    return [
        {
            "report_date": run_date,
            "source_table_type": "intraday",
        },
        {
            "report_date": run_date - timedelta(days=1),
            "source_table_type": "intraday",
        },
        {
            "report_date": run_date - timedelta(days=2),
            "source_table_type": "finalized",
        },
        {
            "report_date": run_date - timedelta(days=3),
            "source_table_type": "finalized",
        },
    ]


def ga4_refresh_skip_reason(
    *,
    report_date: date,
    source_table_type: str,
    source_table_exists: bool,
    already_finalized: bool,
) -> str | None:
    if already_finalized:
        return (
            f"report_date={report_date.isoformat()} already finalized in Cloud SQL; skipping"
        )

    if not source_table_exists:
        return (
            f"missing source_table for report_date={report_date.isoformat()} "
            f"source_table_type={source_table_type}; skipping"
        )

    return None


def bigquery_table_exists(client: Any, table_id: str) -> bool:
    try:
        client.get_table(table_id)
        return True
    except Exception as exc:
        if exc.__class__.__name__ == "NotFound":
            return False
        raise


@dag(
    dag_id=DAG_ID,
    schedule="0 */2 * * *",
    start_date=pendulum.datetime(2026, 6, 1, 7, 0, tz=REPORT_TIMEZONE),
    catchup=False,
    tags=["ga4", "bigquery", "daily-analysis"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    doc_md=__doc__,
)
def ga4_daily_analysis():
    @task
    def write_daily_analysis() -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook  # type: ignore[import-not-found]

        context = get_current_context()
        logical_date = context["logical_date"].in_timezone(REPORT_TIMEZONE)

        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
        client = hook.get_client(project_id=PROJECT_ID)

        results = []
        for candidate in ga4_refresh_candidates(logical_date):
            report_date: date = candidate["report_date"]
            source_table_type = candidate["source_table_type"]
            source_table = ga4_source_table(report_date, intraday=source_table_type == "intraday")
            already_finalized = report_date_is_finalized(report_date)
            skip_reason = ga4_refresh_skip_reason(
                report_date=report_date,
                source_table_type=source_table_type,
                source_table_exists=bigquery_table_exists(client, source_table),
                already_finalized=already_finalized,
            )
            if skip_reason:
                print(f"{DAG_ID}: {skip_reason}")
                continue

            daily_rows = [
                dict(row.items())
                for row in client.query(
                    daily_analysis_query(
                        report_date,
                        source_table=source_table,
                        source_table_type=source_table_type,
                    )
                ).result()
            ]
            landing_page_rows = [
                dict(row.items())
                for row in client.query(
                    landing_page_daily_analysis_query(
                        report_date,
                        source_table=source_table,
                        source_table_type=source_table_type,
                    )
                ).result()
            ]
            purchaser_behavior_rows = [
                dict(row.items())
                for row in client.query(
                    daily_purchaser_behavior_query(
                        report_date,
                        source_table=source_table,
                        source_table_type=source_table_type,
                    )
                ).result()
            ]

            merge_daily_analysis(daily_rows, report_date)
            merge_landing_page_daily_analysis(landing_page_rows, report_date)
            merge_daily_purchaser_behavior(purchaser_behavior_rows, report_date)

            result = {
                "report_date": report_date.isoformat(),
                "source_table": source_table,
                "source_table_type": source_table_type,
                "daily_rows": len(daily_rows),
                "landing_page_rows": len(landing_page_rows),
                "purchaser_behavior_rows": len(purchaser_behavior_rows),
            }
            print(f"{DAG_ID}: wrote {result}")
            results.append(result)

        return {"refreshed": results}

    write_daily_analysis()


ga4_daily_analysis()
