"""Write provisional and finalized GA4 daily session analysis to Cloud SQL.

Every two hours, each candidate report date loads from BigQuery using
`events_intraday_YYYYMMDD` when that table exists, otherwise
`events_YYYYMMDD`. Rows already marked finalized in Cloud SQL are skipped.

Finalized exports are written once per report date; after that the DAG stops
refreshing that date.

Manual backfill (Airflow UI **Trigger DAG w/ config** or CLI `--conf`):

```json
{
  "backfill_start": "2026-05-28",
  "backfill_end": "2026-06-14"
}
```

That creates one mapped task instance per report date in the inclusive range.
Scheduled runs without config still refresh the logical date plus the prior
three days.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Mapping
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
from merino_ga4_jobs.daily_search import (  # noqa: E402  # type: ignore[import-not-found]
    daily_search_content_query,
    daily_search_query,
    merge_daily_search,
    merge_daily_search_content,
)
DAG_ID = "ga4_daily_analysis"
GCP_CONN_ID = "google_cloud_default"


def ga4_refresh_candidates(logical_date: Any) -> list[date]:
    run_date = logical_date.date() if hasattr(logical_date, "date") else logical_date
    return [
        run_date,
        run_date - timedelta(days=1),
        run_date - timedelta(days=2),
        run_date - timedelta(days=3),
    ]


def ga4_report_dates_for_run(
    *,
    logical_date: Any,
    dag_run_conf: Mapping[str, Any] | None,
) -> list[date]:
    conf = dag_run_conf or {}
    start = conf.get("backfill_start")
    end = conf.get("backfill_end")
    if start and end:
        start_date = date.fromisoformat(str(start))
        end_date = date.fromisoformat(str(end))
        if end_date < start_date:
            raise ValueError(
                f"backfill_end must be on or after backfill_start: "
                f"{start_date.isoformat()} > {end_date.isoformat()}"
            )
        dates: list[date] = []
        current = start_date
        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)
        return dates

    return ga4_refresh_candidates(logical_date)


def resolve_ga4_source_table(
    client: Any,
    report_date: date,
    *,
    table_exists: Callable[[Any, str], bool] | None = None,
) -> tuple[str, str] | None:
    """Prefer intraday export, then fall back to finalized events for the date."""
    exists = table_exists or bigquery_table_exists
    intraday_table = ga4_source_table(report_date, intraday=True)
    finalized_table = ga4_source_table(report_date, intraday=False)

    if exists(client, intraday_table):
        return intraday_table, "intraday"
    if exists(client, finalized_table):
        return finalized_table, "finalized"
    return None


def ga4_refresh_skip_reason(
    *,
    report_date: date,
    already_finalized: bool,
    resolved_source: tuple[str, str] | None,
) -> str | None:
    if already_finalized:
        return (
            f"report_date={report_date.isoformat()} already finalized in Cloud SQL; skipping"
        )

    if resolved_source is None:
        return (
            f"missing intraday and finalized source tables for "
            f"report_date={report_date.isoformat()}; skipping"
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


def refresh_ga4_report_date(client: Any, report_date: date) -> dict[str, Any]:
    already_finalized = report_date_is_finalized(report_date)
    resolved_source = resolve_ga4_source_table(client, report_date)
    skip_reason = ga4_refresh_skip_reason(
        report_date=report_date,
        already_finalized=already_finalized,
        resolved_source=resolved_source,
    )
    if skip_reason:
        print(f"{DAG_ID}: {skip_reason}")
        return {
            "report_date": report_date.isoformat(),
            "skipped": skip_reason,
        }

    source_table, source_table_type = resolved_source

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
    daily_search_rows = [
        dict(row.items())
        for row in client.query(
            daily_search_query(
                report_date,
                source_table=source_table,
                source_table_type=source_table_type,
            )
        ).result()
    ]
    daily_search_content_rows = [
        dict(row.items())
        for row in client.query(
            daily_search_content_query(
                report_date,
                source_table=source_table,
                source_table_type=source_table_type,
            )
        ).result()
    ]
    merge_daily_analysis(daily_rows, report_date)
    merge_landing_page_daily_analysis(landing_page_rows, report_date)
    merge_daily_purchaser_behavior(purchaser_behavior_rows, report_date)
    merge_daily_search(daily_search_rows, report_date)
    merge_daily_search_content(daily_search_content_rows, report_date)

    result = {
        "report_date": report_date.isoformat(),
        "source_table": source_table,
        "source_table_type": source_table_type,
        "daily_rows": len(daily_rows),
        "landing_page_rows": len(landing_page_rows),
        "purchaser_behavior_rows": len(purchaser_behavior_rows),
        "daily_search_rows": len(daily_search_rows),
        "daily_search_content_rows": len(daily_search_content_rows),
    }
    print(f"{DAG_ID}: wrote {result}")
    return result


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
    def plan_report_dates() -> list[str]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        context = get_current_context()
        logical_date = context["logical_date"].in_timezone(REPORT_TIMEZONE)
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run is not None else None

        dates = ga4_report_dates_for_run(logical_date=logical_date, dag_run_conf=conf)
        print(f"{DAG_ID}: planned report_dates={[d.isoformat() for d in dates]}")
        return [report_date.isoformat() for report_date in dates]

    @task
    def write_report_date(report_date: str) -> dict[str, Any]:
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook  # type: ignore[import-not-found]

        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
        client = hook.get_client(project_id=PROJECT_ID)
        return refresh_ga4_report_date(client, date.fromisoformat(report_date))

    write_report_date.expand(report_date=plan_report_dates())


ga4_daily_analysis()
