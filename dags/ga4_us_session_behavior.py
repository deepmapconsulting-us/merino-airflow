"""Write provisional and finalized GA4 session behavior aggregates to Cloud SQL.

Every two hours, today's report date loads from `events_intraday_YYYYMMDD` when
that table exists. Once near the end of the day, the DAG also checks the
finalized `events_YYYYMMDD` table for two days ago.

Manual backfill (Airflow UI **Trigger DAG w/ config** or CLI `--conf`):

```json
{
  "backfill_start": "2026-05-28",
  "backfill_end": "2026-06-14"
}
```
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
    ga4_source_table,
)
from merino_ga4_jobs.us_session_behavior import (  # noqa: E402  # type: ignore[import-not-found]
    merge_us_session_daily,
    merge_us_session_daily_hour,
    merge_us_session_daily_state,
    merge_us_session_daily_state_hour,
    us_session_daily_hour_query,
    us_session_daily_query,
    us_session_daily_state_hour_query,
    us_session_daily_state_query,
)

DAG_ID = "ga4_us_session_behavior"
GCP_CONN_ID = "google_cloud_default"
FINALIZED_REPORT_LAG_DAYS = 2
FINALIZED_REFRESH_HOUR = 22
SOURCE_TABLE_TYPES = {"intraday", "finalized"}


def ga4_default_refresh_plan(logical_date: Any) -> list[dict[str, str]]:
    run_date = logical_date.date() if hasattr(logical_date, "date") else logical_date
    run_hour = getattr(logical_date, "hour", FINALIZED_REFRESH_HOUR)
    plan = [
        {
            "report_date": run_date.isoformat(),
            "source_table_type": "intraday",
        }
    ]
    if run_hour == FINALIZED_REFRESH_HOUR:
        finalized_date = run_date - timedelta(days=FINALIZED_REPORT_LAG_DAYS)
        plan.append(
            {
                "report_date": finalized_date.isoformat(),
                "source_table_type": "finalized",
            }
        )
    return plan


def ga4_report_dates_for_run(
    *,
    logical_date: Any,
    dag_run_conf: Mapping[str, Any] | None,
) -> list[dict[str, str]]:
    conf = dag_run_conf or {}
    start = conf.get("backfill_start")
    end = conf.get("backfill_end")
    if start and end:
        source_table_type = str(conf.get("source_table_type") or "finalized")
        if source_table_type not in SOURCE_TABLE_TYPES:
            raise ValueError(
                f"source_table_type must be one of {sorted(SOURCE_TABLE_TYPES)}: "
                f"{source_table_type}"
            )
        start_date = date.fromisoformat(str(start))
        end_date = date.fromisoformat(str(end))
        if end_date < start_date:
            raise ValueError(
                f"backfill_end must be on or after backfill_start: "
                f"{start_date.isoformat()} > {end_date.isoformat()}"
            )
        dates: list[dict[str, str]] = []
        current = start_date
        while current <= end_date:
            dates.append(
                {
                    "report_date": current.isoformat(),
                    "source_table_type": source_table_type,
                }
            )
            current += timedelta(days=1)
        return dates

    return ga4_default_refresh_plan(logical_date)


def resolve_ga4_source_table(
    client: Any,
    report_date: date,
    *,
    source_table_type: str,
    table_exists: Callable[[Any, str], bool] | None = None,
) -> tuple[str, str] | None:
    """Return the requested GA4 source table only when it exists."""
    if source_table_type not in SOURCE_TABLE_TYPES:
        raise ValueError(
            f"source_table_type must be one of {sorted(SOURCE_TABLE_TYPES)}: "
            f"{source_table_type}"
        )

    exists = table_exists or bigquery_table_exists
    source_table = ga4_source_table(report_date, intraday=source_table_type == "intraday")

    if exists(client, source_table):
        return source_table, source_table_type
    return None


def ga4_refresh_skip_reason(
    *,
    report_date: date,
    resolved_source: tuple[str, str] | None,
) -> str | None:
    if resolved_source is None:
        return (
            f"missing requested source table for "
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


def refresh_ga4_us_session_report_date(
    client: Any,
    report_date: date,
    *,
    source_table_type: str,
) -> dict[str, Any]:
    resolved_source = resolve_ga4_source_table(
        client,
        report_date,
        source_table_type=source_table_type,
    )
    skip_reason = ga4_refresh_skip_reason(
        report_date=report_date,
        resolved_source=resolved_source,
    )
    if skip_reason:
        print(f"{DAG_ID}: {skip_reason}")
        return {
            "report_date": report_date.isoformat(),
            "source_table_type": source_table_type,
            "skipped": skip_reason,
        }

    source_table, source_table_type = resolved_source

    us_session_daily_rows = [
        dict(row.items())
        for row in client.query(
            us_session_daily_query(
                report_date,
                source_table=source_table,
                source_table_type=source_table_type,
            )
        ).result()
    ]
    us_session_daily_state_rows = [
        dict(row.items())
        for row in client.query(
            us_session_daily_state_query(
                report_date,
                source_table=source_table,
                source_table_type=source_table_type,
            )
        ).result()
    ]
    us_session_daily_hour_rows = [
        dict(row.items())
        for row in client.query(
            us_session_daily_hour_query(
                report_date,
                source_table=source_table,
                source_table_type=source_table_type,
            )
        ).result()
    ]
    us_session_daily_state_hour_rows = [
        dict(row.items())
        for row in client.query(
            us_session_daily_state_hour_query(
                report_date,
                source_table=source_table,
                source_table_type=source_table_type,
            )
        ).result()
    ]

    merge_us_session_daily(us_session_daily_rows, report_date)
    merge_us_session_daily_state(us_session_daily_state_rows, report_date)
    merge_us_session_daily_hour(us_session_daily_hour_rows, report_date)
    merge_us_session_daily_state_hour(us_session_daily_state_hour_rows, report_date)

    result = {
        "report_date": report_date.isoformat(),
        "source_table": source_table,
        "source_table_type": source_table_type,
        "us_session_daily_rows": len(us_session_daily_rows),
        "us_session_daily_state_rows": len(us_session_daily_state_rows),
        "us_session_daily_hour_rows": len(us_session_daily_hour_rows),
        "us_session_daily_state_hour_rows": len(us_session_daily_state_hour_rows),
    }
    print(f"{DAG_ID}: wrote {result}")
    return result


@dag(
    dag_id=DAG_ID,
    schedule="0 */2 * * *",
    start_date=pendulum.datetime(2026, 6, 1, 7, 0, tz=REPORT_TIMEZONE),
    catchup=False,
    tags=["ga4", "bigquery", "us-session-behavior"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    doc_md=__doc__,
)
def ga4_us_session_behavior():
    @task
    def plan_report_dates() -> list[dict[str, str]]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        context = get_current_context()
        logical_date = context["logical_date"].in_timezone(REPORT_TIMEZONE)
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run is not None else None

        plan = ga4_report_dates_for_run(logical_date=logical_date, dag_run_conf=conf)
        print(f"{DAG_ID}: planned refresh_plan={plan}")
        return plan

    @task
    def write_report_date(refresh_plan: Mapping[str, str]) -> dict[str, Any]:
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook  # type: ignore[import-not-found]

        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
        client = hook.get_client(project_id=PROJECT_ID)
        return refresh_ga4_us_session_report_date(
            client,
            date.fromisoformat(refresh_plan["report_date"]),
            source_table_type=refresh_plan["source_table_type"],
        )

    write_report_date.expand(refresh_plan=plan_report_dates())


ga4_us_session_behavior()
