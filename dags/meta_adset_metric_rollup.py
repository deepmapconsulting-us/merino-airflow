"""Compute rolling daily adset purchase metrics from Meta adset snapshots.

Scheduled runs use the latest available partition in
``marketing.meta_adset_daily_snapshot``. Manual runs can pass:

{
  "partition_date": "2026-07-01"
}
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag, task  # type: ignore[import-not-found]

from meta_gcs import REPORT_TIMEZONE

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
if MODULE_PATH.exists():
    sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs.adset_metric_rollup import (  # noqa: E402  # type: ignore[import-not-found]
    POSTGRES_CONN_ID,
    refresh_adset_metric_rollup,
)

DAG_ID = "meta_adset_metric_rollup"


def partition_date_from_conf(conf: dict[str, Any] | None) -> str | None:
    value = (conf or {}).get("partition_date")
    if value in (None, ""):
        return None
    return str(value)


@dag(
    dag_id=DAG_ID,
    schedule="30 23 * * *",
    start_date=pendulum.datetime(2026, 7, 1, 23, 30, tz=REPORT_TIMEZONE),
    catchup=False,
    tags=["meta", "adset", "metric-rollup"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    doc_md=__doc__,
)
def meta_adset_metric_rollup():
    @task
    def refresh_rollup() -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]
        from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-not-found]

        context = get_current_context()
        dag_run = context.get("dag_run")
        partition_date = partition_date_from_conf(
            dict(getattr(dag_run, "conf", None) or {}) if dag_run is not None else None
        )

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        try:
            result = refresh_adset_metric_rollup(conn, partition_date=partition_date)
        finally:
            conn.close()

        print(f"{DAG_ID}: refreshed {result}")
        return result

    refresh_rollup()


meta_adset_metric_rollup()
