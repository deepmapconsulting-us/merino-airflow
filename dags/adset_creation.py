"""Create planned Meta adsets from audience_analysis queue rows.

Scheduled runs process queue rows for the DAG logical date. Manual runs can pass:

{
  "planned_date": "2026-07-02",
  "dry_run": true
}
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag, task  # type: ignore[import-not-found]

from meta_gcs import REPORT_TIMEZONE, meta_access_token

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
if MODULE_PATH.exists():
    sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs.adset_creation import (  # noqa: E402  # type: ignore[import-not-found]
    POSTGRES_CONN_ID,
    create_planned_adsets,
)
from merino_meta_jobs.facebook_graph import MetaGraphClient  # noqa: E402  # type: ignore[import-not-found]

DAG_ID = "adset_creation"


def planned_date_from_context(context: dict[str, Any]) -> str:
    dag_run = context.get("dag_run")
    conf = dict(getattr(dag_run, "conf", None) or {}) if dag_run is not None else {}
    if conf.get("planned_date"):
        return str(conf["planned_date"])

    logical_date = context.get("logical_date") or context.get("data_interval_start")
    if logical_date is None:
        return pendulum.now(REPORT_TIMEZONE).date().isoformat()
    if hasattr(logical_date, "in_timezone"):
        return logical_date.in_timezone(REPORT_TIMEZONE).date().isoformat()
    return pendulum.instance(logical_date).in_timezone(REPORT_TIMEZONE).date().isoformat()


def dry_run_from_conf(conf: dict[str, Any] | None) -> bool:
    value = (conf or {}).get("dry_run", False)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


@dag(
    dag_id=DAG_ID,
    schedule="0 23 * * *",
    start_date=pendulum.datetime(2026, 7, 1, 23, 0, tz=REPORT_TIMEZONE),
    catchup=False,
    tags=["meta", "adset", "creation"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    doc_md=__doc__,
)
def adset_creation():
    @task
    def create_adsets() -> dict[str, int]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]
        from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-not-found]

        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = dict(getattr(dag_run, "conf", None) or {}) if dag_run is not None else {}
        planned_date = planned_date_from_context(context)
        dry_run = dry_run_from_conf(conf)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        try:
            result = create_planned_adsets(
                conn,
                MetaGraphClient(meta_access_token()),
                planned_date=planned_date,
                dry_run=dry_run,
            )
            if not dry_run:
                conn.commit()
            else:
                conn.rollback()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        print(f"{DAG_ID}: processed planned_date={planned_date} result={result}")
        return result

    create_adsets()


adset_creation()
