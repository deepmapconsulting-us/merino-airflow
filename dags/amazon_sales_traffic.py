"""Refresh Amazon Sales & Traffic for the latest three complete UTC days."""

from __future__ import annotations

from datetime import timedelta

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag  # type: ignore[import-not-found]
from amazon_k8s import amazon_pod

DAG_ID = "amazon_sales_traffic"

SALES_TRAFFIC_COMMAND = """\
set -euo pipefail
{% set conf = dag_run.conf or {} -%}
{% set interval_end = (
  data_interval_end if data_interval_end is defined and data_interval_end is not none
  else (logical_date if logical_date is defined and logical_date is not none else dag_run.run_after)
) -%}
{% set default_end = interval_end - macros.timedelta(days=1) -%}
{% set requested_marketplaces = conf.get("marketplaces", ["US", "CA", "MX", "BR", "AU"]) -%}
START_DATE="{{ conf.get("start") or (default_end - macros.timedelta(days=2)).strftime("%Y-%m-%d") }}"
END_DATE="{{ conf.get("end") or default_end.strftime("%Y-%m-%d") }}"
{% if requested_marketplaces is string -%}
MARKETPLACES="{{ requested_marketplaces }}"
{% else -%}
MARKETPLACES="{{ requested_marketplaces | join(",") }}"
{% endif -%}
ARGS=(--start-date "$START_DATE" --end-date "$END_DATE" --granularity PARENT --granularity CHILD --granularity SKU)
IFS=',' read -ra MARKETPLACE_CODES <<< "$MARKETPLACES"
for MARKETPLACE in "${MARKETPLACE_CODES[@]}"; do
  ARGS+=(--marketplace "$MARKETPLACE")
done
{% if conf.get("overwrite") in [True, "true", "1", "yes"] -%}
ARGS+=(--overwrite)
{% endif -%}
exec merino-amazon-jobs "${ARGS[@]}"
"""


@dag(
    dag_id=DAG_ID,
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["amazon", "sp-api", "sales-traffic"],
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=15),
    },
    doc_md=__doc__,
)
def amazon_sales_traffic():
    amazon_pod(
        task_id="refresh_sales_traffic",
        cmds=["bash", "-lc"],
        arguments=[SALES_TRAFFIC_COMMAND],
    )


amazon_sales_traffic()
