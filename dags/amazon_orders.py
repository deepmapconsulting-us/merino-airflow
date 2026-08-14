"""Incrementally refresh Amazon Orders API 2026 data with a three-day overlap."""

from __future__ import annotations

from datetime import timedelta

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag  # type: ignore[import-not-found]
from amazon_k8s import amazon_pod

DAG_ID = "amazon_orders"

ORDERS_COMMAND = """\
set -euo pipefail
{% set conf = dag_run.conf or {} -%}
{% set interval_end = (
  data_interval_end if data_interval_end is defined and data_interval_end is not none
  else (logical_date if logical_date is defined and logical_date is not none else dag_run.run_after)
) -%}
{% set default_end = interval_end - macros.timedelta(days=1) -%}
{% set requested_marketplaces = conf.get("marketplaces", ["US", "CA", "MX", "BR", "AU"]) -%}
START_DATE="{{ conf.get("start") or (default_end - macros.timedelta(days=3)).strftime("%Y-%m-%d") }}"
END_DATE="{{ conf.get("end") or default_end.strftime("%Y-%m-%d") }}"
if [[ "$START_DATE" < "2026-01-01" ]]; then START_DATE="2026-01-01"; fi
{% if requested_marketplaces is string -%}
MARKETPLACES="{{ requested_marketplaces }}"
{% else -%}
MARKETPLACES="{{ requested_marketplaces | join(",") }}"
{% endif -%}
IFS=',' read -ra MARKETPLACE_CODES <<< "$MARKETPLACES"
for MARKETPLACE in "${MARKETPLACE_CODES[@]}"; do
  merino-amazon-orders \
    --marketplace "$MARKETPLACE" \
    --start-date "$START_DATE" \
    --end-date "$END_DATE"
done
"""


@dag(
    dag_id=DAG_ID,
    schedule="0 10 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["amazon", "sp-api", "orders"],
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=15),
    },
    doc_md=__doc__,
)
def amazon_orders():
    amazon_pod(
        task_id="refresh_orders",
        cmds=["bash", "-lc"],
        arguments=[ORDERS_COMMAND],
    )


amazon_orders()
