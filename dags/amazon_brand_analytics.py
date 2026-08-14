"""Load Brand Analytics for the previous complete Sunday-Saturday week."""

from __future__ import annotations

from datetime import timedelta

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag  # type: ignore[import-not-found]
from amazon_k8s import amazon_pod

DAG_ID = "amazon_brand_analytics"

BRAND_ANALYTICS_COMMAND = """\
set -euo pipefail
{% set conf = dag_run.conf or {} -%}
{% set interval_end = (
  data_interval_end if data_interval_end is defined and data_interval_end is not none
  else (logical_date if logical_date is defined and logical_date is not none else dag_run.run_after)
) -%}
{% set current_week_start = interval_end.start_of("week") -%}
{% set requested_marketplaces = conf.get("marketplaces", ["US", "CA", "MX", "BR", "AU"]) -%}
START_DATE="{{ conf.get("start") or (current_week_start - macros.timedelta(days=8)).strftime("%Y-%m-%d") }}"
END_DATE="{{ conf.get("end") or (current_week_start - macros.timedelta(days=2)).strftime("%Y-%m-%d") }}"
{% if requested_marketplaces is string -%}
MARKETPLACES="{{ requested_marketplaces }}"
{% else -%}
MARKETPLACES="{{ requested_marketplaces | join(",") }}"
{% endif -%}
IFS=',' read -ra MARKETPLACE_CODES <<< "$MARKETPLACES"
for MARKETPLACE in "${MARKETPLACE_CODES[@]}"; do
  merino-amazon-brand-analytics \
    --marketplace "$MARKETPLACE" \
    --start-date "$START_DATE" \
    --end-date "$END_DATE" \
    --period WEEK
done
"""


@dag(
    dag_id=DAG_ID,
    schedule="0 11 * * 1",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["amazon", "sp-api", "brand-analytics"],
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=30),
    },
    doc_md=__doc__,
)
def amazon_brand_analytics():
    amazon_pod(
        task_id="load_previous_week",
        cmds=["bash", "-lc"],
        arguments=[BRAND_ANALYTICS_COMMAND],
    )


amazon_brand_analytics()
