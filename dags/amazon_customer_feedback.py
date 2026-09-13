"""Load weekly US Amazon Customer Feedback review-topic analytics."""

from __future__ import annotations

from datetime import timedelta

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag  # type: ignore[import-not-found]
from amazon_k8s import amazon_pod

DAG_ID = "amazon_customer_feedback"

CUSTOMER_FEEDBACK_COMMAND = """\
set -euo pipefail
{% set conf = dag_run.conf or {} -%}
{% set run_date = (
  logical_date if logical_date is defined and logical_date is not none
  else dag_run.run_after
) -%}
ARGS=(--marketplace US --snapshot-date "{{ conf.get("snapshot_date") or run_date.strftime("%Y-%m-%d") }}")
{% set requested_asins = conf.get("asins", []) -%}
{% if requested_asins is string -%}
ARGS+=(--asin "{{ requested_asins }}")
{% else -%}
{% for asin in requested_asins %}
ARGS+=(--asin "{{ asin }}")
{% endfor -%}
{% endif -%}
exec merino-amazon-customer-feedback "${ARGS[@]}"
"""


@dag(
    dag_id=DAG_ID,
    schedule="0 13 * * 1",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["amazon", "sp-api", "customer-feedback"],
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=15),
    },
    doc_md=__doc__,
)
def amazon_customer_feedback():
    amazon_pod(
        task_id="customer_feedback_us",
        cmds=["bash", "-lc"],
        arguments=[CUSTOMER_FEEDBACK_COMMAND],
    )


amazon_customer_feedback()
