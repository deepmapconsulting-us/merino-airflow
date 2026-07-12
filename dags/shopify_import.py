"""Sync Shopify orders, transactions/fees, and inventory into Cloud SQL every 6 hours.

Runs ``run_shopify_all.sh`` in the ``merino-shopify-cli`` image via
``KubernetesPodOperator``. Each run:

1. **Full order import** — customers from orders, order headers, line items.
   Shopify query uses ``updated_at`` over the last 24 hours.
   Postgres skip when ``shopify.order_line_items`` already exist for the order.
2. **Transaction/fee import** — refunds, captures, processing fees.
   Shopify query uses ``updated_at`` with a longer overlap for late-arriving
   payment events. Only inserts an order stub when the order row is missing.
3. **Inventory** — today's partition snapshot.

Prerequisites:

- Airflow connection ``merino_analytics`` (GSM: ``airflow-connections-merino_analytics``)
- Airflow Variable ``google_geocoding_api_key`` (GSM: ``airflow-variables-google_geocoding_api_key``)
- Kubernetes secret ``shopify-cli-store-auth`` in namespace ``airflow``::

    SHOPIFY_CLI_AUTH_NAMESPACES=airflow bash terraform/scripts/sync-shopify-cli-auth-secret.sh

Manual backfill (Trigger DAG w/ config):

```json
{
  "from_date": "2026-06-01",
  "to_date": "2026-06-14",
  "partition_date": "2026-06-14",
  "overwrite": true
}
```
"""

from __future__ import annotations

from datetime import timedelta

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag  # type: ignore[import-not-found]

from shopify_k8s import shopify_import_pod

DAG_ID = "shopify_import"
REPORT_TIMEZONE = "UTC"
SHOPIFY_ORDER_LOOKBACK_HOURS = 24
SHOPIFY_TRANSACTION_OVERLAP_HOURS = 48

SHOPIFY_IMPORT_COMMAND = """\
set -euo pipefail
{# Airflow 3 manual triggers may omit data_interval_* / logical_date; run_after is always set. #}
{% set conf = dag_run.conf or {} -%}
{% set interval_end = (
  data_interval_end if data_interval_end is defined and data_interval_end is not none
  else (logical_date if logical_date is defined and logical_date is not none else dag_run.run_after)
) -%}
{% set interval_start = (
  data_interval_start if data_interval_start is defined and data_interval_start is not none
  else (interval_end - macros.timedelta(hours=6))
) -%}
FROM_DATE='{{ conf.get("from_date", "") }}'
TO_DATE='{{ conf.get("to_date", "") }}'
PARTITION_DATE='{{ conf.get("partition_date") or conf.get("to_date") or interval_end.in_timezone("UTC").strftime("%Y-%m-%d") }}'
ORDER_QUERY='{{ conf.get("order_query") or ("updated_at:>=" ~ (interval_end - macros.timedelta(hours=24)).in_timezone("UTC").strftime("%Y-%m-%dT%H:%M:%SZ") ~ " updated_at:<" ~ interval_end.in_timezone("UTC").strftime("%Y-%m-%dT%H:%M:%SZ") ~ " financial_status:paid") }}'
TRANSACTION_QUERY='{{ conf.get("transaction_query") or ("updated_at:>=" ~ (interval_start - macros.timedelta(hours=48)).in_timezone("UTC").strftime("%Y-%m-%dT%H:%M:%SZ") ~ " updated_at:<" ~ interval_end.in_timezone("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")) }}'
ARGS=(--partition-date "$PARTITION_DATE")
if [[ -n "$FROM_DATE" && -n "$TO_DATE" ]]; then
  ARGS+=(--from-date "$FROM_DATE" --to-date "$TO_DATE")
else
  ARGS+=(--order-query "$ORDER_QUERY" --transaction-query "$TRANSACTION_QUERY")
fi
{% if conf.get("include_customers") in [True, "true", "1", "yes"] -%}
ARGS+=(--include-customers)
{% endif -%}
{% if conf.get("customer_query") -%}
ARGS+=(--customer-query '{{ conf.get("customer_query") }}')
{% endif -%}
{% if conf.get("overwrite") in [True, "true", "1", "yes"] -%}
ARGS+=(--overwrite)
{% endif -%}
exec bash scripts/run_shopify_all.sh "${ARGS[@]}"
"""


def shopify_incremental_queries(
    *,
    data_interval_start: pendulum.DateTime,
    data_interval_end: pendulum.DateTime,
    order_lookback_hours: int = SHOPIFY_ORDER_LOOKBACK_HOURS,
    transaction_overlap_hours: int = SHOPIFY_TRANSACTION_OVERLAP_HOURS,
) -> dict[str, str]:
    """Build default incremental Shopify search queries from the Airflow interval."""
    window_end = pendulum.instance(data_interval_end).in_timezone("UTC")
    order_window_start = window_end.subtract(hours=order_lookback_hours)
    transaction_window_start = (
        pendulum.instance(data_interval_start).in_timezone("UTC").subtract(hours=transaction_overlap_hours)
    )
    order_window = (
        f"updated_at:>={order_window_start.strftime('%Y-%m-%dT%H:%M:%SZ')} "
        f"updated_at:<{window_end.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    )
    transaction_window = (
        f"updated_at:>={transaction_window_start.strftime('%Y-%m-%dT%H:%M:%SZ')} "
        f"updated_at:<{window_end.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    )
    return {
        "order_query": f"{order_window} financial_status:paid",
        "transaction_query": transaction_window,
    }


@dag(
    dag_id=DAG_ID,
    schedule="0 */6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, 0, 0, tz=REPORT_TIMEZONE),
    catchup=False,
    max_active_runs=1,
    tags=["shopify", "postgres", "inventory", "transactions", "fees"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
    },
    doc_md=__doc__,
)
def shopify_import():
    shopify_import_pod(
        task_id="import_shopify",
        cmds=["bash", "-lc"],
        arguments=[SHOPIFY_IMPORT_COMMAND],
    )


shopify_import()
