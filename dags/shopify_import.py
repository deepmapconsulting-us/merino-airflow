"""Sync Shopify orders, transactions/fees, and inventory into Cloud SQL every 6 hours.

Runs ``run_shopify_all.sh`` in the ``merino-shopify-cli`` image via
``KubernetesPodOperator``. Each run:

1. **Full order import** — customers from orders, order headers, line items.
   Shopify query uses ``updated_at`` over the Airflow interval (+ overlap).
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
SHOPIFY_ORDER_OVERLAP_MINUTES = 30
SHOPIFY_TRANSACTION_OVERLAP_HOURS = 48

SHOPIFY_IMPORT_COMMAND = """\
set -euo pipefail
FROM_DATE='{{ dag_run.conf.get("from_date", "") }}'
TO_DATE='{{ dag_run.conf.get("to_date", "") }}'
PARTITION_DATE='{{ dag_run.conf.get("partition_date") or dag_run.conf.get("to_date") or data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d") }}'
ORDER_QUERY='{{ dag_run.conf.get("order_query") or ("updated_at:>=" ~ (data_interval_start - macros.timedelta(minutes=30)).in_timezone("UTC").strftime("%Y-%m-%dT%H:%M:%SZ") ~ " updated_at:<" ~ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%dT%H:%M:%SZ") ~ " financial_status:paid") }}'
TRANSACTION_QUERY='{{ dag_run.conf.get("transaction_query") or ("updated_at:>=" ~ (data_interval_start - macros.timedelta(hours=48)).in_timezone("UTC").strftime("%Y-%m-%dT%H:%M:%SZ") ~ " updated_at:<" ~ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")) }}'
ARGS=(--partition-date "$PARTITION_DATE")
if [[ -n "$FROM_DATE" && -n "$TO_DATE" ]]; then
  ARGS+=(--from-date "$FROM_DATE" --to-date "$TO_DATE")
else
  ARGS+=(--order-query "$ORDER_QUERY" --transaction-query "$TRANSACTION_QUERY")
fi
{% if dag_run.conf.get("include_customers") in [True, "true", "1", "yes"] -%}
ARGS+=(--include-customers)
{% endif -%}
{% if dag_run.conf.get("customer_query") -%}
ARGS+=(--customer-query '{{ dag_run.conf.get("customer_query") }}')
{% endif -%}
{% if dag_run.conf.get("overwrite") in [True, "true", "1", "yes"] -%}
ARGS+=(--overwrite)
{% endif -%}
exec bash scripts/run_shopify_all.sh "${ARGS[@]}"
"""


def shopify_incremental_queries(
    *,
    data_interval_start: pendulum.DateTime,
    data_interval_end: pendulum.DateTime,
    order_overlap_minutes: int = SHOPIFY_ORDER_OVERLAP_MINUTES,
    transaction_overlap_hours: int = SHOPIFY_TRANSACTION_OVERLAP_HOURS,
) -> dict[str, str]:
    """Build default incremental Shopify search queries from the Airflow interval."""
    order_window_start = (
        pendulum.instance(data_interval_start).in_timezone("UTC").subtract(minutes=order_overlap_minutes)
    )
    transaction_window_start = (
        pendulum.instance(data_interval_start).in_timezone("UTC").subtract(hours=transaction_overlap_hours)
    )
    window_end = pendulum.instance(data_interval_end).in_timezone("UTC")
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
