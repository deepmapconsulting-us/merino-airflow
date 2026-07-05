"""Sync Shopify order transactions and payment fees into Cloud SQL.

The job uses Shopify order ``updated_at`` rather than ``created_at`` because
transactions and fees can arrive after the order is placed, especially captures,
refunds, and foreign exchange adjustments.

Manual backfill (Trigger DAG w/ config):

```json
{
  "from_date": "2026-05-01",
  "to_date": "2026-07-03"
}
```
"""

from __future__ import annotations

from datetime import timedelta

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag  # type: ignore[import-not-found]

from shopify_k8s import shopify_import_pod

DAG_ID = "shopify_order_transactions"
REPORT_TIMEZONE = "UTC"
SHOPIFY_TRANSACTION_OVERLAP_HOURS = 48

SHOPIFY_TRANSACTION_COMMAND = """\
set -euo pipefail
FROM_DATE='{{ dag_run.conf.get("from_date", "") }}'
TO_DATE='{{ dag_run.conf.get("to_date", "") }}'
ORDER_QUERY='{{ dag_run.conf.get("order_query") or ("updated_at:>=" ~ (data_interval_start - macros.timedelta(hours=48)).in_timezone("UTC").strftime("%Y-%m-%dT%H:%M:%SZ") ~ " updated_at:<" ~ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")) }}'
ARGS=()
if [[ -n "$FROM_DATE" && -n "$TO_DATE" ]]; then
  ARGS+=(--from-date "$FROM_DATE" --to-date "$TO_DATE" --date-field updated_at)
else
  ARGS+=(--order-query "$ORDER_QUERY")
fi
{% if dag_run.conf.get("dry_run") in [True, "true", "1", "yes"] -%}
ARGS+=(--dry-run)
{% endif -%}
exec python3 shopify/import_order_transactions.py "${ARGS[@]}"
"""


def transaction_order_query(
    *,
    data_interval_start: pendulum.DateTime,
    data_interval_end: pendulum.DateTime,
    overlap_hours: int = SHOPIFY_TRANSACTION_OVERLAP_HOURS,
) -> str:
    """Build the default transaction sync query from the Airflow interval."""
    window_start = pendulum.instance(data_interval_start).in_timezone("UTC").subtract(hours=overlap_hours)
    window_end = pendulum.instance(data_interval_end).in_timezone("UTC")
    return (
        f"updated_at:>={window_start.strftime('%Y-%m-%dT%H:%M:%SZ')} "
        f"updated_at:<{window_end.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    )


@dag(
    dag_id=DAG_ID,
    schedule="0 */6 * * *",
    start_date=pendulum.datetime(2026, 7, 1, 0, 0, tz=REPORT_TIMEZONE),
    catchup=False,
    max_active_runs=1,
    tags=["shopify", "postgres", "transactions", "fees"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
    },
    doc_md=__doc__,
)
def shopify_order_transactions():
    shopify_import_pod(
        task_id="import_shopify_order_transactions",
        cmds=["bash", "-lc"],
        arguments=[SHOPIFY_TRANSACTION_COMMAND],
    )


shopify_order_transactions()
