"""Sync Shopify customers, orders, and inventory into Cloud SQL every 12 hours.

Runs ``run_shopify_all.sh`` in the ``merino-shopify-cli`` image via
``KubernetesPodOperator``. Default behavior uses an incremental ``updated_at``
window from Airflow data interval boundaries with a small overlap to catch late
writes; inventory uses the current run date partition.

Prerequisites:

- Airflow connection ``merino_analytics`` (GSM: ``airflow-connections-merino_analytics``)
- Airflow Variable ``google_geocoding_api_key`` (GSM: ``airflow-variables-google_geocoding_api_key``)
  — required for **customer import** (Google Geocoding API → lat/lng → H3). Not used by inventory
  or by the standalone ``backfill_h3.py`` job (that only reads existing lat/lng from Postgres).
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
SHOPIFY_INCREMENTAL_OVERLAP_MINUTES = 30

SHOPIFY_IMPORT_COMMAND = """\
set -euo pipefail
FROM_DATE='{{ dag_run.conf.get("from_date", "") }}'
TO_DATE='{{ dag_run.conf.get("to_date", "") }}'
PARTITION_DATE='{{ dag_run.conf.get("partition_date") or dag_run.conf.get("to_date") or data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d") }}'
CUSTOMER_QUERY='{{ dag_run.conf.get("customer_query") or ("updated_at:>=" ~ (data_interval_start - macros.timedelta(minutes=30)).in_timezone("UTC").strftime("%Y-%m-%dT%H:%M:%SZ") ~ " updated_at:<" ~ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")) }}'
ORDER_QUERY='{{ dag_run.conf.get("order_query") or ("updated_at:>=" ~ (data_interval_start - macros.timedelta(minutes=30)).in_timezone("UTC").strftime("%Y-%m-%dT%H:%M:%SZ") ~ " updated_at:<" ~ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%dT%H:%M:%SZ") ~ " financial_status:paid") }}'
ARGS=(--partition-date "$PARTITION_DATE")
if [[ -n "$FROM_DATE" && -n "$TO_DATE" ]]; then
  ARGS+=(--from-date "$FROM_DATE" --to-date "$TO_DATE")
else
  ARGS+=(--customer-query "$CUSTOMER_QUERY" --order-query "$ORDER_QUERY")
fi
{% if dag_run.conf.get("overwrite") in [True, "true", "1", "yes"] -%}
ARGS+=(--overwrite)
{% endif -%}
exec bash scripts/run_shopify_all.sh "${ARGS[@]}"
"""


def shopify_incremental_queries(
    *,
    data_interval_start: pendulum.DateTime,
    data_interval_end: pendulum.DateTime,
    overlap_minutes: int = SHOPIFY_INCREMENTAL_OVERLAP_MINUTES,
) -> dict[str, str]:
    """Build default incremental Shopify search queries from the Airflow interval."""
    window_start = pendulum.instance(data_interval_start).in_timezone("UTC").subtract(minutes=overlap_minutes)
    window_end = pendulum.instance(data_interval_end).in_timezone("UTC")
    customer_query = (
        f"updated_at:>={window_start.strftime('%Y-%m-%dT%H:%M:%SZ')} "
        f"updated_at:<{window_end.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    )
    return {
        "customer_query": customer_query,
        "order_query": f"{customer_query} financial_status:paid",
    }


@dag(
    dag_id=DAG_ID,
    schedule="0 */12 * * *",
    start_date=pendulum.datetime(2026, 1, 1, 0, 0, tz=REPORT_TIMEZONE),
    catchup=False,
    tags=["shopify", "postgres", "inventory"],
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
