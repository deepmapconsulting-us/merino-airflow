"""Sync Shopify customers, orders, and inventory into Cloud SQL every 12 hours.

Runs ``run_shopify_all.sh`` in the ``merino-shopify-cli`` image via
``KubernetesPodOperator``. Default window: customers/orders last 2 days,
inventory partition for today (UTC).

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
from typing import Any

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag  # type: ignore[import-not-found]

from shopify_k8s import shopify_import_pod

DAG_ID = "shopify_import"
REPORT_TIMEZONE = "UTC"

SHOPIFY_IMPORT_COMMAND = """\
set -euo pipefail
FROM_DATE='{{ dag_run.conf.get("from_date") or (logical_date - macros.timedelta(days=1)).strftime("%Y-%m-%d") }}'
TO_DATE='{{ dag_run.conf.get("to_date") or logical_date.strftime("%Y-%m-%d") }}'
PARTITION_DATE='{{ dag_run.conf.get("partition_date") or dag_run.conf.get("to_date") or logical_date.strftime("%Y-%m-%d") }}'
ARGS=(--from-date "$FROM_DATE" --to-date "$TO_DATE" --partition-date "$PARTITION_DATE")
{% if dag_run.conf.get("overwrite") in [True, "true", "1", "yes"] -%}
ARGS+=(--overwrite)
{% endif -%}
exec bash scripts/run_shopify_all.sh "${ARGS[@]}"
"""


def shopify_run_arguments(*, logical_date: pendulum.DateTime, dag_run_conf: dict[str, Any]) -> list[str]:
    """Build CLI args for ``run_shopify_all.sh`` from schedule or manual conf."""
    if dag_run_conf.get("from_date") and dag_run_conf.get("to_date"):
        from_date = str(dag_run_conf["from_date"])
        to_date = str(dag_run_conf["to_date"])
    else:
        to_date = logical_date.format("YYYY-MM-DD")
        from_date = logical_date.subtract(days=1).format("YYYY-MM-DD")

    partition_date = str(dag_run_conf.get("partition_date") or to_date)
    args = [
        "--from-date",
        from_date,
        "--to-date",
        to_date,
        "--partition-date",
        partition_date,
    ]
    if str(dag_run_conf.get("overwrite", "")).strip().lower() in {"1", "true", "yes"}:
        args.append("--overwrite")
    return args


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
