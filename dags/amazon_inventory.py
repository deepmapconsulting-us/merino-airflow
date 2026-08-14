"""Load listings, FBA inventory, and inventory age for each Amazon marketplace."""

from __future__ import annotations

from datetime import timedelta

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag  # type: ignore[import-not-found]
from amazon_k8s import amazon_pod

DAG_ID = "amazon_inventory"
MARKETPLACES = ("US", "CA", "MX", "BR", "AU")

INVENTORY_COMMAND = """\
set -euo pipefail
{% set conf = dag_run.conf or {} -%}
{% set interval_end = (
  data_interval_end if data_interval_end is defined and data_interval_end is not none
  else (logical_date if logical_date is defined and logical_date is not none else dag_run.run_after)
) -%}
{% set requested_marketplaces = conf.get("marketplaces", ["US", "CA", "MX", "BR", "AU"]) -%}
SNAPSHOT_DATE="{{ conf.get("end") or conf.get("snapshot_date") or interval_end.strftime("%Y-%m-%d") }}"
{% if requested_marketplaces is string -%}
MARKETPLACES="{{ requested_marketplaces }}"
{% else -%}
MARKETPLACES="{{ requested_marketplaces | join(",") }}"
{% endif -%}
case ",$MARKETPLACES," in
  *,__MARKETPLACE__,*) ;;
  *) exit 0 ;;
esac
exec __ENTRYPOINT__ --marketplace __MARKETPLACE__ --snapshot-date "$SNAPSHOT_DATE"
"""


def inventory_command(entrypoint: str, marketplace: str) -> str:
    return INVENTORY_COMMAND.replace("__ENTRYPOINT__", entrypoint).replace(
        "__MARKETPLACE__",
        marketplace,
    )


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["amazon", "sp-api", "inventory"],
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=15),
    },
    doc_md=__doc__,
)
def amazon_inventory():
    for marketplace in MARKETPLACES:
        listings = amazon_pod(
            task_id=f"listings_{marketplace.lower()}",
            cmds=["bash", "-lc"],
            arguments=[inventory_command("merino-amazon-listings", marketplace)],
        )
        inventory = amazon_pod(
            task_id=f"fba_inventory_{marketplace.lower()}",
            cmds=["bash", "-lc"],
            arguments=[inventory_command("merino-amazon-fba-inventory", marketplace)],
        )
        inventory_age = amazon_pod(
            task_id=f"inventory_age_{marketplace.lower()}",
            cmds=["bash", "-lc"],
            arguments=[
                inventory_command("merino-amazon-fba-inventory-age", marketplace)
            ],
        )
        listings >> inventory >> inventory_age


amazon_inventory()
