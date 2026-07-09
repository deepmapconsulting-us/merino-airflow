"""Apply pending Meta adset evaluation queue rows.

This DAG is intentionally separate from `meta_adset_evaluation`: the evaluation
DAG reads evidence and writes queue rows, while this DAG performs Meta-side
changes from those queues.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag  # type: ignore[import-not-found]

from meta_adset_evaluation_k8s import meta_adset_evaluation_pod
from meta_gcs import REPORT_TIMEZONE

DAG_ID = "meta_adset_creation_trigger"
DAG_SCHEDULE = "*/10 * * * *"


def apply_budget_command_args(budget_change_type: str) -> dict[str, object]:
    return {
        "cmds": ["python", "-m", "meta_adset_evaluation_agent.apply_budget_changes"],
        "arguments": ["--budget-change-type", budget_change_type],
    }


def apply_ad_status_schedule_command_args() -> dict[str, object]:
    return {
        "cmds": ["python", "-m", "meta_adset_evaluation_agent.apply_ad_status_schedules"],
        "arguments": [],
    }


def apply_adset_split_command_args() -> dict[str, object]:
    return {
        "cmds": ["python", "-m", "meta_adset_evaluation_agent.apply_adset_splits"],
        "arguments": [],
    }


@dag(
    dag_id=DAG_ID,
    schedule=DAG_SCHEDULE,
    start_date=pendulum.datetime(2026, 7, 9, 0, 0, tz=REPORT_TIMEZONE),
    catchup=False,
    tags=["meta", "adset", "creation", "agent"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    doc_md=__doc__,
)
def meta_adset_creation_trigger():
    meta_adset_evaluation_pod(
        task_id="apply_increase_budget_changes",
        **apply_budget_command_args("increase_budget"),
    )
    meta_adset_evaluation_pod(
        task_id="apply_set_budget_changes",
        **apply_budget_command_args("set_budget"),
    )
    meta_adset_evaluation_pod(
        task_id="apply_ad_status_schedules",
        **apply_ad_status_schedule_command_args(),
    )
    meta_adset_evaluation_pod(
        task_id="apply_adset_splits",
        **apply_adset_split_command_args(),
    )


meta_adset_creation_trigger()
