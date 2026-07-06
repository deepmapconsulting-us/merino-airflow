"""Run the Meta adset evaluation agent for active campaign adsets.

Manual trigger config:

```json
{
  "source": "facebook",
  "campaign_id": "23800000000000000",
  "adset_ids": "23800000000000000,23800000000000001",
  "date": "2026-07-03"
}
```

Scheduled increase-budget runs read the latest campaign config snapshot, group
ACTIVE adsets by ACTIVE campaign, then evaluate up to 10 adsets per worker pod.
Campaigns with more than 10 active adsets are split into multiple pods.

`meta_adset_evaluation` runs hourly and uses `increase-budget` mode for
current-day performance checks. `meta_adset_set_budget_evaluation` runs daily
and uses `set-budget` mode with the previous seven days plus today.

Manual runs can pass `campaign_id` and `adset_ids` in DAG conf to override
snapshot discovery.

Scheduled runs only evaluate campaigns listed in the Airflow Variable
`meta_adset_evaluation_campaign_ids` (comma-separated). Default:
`52535307578056`.
"""

from __future__ import annotations

from datetime import timedelta
import shlex
import sys
from pathlib import Path
from typing import Any

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag, get_current_context, task  # type: ignore[import-not-found]

try:
    from airflow.providers.standard.sensors.external_task import ExternalTaskSensor  # type: ignore[import-not-found]
except ImportError:
    from airflow.sensors.external_task import ExternalTaskSensor  # type: ignore[import-not-found,no-redef]

from meta_adset_evaluation_k8s import meta_adset_evaluation_pod, meta_adset_evaluation_pod_partial
from meta_gcs import (
    REPORT_TIMEZONE,
    campaign_config_logical_date,
    read_json_from_gcs,
    read_latest_snapshot_pointer,
)

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
if MODULE_PATH.exists():
    sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs.adset_config import active_adsets_from_flat  # noqa: E402  # type: ignore[import-not-found]
from merino_meta_jobs.object_property import flatten_config_snapshot  # noqa: E402  # type: ignore[import-not-found]

DAG_ID = "meta_adset_evaluation"
CAMPAIGN_CONFIG_DAG_ID = "facebook_campaign_config_update"
CONFIG_GCS_PREFIX = "facebook_campaign_config_update"
MAX_ADSETS_PER_CAMPAIGN_WORKER = 10
ALLOWED_CAMPAIGN_IDS_VARIABLE = "meta_adset_evaluation_campaign_ids"
DEFAULT_ALLOWED_CAMPAIGN_IDS = "52535307578056"


PRELOAD_CAMPAIGN_COMMAND = """\
set -euo pipefail
SOURCE='{{ dag_run.conf.get("source", "facebook") }}'
CAMPAIGN_ID='{{ dag_run.conf.get("campaign_id", "") }}'
REPORT_DATE='{{ dag_run.conf.get("date", "") }}'

if [[ -z "$CAMPAIGN_ID" ]]; then
  CAMPAIGN_ID="${META_ADSET_EVALUATION_DEFAULT_CAMPAIGN_ID:-}"
fi
if [[ -z "$CAMPAIGN_ID" ]]; then
  echo "no campaign_id configured; skipping adset evaluation"
  exit 0
fi

ARGS=(--mode preload-campaign --source "$SOURCE" --campaign-id "$CAMPAIGN_ID")
if [[ -n "$REPORT_DATE" ]]; then
  ARGS+=(--date "$REPORT_DATE")
fi

exec python -m meta_adset_evaluation_agent "${ARGS[@]}"
"""


def dag_conf_values(conf: dict[str, Any]) -> tuple[str, str, str]:
    source = str(conf.get("source") or "facebook").strip() or "facebook"
    campaign_id = str(conf.get("campaign_id") or "").strip()
    report_date = str(conf.get("date") or "").strip()
    return source, campaign_id, report_date


def evaluate_adset_command(
    adset_id: str,
    *,
    source: str = "facebook",
    campaign_id: str = "",
    report_date: str = "",
) -> str:
    return budget_adset_command(
        adset_id,
        mode="increase-budget",
        source=source,
        campaign_id=campaign_id,
        report_date=report_date,
    )


def set_budget_adset_command(
    adset_id: str,
    *,
    source: str = "facebook",
    campaign_id: str = "",
    report_date: str = "",
) -> str:
    return budget_adset_command(
        adset_id,
        mode="set-budget",
        source=source,
        campaign_id=campaign_id,
        report_date=report_date,
    )


def evaluate_campaign_adsets_command(
    campaign_id: str,
    adset_ids: list[str],
    *,
    source: str = "facebook",
    report_date: str = "",
) -> str:
    return budget_campaign_command(
        campaign_id,
        adset_ids,
        mode="increase-budget",
        source=source,
        report_date=report_date,
    )


def budget_campaign_command(
    campaign_id: str,
    adset_ids: list[str],
    *,
    mode: str,
    source: str = "facebook",
    report_date: str = "",
) -> str:
    quoted_campaign_id = shlex.quote(campaign_id)
    quoted_adset_ids = shlex.quote(",".join(adset_ids))
    quoted_mode = shlex.quote(mode)
    quoted_source = shlex.quote(source)
    quoted_report_date = shlex.quote(report_date)
    return f"""\
set -euo pipefail
SOURCE={quoted_source}
REPORT_DATE={quoted_report_date}
CAMPAIGN_ID={quoted_campaign_id}
ADSET_IDS={quoted_adset_ids}
MODE={quoted_mode}

if [[ -z "$CAMPAIGN_ID" ]]; then
  echo "no campaign_id configured; skipping adset evaluation"
  exit 0
fi
if [[ -z "$ADSET_IDS" ]]; then
  echo "no adset_ids configured; skipping adset evaluation"
  exit 0
fi

ARGS=(--mode "$MODE" --source "$SOURCE" --campaign-id "$CAMPAIGN_ID" --adset-ids "$ADSET_IDS")
if [[ -n "$REPORT_DATE" ]]; then
  ARGS+=(--date "$REPORT_DATE")
fi

exec python -m meta_adset_evaluation_agent "${{ARGS[@]}}"
"""


def budget_adset_command(
    adset_id: str,
    *,
    mode: str,
    source: str = "facebook",
    campaign_id: str = "",
    report_date: str = "",
) -> str:
    quoted_adset_id = shlex.quote(adset_id)
    quoted_mode = shlex.quote(mode)
    quoted_source = shlex.quote(source)
    quoted_report_date = shlex.quote(report_date)
    if campaign_id:
        campaign_id_line = f"CAMPAIGN_ID={shlex.quote(campaign_id)}"
        campaign_id_fallback = ""
    else:
        campaign_id_line = 'CAMPAIGN_ID=""'
        campaign_id_fallback = """\
if [[ -z "$CAMPAIGN_ID" ]]; then
  CAMPAIGN_ID="${META_ADSET_EVALUATION_DEFAULT_CAMPAIGN_ID:-}"
fi

"""
    return f"""\
set -euo pipefail
SOURCE={quoted_source}
{campaign_id_line}
REPORT_DATE={quoted_report_date}
ADSET_ID={quoted_adset_id}
MODE={quoted_mode}

{campaign_id_fallback}if [[ -z "$CAMPAIGN_ID" ]]; then
  echo "no campaign_id configured; skipping adset evaluation"
  exit 0
fi
if [[ -z "$ADSET_ID" ]]; then
  echo "no adset_id configured; skipping adset evaluation"
  exit 0
fi

ARGS=(--mode "$MODE" --source "$SOURCE" --campaign-id "$CAMPAIGN_ID" --adset-id "$ADSET_ID")
if [[ -n "$REPORT_DATE" ]]; then
  ARGS+=(--date "$REPORT_DATE")
fi

exec python -m meta_adset_evaluation_agent "${{ARGS[@]}}"
"""


def adset_evaluation_command() -> str:
    return PRELOAD_CAMPAIGN_COMMAND


def preload_campaign_command() -> str:
    return PRELOAD_CAMPAIGN_COMMAND


def adset_ids_from_text(raw: str) -> list[str]:
    return list(dict.fromkeys(part.strip() for part in raw.split(",") if part.strip()))


def chunk_adset_ids(
    adset_ids: list[str],
    max_size: int = MAX_ADSETS_PER_CAMPAIGN_WORKER,
) -> list[list[str]]:
    if not adset_ids:
        return []
    return [adset_ids[index : index + max_size] for index in range(0, len(adset_ids), max_size)]


def split_campaign_adset_groups(
    groups: list[dict[str, Any]],
    max_adsets: int = MAX_ADSETS_PER_CAMPAIGN_WORKER,
) -> list[dict[str, Any]]:
    split: list[dict[str, Any]] = []
    for group in groups:
        campaign_id = str(group["campaign_id"])
        for adset_chunk in chunk_adset_ids(list(group["adset_ids"]), max_adsets):
            split.append({"campaign_id": campaign_id, "adset_ids": adset_chunk})
    return split


def allowed_campaign_ids() -> set[str]:
    try:
        from airflow.models import Variable  # type: ignore[import-not-found]

        raw = Variable.get(ALLOWED_CAMPAIGN_IDS_VARIABLE, default_var=DEFAULT_ALLOWED_CAMPAIGN_IDS)
    except Exception:
        raw = DEFAULT_ALLOWED_CAMPAIGN_IDS
    return set(adset_ids_from_text(str(raw)))


def filter_campaign_adset_groups(
    groups: list[dict[str, Any]],
    allowed_campaign_ids: set[str],
) -> list[dict[str, Any]]:
    if not allowed_campaign_ids:
        return []
    return [
        group
        for group in groups
        if str(group["campaign_id"]) in allowed_campaign_ids
    ]


def manual_campaign_adset_groups(conf: dict[str, Any]) -> list[dict[str, Any]]:
    campaign_id = str(conf.get("campaign_id") or "").strip()
    raw_adset_ids = str(conf.get("adset_ids") or conf.get("adset_id") or "").strip()
    adset_ids = adset_ids_from_text(raw_adset_ids)
    if not campaign_id or not adset_ids:
        return []
    return split_campaign_adset_groups([{"campaign_id": campaign_id, "adset_ids": adset_ids}])


def active_campaign_adset_groups(
    snapshot: dict[str, Any],
    *,
    allowed_campaign_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    flat = flatten_config_snapshot(snapshot)
    grouped: dict[str, list[str]] = {}
    for adset in active_adsets_from_flat(flat):
        campaign_id = str(adset.get("campaign_id") or "").strip()
        adset_id = str(adset.get("adset_id") or "").strip()
        if not campaign_id or not adset_id:
            continue
        grouped.setdefault(campaign_id, [])
        if adset_id not in grouped[campaign_id]:
            grouped[campaign_id].append(adset_id)
    groups = [
        {"campaign_id": campaign_id, "adset_ids": adset_ids}
        for campaign_id, adset_ids in sorted(grouped.items())
    ]
    groups = split_campaign_adset_groups(groups)
    if allowed_campaign_ids is not None:
        groups = filter_campaign_adset_groups(groups, allowed_campaign_ids)
    return groups


def latest_config_snapshot() -> dict[str, Any]:
    import google.auth  # type: ignore[import-not-found]
    from google.cloud import storage  # type: ignore[import-not-found]

    credentials, _project_id = google.auth.default()
    storage_client = storage.Client(credentials=credentials)
    _pointer_uri, pointer = read_latest_snapshot_pointer(storage_client, CONFIG_GCS_PREFIX)
    snapshot_uri = str(pointer["final_output"])
    snapshot = read_json_from_gcs(storage_client, snapshot_uri)
    return snapshot if isinstance(snapshot, dict) else {}


def adset_evaluation_conf_example() -> dict[str, str]:
    return {
        "source": "facebook",
        "campaign_id": "23800000000000000",
        "adset_ids": "23800000000000000,23800000000000001",
        "date": "2026-07-03",
    }


def build_active_adset_worker_plan(
    groups: list[dict[str, Any]],
    *,
    source: str = "facebook",
    report_date: str = "",
) -> list[dict[str, Any]]:
    plan: list[dict[str, Any]] = []
    for group in groups:
        campaign_id = str(group["campaign_id"])
        plan.append(
            {
                "name": campaign_id,
                "arguments": [
                    evaluate_campaign_adsets_command(
                        campaign_id,
                        list(group["adset_ids"]),
                        source=source,
                        report_date=report_date,
                    )
                ],
            }
        )
    return plan


@task
def adset_worker_arguments() -> list[list[str]]:
    return budget_worker_arguments("increase-budget")


@task
def active_adset_worker_plan() -> list[dict[str, Any]]:
    context = get_current_context()
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    source, _campaign_id, report_date = dag_conf_values(conf)
    groups = manual_campaign_adset_groups(conf)
    if not groups:
        groups = active_campaign_adset_groups(
            latest_config_snapshot(),
            allowed_campaign_ids=allowed_campaign_ids(),
        )
    return build_active_adset_worker_plan(groups, source=source, report_date=report_date)


@task
def set_budget_worker_arguments() -> list[list[str]]:
    return budget_worker_arguments("set-budget")


def budget_worker_arguments(mode: str) -> list[list[str]]:
    context = get_current_context()
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    source, campaign_id, report_date = dag_conf_values(conf)
    raw_adset_ids = str(conf.get("adset_ids") or conf.get("adset_id") or "")
    if not raw_adset_ids:
        try:
            from airflow.models import Variable  # type: ignore[import-not-found]

            raw_adset_ids = Variable.get("meta_adset_evaluation_adset_id", default_var="")
        except Exception:
            raw_adset_ids = ""
    return [
        [
            budget_adset_command(
                adset_id,
                mode=mode,
                source=source,
                campaign_id=campaign_id,
                report_date=report_date,
            )
        ]
        for adset_id in adset_ids_from_text(raw_adset_ids)
    ]


@dag(
    dag_id=DAG_ID,
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 7, 1, 0, 0, tz=REPORT_TIMEZONE),
    catchup=False,
    tags=["meta", "adset", "evaluation", "agent"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    doc_md=__doc__,
)
def meta_adset_evaluation():
    wait_for_campaign_config = ExternalTaskSensor(
        task_id="wait_for_facebook_campaign_config_update",
        external_dag_id=CAMPAIGN_CONFIG_DAG_ID,
        external_task_id=None,
        execution_date_fn=campaign_config_logical_date,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=3 * 60 * 60,
    )
    worker_plan = active_adset_worker_plan()
    workers = meta_adset_evaluation_pod_partial(
        task_id="evaluate_campaign_adsets",
        cmds=["bash", "-lc"],
        map_index_template="{{ name }}",
    ).expand_kwargs(worker_plan)
    apply_budget_increases = meta_adset_evaluation_pod(
        task_id="apply_budget_increases",
        cmds=["python", "-m", "meta_adset_evaluation_agent.apply_budget_changes"],
        arguments=["--budget-change-type", "increase_budget"],
    )
    wait_for_campaign_config >> workers >> apply_budget_increases


meta_adset_evaluation()


@dag(
    dag_id="meta_adset_set_budget_evaluation",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2026, 7, 1, 0, 0, tz=REPORT_TIMEZONE),
    catchup=False,
    tags=["meta", "adset", "evaluation", "agent", "set-budget"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    doc_md=__doc__,
)
def meta_adset_set_budget_evaluation():
    preload = meta_adset_evaluation_pod(
        task_id="preload_campaign",
        cmds=["bash", "-lc"],
        arguments=[PRELOAD_CAMPAIGN_COMMAND],
    )
    worker_args = set_budget_worker_arguments()
    workers = meta_adset_evaluation_pod_partial(task_id="set_budget_adset", cmds=["bash", "-lc"]).expand(
        arguments=worker_args
    )
    preload >> workers


meta_adset_set_budget_evaluation()
