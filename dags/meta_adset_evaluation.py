"""Run the Meta adset evaluation agent with one preload pod and per-adset workers.

Manual trigger config:

```json
{
  "source": "facebook",
  "campaign_id": "23800000000000000",
  "adset_ids": "23800000000000000,23800000000000001",
  "date": "2026-07-03"
}
```

The preload pod fetches current-day campaign adset performance through
meta-ads-mcp and caches campaign/day evidence in Redis for 30 minutes. Worker
pods then evaluate one adset each from the shared cache. Scheduled runs can use
Airflow Variables `meta_adset_evaluation_campaign_id` and
`meta_adset_evaluation_adset_id`; the adset variable can be comma-separated.
Without those variables, the scheduled pod
exits cleanly without work.
"""

from __future__ import annotations

from datetime import timedelta
import shlex

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag, get_current_context, task  # type: ignore[import-not-found]

from meta_adset_evaluation_k8s import meta_adset_evaluation_pod, meta_adset_evaluation_pod_partial
from meta_gcs import REPORT_TIMEZONE

DAG_ID = "meta_adset_evaluation"


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

exec python -m meta_adset_evaluation_agent "${{ARGS[@]}}"
"""


def evaluate_adset_command(adset_id: str) -> str:
    quoted_adset_id = shlex.quote(adset_id)
    return f"""\
set -euo pipefail
SOURCE='{{{{ dag_run.conf.get("source", "facebook") }}}}'
CAMPAIGN_ID='{{{{ dag_run.conf.get("campaign_id", "") }}}}'
REPORT_DATE='{{{{ dag_run.conf.get("date", "") }}}}'
ADSET_ID={quoted_adset_id}

if [[ -z "$CAMPAIGN_ID" ]]; then
  CAMPAIGN_ID="${{META_ADSET_EVALUATION_DEFAULT_CAMPAIGN_ID:-}}"
fi

if [[ -z "$CAMPAIGN_ID" ]]; then
  echo "no campaign_id configured; skipping adset evaluation"
  exit 0
fi
if [[ -z "$ADSET_ID" ]]; then
  echo "no adset_id configured; skipping adset evaluation"
  exit 0
fi

ARGS=(--mode evaluate --source "$SOURCE" --campaign-id "$CAMPAIGN_ID" --adset-id "$ADSET_ID")
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


def adset_evaluation_conf_example() -> dict[str, str]:
    return {
        "source": "facebook",
        "campaign_id": "23800000000000000",
        "adset_ids": "23800000000000000,23800000000000001",
        "date": "2026-07-03",
    }


@task
def adset_worker_arguments() -> list[list[str]]:
    context = get_current_context()
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    raw_adset_ids = str(conf.get("adset_ids") or conf.get("adset_id") or "")
    if not raw_adset_ids:
        try:
            from airflow.models import Variable  # type: ignore[import-not-found]

            raw_adset_ids = Variable.get("meta_adset_evaluation_adset_id", default_var="")
        except Exception:
            raw_adset_ids = ""
    return [[evaluate_adset_command(adset_id)] for adset_id in adset_ids_from_text(raw_adset_ids)]


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
    preload = meta_adset_evaluation_pod(
        task_id="preload_campaign",
        cmds=["bash", "-lc"],
        arguments=[PRELOAD_CAMPAIGN_COMMAND],
    )
    worker_args = adset_worker_arguments()
    workers = meta_adset_evaluation_pod_partial(task_id="evaluate_adset", cmds=["bash", "-lc"]).expand(
        arguments=worker_args
    )
    preload >> workers


meta_adset_evaluation()
