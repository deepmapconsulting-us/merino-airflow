"""Manual job to create the Chinese creative media analysis schema prompt.

Calls media-analysis-mcp ``/api/v1/translate/chinese-schema-prompt``. The MCP
does the OpenAI translation and creates the Langfuse production prompt only when
it does not already exist, unless ``force`` is true.

Manual DAG run config can override:

```json
{
  "source_prompt_name": "media_analysis_mcp/video_analysis_schema",
  "target_prompt_name": "media_analysis_mcp/创意媒体分析快照结构",
  "translation_prompt_name": "media_analysis_mcp/translate_schema_to_chineese",
  "input_content": "Representative creative analysis examples...",
  "model": "gpt-5.5",
  "force": false,
  "dry_run": false
}
```
"""

from __future__ import annotations

import json
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag, task  # type: ignore[import-not-found]

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
if MODULE_PATH.exists():
    sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs.media_analysis import (  # noqa: E402  # type: ignore[import-not-found]
    mcp_gateway_token,
    media_analysis_base_url,
    translate_chinese_schema_prompt,
)

DAG_ID = "tranlsate_chinese_scehma"
REPORT_TIMEZONE = "America/Los_Angeles"

DEFAULT_SOURCE_PROMPT_NAME = "media_analysis_mcp/video_analysis_schema"
DEFAULT_TARGET_PROMPT_NAME = "media_analysis_mcp/创意媒体分析快照结构"
DEFAULT_TRANSLATION_PROMPT_NAME = "media_analysis_mcp/translate_schema_to_chineese"


def _bool_value(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _conf_value(conf: dict[str, Any], key: str, default: str = "") -> str:
    value = conf.get(key)
    if value is None:
        return default
    return str(value).strip()


@dag(
    dag_id=DAG_ID,
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, 0, 0, tz=REPORT_TIMEZONE),
    catchup=False,
    max_active_runs=1,
    tags=["meta", "creative", "media-analysis", "manual"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    doc_md=__doc__,
)
def tranlsate_chinese_scehma():
    @task
    def run_translation() -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run and isinstance(dag_run.conf, dict) else {}
        payload = translate_chinese_schema_prompt(
            gateway_token=mcp_gateway_token(),
            base_url=media_analysis_base_url(),
            source_prompt_name=_conf_value(
                conf,
                "source_prompt_name",
                DEFAULT_SOURCE_PROMPT_NAME,
            ),
            target_prompt_name=_conf_value(
                conf,
                "target_prompt_name",
                DEFAULT_TARGET_PROMPT_NAME,
            ),
            translation_prompt_name=_conf_value(
                conf,
                "translation_prompt_name",
                DEFAULT_TRANSLATION_PROMPT_NAME,
            ),
            input_content=_conf_value(conf, "input_content"),
            model=_conf_value(conf, "model"),
            force=_bool_value(conf.get("force"), default=False),
            dry_run=_bool_value(conf.get("dry_run"), default=False),
        )
        print(f"{DAG_ID}: MCP response:")
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        return payload

    run_translation()


tranlsate_chinese_scehma()
