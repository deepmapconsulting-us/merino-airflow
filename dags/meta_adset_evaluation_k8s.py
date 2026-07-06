"""KubernetesPodOperator settings for Meta adset evaluation agent jobs."""

from __future__ import annotations

from kubernetes.client import models as k8s

try:
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
except ImportError:  # pragma: no cover - older provider layout
    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (  # type: ignore[no-redef]
        KubernetesPodOperator,
    )

PROJECT_ID = "merino-agent"
REGION = "us-west2"
META_ADSET_EVALUATION_IMAGE = (
    f"{REGION}-docker.pkg.dev/{PROJECT_ID}/merino/merino-meta-adset-evaluation-agent:0.1.0"
)
AIRFLOW_NAMESPACE = "airflow"
TASK_RUNNER_KSA = "merino-airflow-task-runner"
REDIS_CONN_ID = "merino_redis"
POSTGRES_CONN_ID = "merino_analytics"
DEFAULT_META_ADS_MCP_URL = "http://meta-ads-mcp.merino-mcp.svc.cluster.local:8080/mcp/"


def meta_adset_evaluation_env() -> list[k8s.V1EnvVar]:
    return [
        k8s.V1EnvVar(name="PYTHONUNBUFFERED", value="1"),
        k8s.V1EnvVar(name="META_ADS_MCP_URL", value=DEFAULT_META_ADS_MCP_URL),
        k8s.V1EnvVar(name="META_ACCESS_TOKEN", value="{{ var.value.meta_access_token }}"),
        k8s.V1EnvVar(name="META_MCP_GATEWAY_TOKEN", value="{{ var.value.meta_mcp_gateway_token }}"),
        k8s.V1EnvVar(name="OPENAI_API_KEY", value="{{ var.value.get('openai_api_key', '') }}"),
        k8s.V1EnvVar(name="INFERENCE_CONFIG__OPENAI_API_KEY", value="{{ var.value.get('openai_api_key', '') }}"),
        k8s.V1EnvVar(
            name="INFERENCE_CONFIG__OPENAI_MODEL",
            value="{{ var.value.get('meta_adset_evaluation_openai_model', 'gpt-5.5') }}",
        ),
        k8s.V1EnvVar(name="INFERENCE_CONFIG__INFERENCE_PROVIDER", value="openai"),
        k8s.V1EnvVar(name="INFERENCE_CONFIG__INFERENCE_PROJECT_NAME", value="meta_adset_evaluation_agent"),
        k8s.V1EnvVar(name="PROMPT_LABEL_CONFIG__PROMPT_BACKEND", value="langfuse"),
        k8s.V1EnvVar(
            name="PROMPT_LABEL_CONFIG__PROMPT_LABEL",
            value="{{ var.value.get('meta_adset_evaluation_prompt_label', 'production') }}",
        ),
        k8s.V1EnvVar(
            name="LANGFUSE_PUBLIC_KEY",
            value="{{ var.value.get('adset_budget_langfuse_public_key', '') }}",
        ),
        k8s.V1EnvVar(
            name="LANGFUSE_SECRET_KEY",
            value="{{ var.value.get('adset_budget_langfuse_secret_key', '') }}",
        ),
        k8s.V1EnvVar(
            name="LANGFUSE_BASE_URL",
            value="{{ var.value.get('adset_budget_langfuse_base_url', 'https://langfuse.merino-aiagent.com') }}",
        ),
        k8s.V1EnvVar(
            name="LANGFUSE_CONFIG__LANGFUSE_PUBLIC_KEY",
            value="{{ var.value.get('adset_budget_langfuse_public_key', '') }}",
        ),
        k8s.V1EnvVar(
            name="LANGFUSE_CONFIG__LANGFUSE_SECRET_KEY",
            value="{{ var.value.get('adset_budget_langfuse_secret_key', '') }}",
        ),
        k8s.V1EnvVar(
            name="LANGFUSE_CONFIG__LANGFUSE_BASE_URL",
            value="{{ var.value.get('adset_budget_langfuse_base_url', 'https://langfuse.merino-aiagent.com') }}",
        ),
        k8s.V1EnvVar(
            name="META_ADSET_EVALUATION_DEFAULT_CAMPAIGN_ID",
            value="{{ var.value.get('meta_adset_evaluation_campaign_id', '') }}",
        ),
        k8s.V1EnvVar(
            name="META_ADSET_EVALUATION_DEFAULT_ADSET_ID",
            value="{{ var.value.get('meta_adset_evaluation_adset_id', '') }}",
        ),
        k8s.V1EnvVar(
            name="GLOBAL_ADSET_BUDGET_MAX",
            value="{{ var.value.get('global_adset_budget_max', '') }}",
        ),
        k8s.V1EnvVar(
            name="META_ADSET_EVALUATION_BUDGET_SPEND_THRESHOLD",
            value="{{ var.value.get('meta_adset_evaluation_budget_spend_threshold', '0.85') }}",
        ),
        k8s.V1EnvVar(name="MCP_REDIS_HOST", value=f"{{{{ conn.{REDIS_CONN_ID}.host }}}}"),
        k8s.V1EnvVar(name="MCP_REDIS_PORT", value=f"{{{{ conn.{REDIS_CONN_ID}.port or 6379 }}}}"),
        k8s.V1EnvVar(name="MCP_REDIS_PASSWORD", value=f"{{{{ conn.{REDIS_CONN_ID}.password or '' }}}}"),
        k8s.V1EnvVar(name="MCP_REDIS_DB", value=f"{{{{ conn.{REDIS_CONN_ID}.schema or 0 }}}}"),
        k8s.V1EnvVar(name="POSTGRES_HOST", value=f"{{{{ conn.{POSTGRES_CONN_ID}.host }}}}"),
        k8s.V1EnvVar(name="POSTGRES_PORT", value=f"{{{{ conn.{POSTGRES_CONN_ID}.port or 5432 }}}}"),
        k8s.V1EnvVar(name="POSTGRES_USER", value=f"{{{{ conn.{POSTGRES_CONN_ID}.login }}}}"),
        k8s.V1EnvVar(name="POSTGRES_PASSWORD", value=f"{{{{ conn.{POSTGRES_CONN_ID}.password }}}}"),
        k8s.V1EnvVar(name="POSTGRES_DB", value=f"{{{{ conn.{POSTGRES_CONN_ID}.schema }}}}"),
        k8s.V1EnvVar(name="META_ADSET_EVALUATION_WRITE_DATABASE", value="true"),
    ]


def meta_adset_evaluation_pod(
    *,
    task_id: str,
    cmds: list[str] | None = None,
    arguments: list[str] | None = None,
) -> KubernetesPodOperator:
    return KubernetesPodOperator(**meta_adset_evaluation_pod_kwargs(task_id=task_id, cmds=cmds, arguments=arguments))


def meta_adset_evaluation_pod_partial(
    *,
    task_id: str,
    cmds: list[str] | None = None,
    map_index_template: str | None = None,
) -> object:
    kwargs = meta_adset_evaluation_pod_kwargs(task_id=task_id, cmds=cmds, arguments=None)
    if map_index_template is not None:
        kwargs["map_index_template"] = map_index_template
    return KubernetesPodOperator.partial(**kwargs)


def meta_adset_evaluation_pod_kwargs(
    *,
    task_id: str,
    cmds: list[str] | None = None,
    arguments: list[str] | None = None,
) -> dict[str, object]:
    kwargs: dict[str, object] = dict(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        namespace=AIRFLOW_NAMESPACE,
        image=META_ADSET_EVALUATION_IMAGE,
        image_pull_policy="IfNotPresent",
        service_account_name=TASK_RUNNER_KSA,
        in_cluster=True,
        get_logs=True,
        on_finish_action="delete_pod",
        is_delete_operator_pod=True,
        startup_timeout_seconds=300,
        cmds=cmds or ["python", "-m", "meta_adset_evaluation_agent"],
        env_vars=meta_adset_evaluation_env(),
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "250m", "memory": "512Mi"},
            limits={"cpu": "1000m", "memory": "1Gi"},
        ),
    )
    if arguments is not None:
        kwargs["arguments"] = arguments
    return kwargs
