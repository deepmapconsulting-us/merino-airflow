"""KubernetesPodOperator settings shared by Amazon ingestion DAGs."""

from __future__ import annotations

from kubernetes.client import models as k8s

try:
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
except ImportError:  # pragma: no cover - older provider layout
    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (  # type: ignore[no-redef]
        KubernetesPodOperator,
    )

AMAZON_IMAGE = "us-west2-docker.pkg.dev/merino-agent/merino/merino-amazon-jobs:0.1.1"
AIRFLOW_NAMESPACE = "airflow"
TASK_RUNNER_KSA = "merino-airflow-task-runner"
POSTGRES_CONN_ID = "merino_analytics"


def amazon_pod_env() -> list[k8s.V1EnvVar]:
    """Return templated runtime configuration without logging secret values."""
    return [
        k8s.V1EnvVar(name="PYTHONUNBUFFERED", value="1"),
        k8s.V1EnvVar(
            name="AMAZON_DATABASE_URL",
            value=f"{{{{ conn.{POSTGRES_CONN_ID}.get_uri() }}}}",
        ),
        k8s.V1EnvVar(name="SP_API_CLIENT_ID", value="{{ var.value.sp_api_client_id }}"),
        k8s.V1EnvVar(
            name="SP_API_CLIENT_SECRET",
            value="{{ var.value.sp_api_client_secret }}",
        ),
        k8s.V1EnvVar(
            name="SP_API_REFRESH_TOKEN",
            value="{{ var.value.sp_api_refresh_token }}",
        ),
        k8s.V1EnvVar(
            name="AMAZON_ACCOUNT_KEY",
            value="{{ var.value.amazon_account_key }}",
        ),
        k8s.V1EnvVar(
            name="AMAZON_SELLER_ID",
            value="{{ var.value.get('sp_api_seller_id', var.value.get('amazon_seller_id', '')) }}",
        ),
        k8s.V1EnvVar(
            name="AMAZON_SELLER_DISPLAY_NAME",
            value="{{ var.value.amazon_seller_display_name }}",
        ),
        k8s.V1EnvVar(
            name="AMAZON_BRAND_KEY",
            value="{{ var.value.amazon_brand_key }}",
        ),
        k8s.V1EnvVar(
            name="AMAZON_BRAND_NAME",
            value="{{ var.value.amazon_brand_name }}",
        ),
        k8s.V1EnvVar(
            name="AMAZON_ADS_CLIENT_ID",
            value="{{ var.value.get('amazon_ads_client_id', '') }}",
        ),
        k8s.V1EnvVar(
            name="AMAZON_ADS_CLIENT_SECRET",
            value="{{ var.value.get('amazon_ads_client_secret', '') }}",
        ),
        k8s.V1EnvVar(
            name="AMAZON_ADS_REFRESH_TOKEN",
            value="{{ var.value.get('amazon_ads_refresh_token', '') }}",
        ),
        k8s.V1EnvVar(
            name="AMAZON_ADS_PROFILE_ID_US",
            value="{{ var.value.get('amazon_ads_profile_id_us', '') }}",
        ),
        k8s.V1EnvVar(
            name="AMAZON_ADS_PROFILE_ID_CA",
            value="{{ var.value.get('amazon_ads_profile_id_ca', '') }}",
        ),
        k8s.V1EnvVar(
            name="AMAZON_ADS_PROFILE_ID_MX",
            value="{{ var.value.get('amazon_ads_profile_id_mx', '') }}",
        ),
        k8s.V1EnvVar(
            name="AMAZON_ADS_PROFILE_ID_BR",
            value="{{ var.value.get('amazon_ads_profile_id_br', '') }}",
        ),
        k8s.V1EnvVar(
            name="AMAZON_ADS_PROFILE_ID_AU",
            value="{{ var.value.get('amazon_ads_profile_id_au', '') }}",
        ),
    ]


def amazon_pod(
    *,
    task_id: str,
    cmds: list[str] | None = None,
    arguments: list[str] | None = None,
) -> KubernetesPodOperator:
    """Launch one Amazon runtime pod."""
    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        namespace=AIRFLOW_NAMESPACE,
        image=AMAZON_IMAGE,
        image_pull_policy="IfNotPresent",
        service_account_name=TASK_RUNNER_KSA,
        in_cluster=True,
        get_logs=True,
        on_finish_action="delete_pod",
        is_delete_operator_pod=True,
        startup_timeout_seconds=600,
        cmds=cmds or ["merino-amazon-jobs"],
        arguments=arguments or [],
        env_vars=amazon_pod_env(),
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "1Gi"},
            limits={"cpu": "2000m", "memory": "2Gi"},
        ),
    )
