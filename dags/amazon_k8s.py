"""KubernetesPodOperator settings shared by Amazon ingestion DAGs."""

from __future__ import annotations

from kubernetes.client import models as k8s

try:
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
except ImportError:  # pragma: no cover - older provider layout
    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (  # type: ignore[no-redef]
        KubernetesPodOperator,
    )

AMAZON_IMAGE = "us-west2-docker.pkg.dev/merino-agent/merino/merino-amazon-jobs:0.1.2"
AIRFLOW_NAMESPACE = "airflow"
TASK_RUNNER_KSA = "merino-airflow-task-runner"
POSTGRES_CONN_ID = "merino_analytics"
AMAZON_SP_API_POOL = "amazon_sp_api"
SP_API_LOCK_CMD = "merino-amazon-with-lock"
REDIS_HOST = "merino-mcp-redis.merino-mcp.svc.cluster.local"
REDIS_SECRET_NAME = "merino-mcp-redis-password"
REDIS_SECRET_KEY = "password"


def ensure_amazon_sp_api_pool() -> None:
    """Create the one-slot SP-API pool so overlapping Amazon DAGs queue."""
    try:
        from airflow.models.pool import Pool
    except ImportError:
        return
    try:
        Pool.create_or_update_pool(
            name=AMAZON_SP_API_POOL,
            slots=1,
            description="Serialize Amazon SP-API pods onto shared Reports/Orders quota.",
            include_deferred=False,
        )
    except TypeError:
        try:
            Pool.create_or_update_pool(
                name=AMAZON_SP_API_POOL,
                slots=1,
                description="Serialize Amazon SP-API pods onto shared Reports/Orders quota.",
            )
        except Exception:
            return
    except Exception:
        return


ensure_amazon_sp_api_pool()


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
        k8s.V1EnvVar(name="MERINO_REDIS_HOST", value=REDIS_HOST),
        k8s.V1EnvVar(name="MERINO_REDIS_PORT", value="6379"),
        k8s.V1EnvVar(name="MERINO_REDIS_DB", value="0"),
        k8s.V1EnvVar(
            name="MERINO_REDIS_PASSWORD",
            value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name=REDIS_SECRET_NAME,
                    key=REDIS_SECRET_KEY,
                ),
            ),
        ),
    ]


def amazon_pod(
    *,
    task_id: str,
    cmds: list[str] | None = None,
    arguments: list[str] | None = None,
    sp_api: bool = True,
) -> KubernetesPodOperator:
    """Launch one Amazon runtime pod.

    SP-API tasks take the `amazon_sp_api` pool (1 slot) and wrap the command
    with `merino-amazon-with-lock` so overlapping DAGs cannot burn the same
    Reports/Orders quota. Amazon Ads uses a different API and skips both.
    """
    command = list(cmds or ["merino-amazon-jobs"])
    operator_kwargs: dict[str, object] = {}
    if sp_api:
        command = [SP_API_LOCK_CMD, *command]
        operator_kwargs["pool"] = AMAZON_SP_API_POOL
        operator_kwargs["pool_slots"] = 1
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
        cmds=command,
        arguments=arguments or [],
        env_vars=amazon_pod_env(),
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "1Gi"},
            limits={"cpu": "2000m", "memory": "2Gi"},
        ),
        **operator_kwargs,
    )
