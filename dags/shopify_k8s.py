"""KubernetesPodOperator settings for Shopify batch jobs."""

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
SHOPIFY_IMAGE = f"{REGION}-docker.pkg.dev/{PROJECT_ID}/merino/merino-shopify-cli:0.1.1"
AIRFLOW_NAMESPACE = "airflow"
TASK_RUNNER_KSA = "merino-airflow-task-runner"
POSTGRES_CONN_ID = "merino_analytics"
POSTGRES_DB = "merino-shopify"
GOOGLE_GEOCODING_VARIABLE = "google_geocoding_api_key"
GOOGLE_GEOCODING_GSM_SECRET = "airflow-variables-google_geocoding_api_key"
SHOPIFY_AUTH_SECRET = "shopify-cli-store-auth"
SHOPIFY_AUTH_SECRET_KEY = "config.json"
SHOPIFY_AUTH_MOUNT = "/root/.config/shopify-cli-store-nodejs"

SHOPIFY_AUTH_VOLUME = k8s.V1Volume(
    name="shopify-cli-auth",
    secret=k8s.V1SecretVolumeSource(
        secret_name=SHOPIFY_AUTH_SECRET,
        items=[k8s.V1KeyToPath(key=SHOPIFY_AUTH_SECRET_KEY, path=SHOPIFY_AUTH_SECRET_KEY)],
    ),
)
SHOPIFY_AUTH_VOLUME_MOUNT = k8s.V1VolumeMount(
    name="shopify-cli-auth",
    mount_path=SHOPIFY_AUTH_MOUNT,
    read_only=True,
)


def shopify_pod_env() -> dict[str, str]:
    """Env vars injected into the Shopify CLI batch image."""
    return {
        "PYTHONUNBUFFERED": "1",
        "POSTGRES_HOST": f"{{{{ conn.{POSTGRES_CONN_ID}.host }}}}",
        "POSTGRES_PORT": f"{{{{ conn.{POSTGRES_CONN_ID}.port or 5432 }}}}",
        "POSTGRES_USER": f"{{{{ conn.{POSTGRES_CONN_ID}.login }}}}",
        "POSTGRES_PASSWORD": f"{{{{ conn.{POSTGRES_CONN_ID}.password }}}}",
        "POSTGRES_DB": POSTGRES_DB,
        "POSTGRES_CONNECT_TIMEOUT": "30",
        "GOOGLE_GEOCODING_API_KEY": f"{{{{ var.value.get('{GOOGLE_GEOCODING_VARIABLE}', default='') }}}}",
    }


def shopify_import_pod(
    *,
    task_id: str,
    cmds: list[str] | None = None,
    arguments: list[str] | None = None,
) -> KubernetesPodOperator:
    """Launch one Shopify batch pod with store auth + Postgres env."""
    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        namespace=AIRFLOW_NAMESPACE,
        image=SHOPIFY_IMAGE,
        image_pull_policy="IfNotPresent",
        service_account_name=TASK_RUNNER_KSA,
        in_cluster=True,
        get_logs=True,
        on_finish_action="delete_pod",
        is_delete_operator_pod=True,
        startup_timeout_seconds=600,
        cmds=cmds or ["bash", "scripts/run_shopify_all.sh"],
        arguments=arguments or [],
        env_vars=shopify_pod_env(),
        volumes=[SHOPIFY_AUTH_VOLUME],
        volume_mounts=[SHOPIFY_AUTH_VOLUME_MOUNT],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "1Gi"},
            limits={"cpu": "2000m", "memory": "2Gi"},
        ),
    )
