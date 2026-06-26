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
GOOGLE_GEOCODING_SECRET = "merino-airflow-google-geocoding-api-key"
GOOGLE_GEOCODING_SECRET_KEY = "api-key"
SHOPIFY_AUTH_SECRET = "shopify-cli-store-auth"
SHOPIFY_AUTH_SECRET_KEY = "config.json"
SHOPIFY_AUTH_MOUNT = "/root/.config/shopify-cli-store-nodejs"
# Shopify CLI rewrites config.json on load; mount the K8s secret elsewhere and copy into a writable dir.
SHOPIFY_AUTH_SECRET_MOUNT = "/mnt/shopify-cli-auth"

SHOPIFY_AUTH_SECRET_VOLUME = k8s.V1Volume(
    name="shopify-cli-auth",
    secret=k8s.V1SecretVolumeSource(
        secret_name=SHOPIFY_AUTH_SECRET,
        items=[k8s.V1KeyToPath(key=SHOPIFY_AUTH_SECRET_KEY, path=SHOPIFY_AUTH_SECRET_KEY)],
    ),
)
SHOPIFY_AUTH_CONFIG_VOLUME = k8s.V1Volume(
    name="shopify-cli-config",
    empty_dir=k8s.V1EmptyDirVolumeSource(),
)
SHOPIFY_AUTH_SECRET_VOLUME_MOUNT = k8s.V1VolumeMount(
    name="shopify-cli-auth",
    mount_path=SHOPIFY_AUTH_SECRET_MOUNT,
    read_only=True,
)
SHOPIFY_AUTH_CONFIG_VOLUME_MOUNT = k8s.V1VolumeMount(
    name="shopify-cli-config",
    mount_path=SHOPIFY_AUTH_MOUNT,
)
SHOPIFY_AUTH_INIT_CONTAINER = k8s.V1Container(
    name="shopify-cli-auth-init",
    image="busybox:1.36",
    command=[
        "sh",
        "-c",
        (
            f"mkdir -p {SHOPIFY_AUTH_MOUNT} && "
            f"cp {SHOPIFY_AUTH_SECRET_MOUNT}/{SHOPIFY_AUTH_SECRET_KEY} "
            f"{SHOPIFY_AUTH_MOUNT}/{SHOPIFY_AUTH_SECRET_KEY} && "
            f"chmod 600 {SHOPIFY_AUTH_MOUNT}/{SHOPIFY_AUTH_SECRET_KEY}"
        ),
    ],
    volume_mounts=[SHOPIFY_AUTH_SECRET_VOLUME_MOUNT, SHOPIFY_AUTH_CONFIG_VOLUME_MOUNT],
)


def shopify_pod_env() -> list[k8s.V1EnvVar]:
    """Env vars injected into the Shopify CLI batch image."""
    return [
        k8s.V1EnvVar(name="CI", value="true"),
        k8s.V1EnvVar(name="PYTHONUNBUFFERED", value="1"),
        k8s.V1EnvVar(name="POSTGRES_HOST", value=f"{{{{ conn.{POSTGRES_CONN_ID}.host }}}}"),
        k8s.V1EnvVar(name="POSTGRES_PORT", value=f"{{{{ conn.{POSTGRES_CONN_ID}.port or 5432 }}}}"),
        k8s.V1EnvVar(name="POSTGRES_USER", value=f"{{{{ conn.{POSTGRES_CONN_ID}.login }}}}"),
        k8s.V1EnvVar(name="POSTGRES_PASSWORD", value=f"{{{{ conn.{POSTGRES_CONN_ID}.password }}}}"),
        k8s.V1EnvVar(name="POSTGRES_DB", value=POSTGRES_DB),
        k8s.V1EnvVar(name="POSTGRES_CONNECT_TIMEOUT", value="30"),
        k8s.V1EnvVar(
            name="GOOGLE_GEOCODING_API_KEY",
            value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name=GOOGLE_GEOCODING_SECRET,
                    key=GOOGLE_GEOCODING_SECRET_KEY,
                ),
            ),
        ),
    ]


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
        init_containers=[SHOPIFY_AUTH_INIT_CONTAINER],
        volumes=[SHOPIFY_AUTH_SECRET_VOLUME, SHOPIFY_AUTH_CONFIG_VOLUME],
        volume_mounts=[SHOPIFY_AUTH_CONFIG_VOLUME_MOUNT],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "1Gi"},
            limits={"cpu": "2000m", "memory": "2Gi"},
        ),
    )
