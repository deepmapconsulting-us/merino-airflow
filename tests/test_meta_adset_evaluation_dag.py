from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def load_dag_module():
    mock_k8s = types.ModuleType("meta_adset_evaluation_k8s")

    class FakeOperator:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

        def __rshift__(self, other: object) -> object:
            return other

    class FakePartial:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

        def expand(self, **kwargs: object) -> FakeOperator:
            return FakeOperator(expand_kwargs=kwargs, partial_kwargs=self.kwargs)

    mock_k8s.meta_adset_evaluation_pod = lambda **kwargs: FakeOperator(**kwargs)
    mock_k8s.meta_adset_evaluation_pod_partial = lambda **kwargs: FakePartial(**kwargs)
    sys.modules["meta_adset_evaluation_k8s"] = mock_k8s

    mock_meta_gcs = types.ModuleType("meta_gcs")
    mock_meta_gcs.REPORT_TIMEZONE = "America/Los_Angeles"
    sys.modules["meta_gcs"] = mock_meta_gcs

    mock_airflow = types.ModuleType("airflow")
    mock_airflow_sdk = types.ModuleType("airflow.sdk")
    mock_airflow_sdk.dag = lambda **_kwargs: (lambda fn: fn)
    mock_airflow_sdk.task = lambda fn: fn
    mock_airflow_sdk.get_current_context = lambda: {"dag_run": types.SimpleNamespace(conf={})}
    sys.modules.setdefault("airflow", mock_airflow)
    sys.modules["airflow.sdk"] = mock_airflow_sdk

    spec = importlib.util.spec_from_file_location(
        "meta_adset_evaluation_dag_for_test",
        REPO / "airflow" / "dags" / "meta_adset_evaluation.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_k8s_module():
    mock_kubernetes = types.ModuleType("kubernetes")
    mock_client = types.ModuleType("kubernetes.client")

    class FakeEnvVar:
        def __init__(self, name: str, value: str | None = None, **_kwargs: object) -> None:
            self.name = name
            self.value = value

    class FakeResources:
        def __init__(self, **_kwargs: object) -> None:
            pass

    mock_client.models = types.SimpleNamespace(V1EnvVar=FakeEnvVar, V1ResourceRequirements=FakeResources)
    mock_kubernetes.client = mock_client
    sys.modules["kubernetes"] = mock_kubernetes
    sys.modules["kubernetes.client"] = mock_client

    class FakeKubernetesPodOperator:
        def __new__(cls, **kwargs: object) -> dict[str, object]:
            return dict(kwargs)

        @classmethod
        def partial(cls, **kwargs: object) -> dict[str, object]:
            return dict(kwargs)

    mock_pod_module = types.ModuleType("airflow.providers.cncf.kubernetes.operators.pod")
    mock_pod_module.KubernetesPodOperator = FakeKubernetesPodOperator
    sys.modules["airflow.providers.cncf.kubernetes.operators.pod"] = mock_pod_module

    spec = importlib.util.spec_from_file_location(
        "meta_adset_evaluation_k8s_for_test",
        REPO / "airflow" / "dags" / "meta_adset_evaluation_k8s.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class MetaAdsetEvaluationDagTest(unittest.TestCase):
    def test_preload_command_uses_required_dag_conf_params(self) -> None:
        module = load_dag_module()

        command = module.preload_campaign_command()

        self.assertIn('CAMPAIGN_ID=\'{{ dag_run.conf.get("campaign_id", "") }}\'', command)
        self.assertIn('CAMPAIGN_ID="${META_ADSET_EVALUATION_DEFAULT_CAMPAIGN_ID:-}"', command)
        self.assertIn('ARGS=(--mode preload-campaign --source "$SOURCE" --campaign-id "$CAMPAIGN_ID")', command)
        self.assertIn('ARGS+=(--date "$REPORT_DATE")', command)

    def test_worker_command_uses_one_adset_id(self) -> None:
        module = load_dag_module()

        command = module.evaluate_adset_command("987")

        self.assertIn("ADSET_ID=987", command)
        self.assertIn('ARGS=(--mode evaluate --source "$SOURCE" --campaign-id "$CAMPAIGN_ID" --adset-id "$ADSET_ID")', command)
        self.assertIn('ARGS+=(--date "$REPORT_DATE")', command)

    def test_conf_example_documents_multi_adset_trigger(self) -> None:
        module = load_dag_module()

        self.assertEqual(
            module.adset_evaluation_conf_example(),
            {
                "source": "facebook",
                "campaign_id": "23800000000000000",
                "adset_ids": "23800000000000000,23800000000000001",
                "date": "2026-07-03",
            },
        )

    def test_dag_runs_hourly(self) -> None:
        dag_path = REPO / "airflow" / "dags" / "meta_adset_evaluation.py"
        source = dag_path.read_text(encoding="utf-8")

        self.assertIn('schedule="0 * * * *"', source)

    def test_dag_splits_preload_and_mapped_workers(self) -> None:
        dag_path = REPO / "airflow" / "dags" / "meta_adset_evaluation.py"
        source = dag_path.read_text(encoding="utf-8")

        self.assertIn('task_id="preload_campaign"', source)
        self.assertIn('task_id="evaluate_adset"', source)
        self.assertIn(".expand(", source)
        self.assertIn("preload >> workers", source)

    def test_pod_env_passes_inference_core_and_langfuse_settings(self) -> None:
        module = load_k8s_module()

        env_by_name = {env.name: env.value for env in module.meta_adset_evaluation_env()}

        self.assertIn("{{ var.value.get('openai_api_key', '') }}", env_by_name["INFERENCE_CONFIG__OPENAI_API_KEY"])
        self.assertEqual(env_by_name["INFERENCE_CONFIG__INFERENCE_PROJECT_NAME"], "meta_adset_evaluation_agent")
        self.assertEqual(env_by_name["PROMPT_LABEL_CONFIG__PROMPT_BACKEND"], "langfuse")
        self.assertIn("adset_budget_langfuse_public_key", env_by_name["LANGFUSE_CONFIG__LANGFUSE_PUBLIC_KEY"])
        self.assertIn("adset_budget_langfuse_secret_key", env_by_name["LANGFUSE_CONFIG__LANGFUSE_SECRET_KEY"])
        self.assertEqual(env_by_name["GLOBAL_ADSET_BUDGET_MAX"], "{{ var.value.get('global_adset_budget_max', '') }}")

    def test_pod_partial_preserves_log_settings_for_mapping(self) -> None:
        module = load_k8s_module()

        partial = module.meta_adset_evaluation_pod_partial(task_id="evaluate_adset", cmds=["bash", "-lc"])

        self.assertTrue(partial["get_logs"])
        self.assertEqual(partial["task_id"], "evaluate_adset")


if __name__ == "__main__":
    unittest.main()
