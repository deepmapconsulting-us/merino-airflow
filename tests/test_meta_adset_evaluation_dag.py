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

        def expand_kwargs(self, mapped_kwargs: object) -> FakeOperator:
            return FakeOperator(expand_kwargs=mapped_kwargs, partial_kwargs=self.kwargs)

    mock_k8s.meta_adset_evaluation_pod = lambda **kwargs: FakeOperator(**kwargs)
    mock_k8s.meta_adset_evaluation_pod_partial = lambda **kwargs: FakePartial(**kwargs)
    sys.modules["meta_adset_evaluation_k8s"] = mock_k8s

    mock_meta_gcs = types.ModuleType("meta_gcs")
    mock_meta_gcs.REPORT_TIMEZONE = "America/Los_Angeles"
    mock_meta_gcs.campaign_config_logical_date = lambda *_args, **_kwargs: None
    mock_meta_gcs.read_json_from_gcs = lambda *_args, **_kwargs: {}
    mock_meta_gcs.read_latest_snapshot_pointer = lambda *_args, **_kwargs: ("", {"final_output": "gs://bucket/snapshot.json"})
    sys.modules["meta_gcs"] = mock_meta_gcs

    mock_airflow = types.ModuleType("airflow")
    mock_airflow_sdk = types.ModuleType("airflow.sdk")
    mock_airflow_sdk.dag = lambda **_kwargs: (lambda fn: fn)
    mock_airflow_sdk.task = lambda fn: fn
    mock_airflow_sdk.get_current_context = lambda: {"dag_run": types.SimpleNamespace(conf={})}
    sys.modules.setdefault("airflow", mock_airflow)
    sys.modules["airflow.sdk"] = mock_airflow_sdk

    mock_sensor_module = types.ModuleType("airflow.providers.standard.sensors.external_task")

    class FakeExternalTaskSensor:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

        def __rshift__(self, other: object) -> object:
            return other

    mock_sensor_module.ExternalTaskSensor = FakeExternalTaskSensor
    sys.modules["airflow.providers.standard.sensors.external_task"] = mock_sensor_module

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
        self.assertIn('exec python -m meta_adset_evaluation_agent "${ARGS[@]}"', command)

    def test_worker_command_uses_one_adset_id(self) -> None:
        module = load_dag_module()

        command = module.evaluate_adset_command("987")

        self.assertIn("ADSET_ID=987", command)
        self.assertIn("SOURCE=facebook", command)
        self.assertIn("MODE=increase-budget", command)
        self.assertIn('ARGS=(--mode "$MODE" --source "$SOURCE" --campaign-id "$CAMPAIGN_ID" --adset-id "$ADSET_ID")', command)
        self.assertIn('ARGS+=(--date "$REPORT_DATE")', command)
        self.assertNotIn("dag_run.conf", command)

    def test_campaign_worker_command_uses_comma_separated_adsets(self) -> None:
        module = load_dag_module()

        command = module.evaluate_campaign_adsets_command("2381", ["987", "654"])

        self.assertIn("CAMPAIGN_ID=2381", command)
        self.assertIn("ADSET_IDS=987,654", command)
        self.assertIn("SOURCE=facebook", command)
        self.assertIn("MODE=increase-budget", command)
        self.assertIn('ARGS=(--mode "$MODE" --source "$SOURCE" --campaign-id "$CAMPAIGN_ID" --adset-ids "$ADSET_IDS")', command)
        self.assertNotIn("dag_run.conf", command)

    def test_active_campaign_groups_filter_active_campaign_and_adsets(self) -> None:
        module = load_dag_module()

        snapshot = {
            "accounts": {
                "act_1": {
                    "campaigns": [
                        {
                            "id": "camp_1",
                            "status": "ACTIVE",
                            "adsets": [
                                {"id": "adset_1", "campaign_id": "camp_1", "status": "ACTIVE"},
                                {"id": "adset_2", "campaign_id": "camp_1", "status": "PAUSED"},
                            ],
                        },
                        {
                            "id": "camp_2",
                            "status": "PAUSED",
                            "adsets": [
                                {"id": "adset_3", "campaign_id": "camp_2", "status": "ACTIVE"},
                            ],
                        },
                    ]
                }
            }
        }

        self.assertEqual(
            module.active_campaign_adset_groups(snapshot),
            [{"campaign_id": "camp_1", "adset_ids": ["adset_1"]}],
        )

    def test_active_campaign_groups_respect_allowed_campaign_ids(self) -> None:
        module = load_dag_module()

        snapshot = {
            "accounts": {
                "act_1": {
                    "campaigns": [
                        {
                            "id": "52535307578056",
                            "status": "ACTIVE",
                            "adsets": [
                                {"id": "adset_1", "campaign_id": "52535307578056", "status": "ACTIVE"},
                            ],
                        },
                        {
                            "id": "other_campaign",
                            "status": "ACTIVE",
                            "adsets": [
                                {"id": "adset_2", "campaign_id": "other_campaign", "status": "ACTIVE"},
                            ],
                        },
                    ]
                }
            }
        }

        groups = module.active_campaign_adset_groups(
            snapshot,
            allowed_campaign_ids={"52535307578056"},
        )

        self.assertEqual(
            groups,
            [{"campaign_id": "52535307578056", "adset_ids": ["adset_1"]}],
        )

    def test_manual_conf_campaign_group_overrides_snapshot_discovery(self) -> None:
        module = load_dag_module()

        groups = module.manual_campaign_adset_groups({"campaign_id": "camp_1", "adset_ids": "a,b,a"})

        self.assertEqual(groups, [{"campaign_id": "camp_1", "adset_ids": ["a", "b"]}])

    def test_campaign_groups_split_when_more_than_ten_adsets(self) -> None:
        module = load_dag_module()

        adset_ids = [f"adset_{index}" for index in range(1, 24)]
        snapshot = {
            "accounts": {
                "act_1": {
                    "campaigns": [
                        {
                            "id": "camp_1",
                            "status": "ACTIVE",
                            "adsets": [
                                {"id": adset_id, "campaign_id": "camp_1", "status": "ACTIVE"}
                                for adset_id in adset_ids
                            ],
                        }
                    ]
                }
            }
        }

        groups = module.active_campaign_adset_groups(snapshot)

        self.assertEqual(len(groups), 3)
        self.assertEqual(groups[0]["adset_ids"], adset_ids[:10])
        self.assertEqual(groups[1]["adset_ids"], adset_ids[10:20])
        self.assertEqual(groups[2]["adset_ids"], adset_ids[20:])
        self.assertTrue(all(group["campaign_id"] == "camp_1" for group in groups))

    def test_manual_conf_splits_large_adset_list(self) -> None:
        module = load_dag_module()

        adset_ids = [f"adset_{index}" for index in range(1, 12)]
        groups = module.manual_campaign_adset_groups(
            {"campaign_id": "camp_1", "adset_ids": ",".join(adset_ids)}
        )

        self.assertEqual(
            groups,
            [
                {"campaign_id": "camp_1", "adset_ids": adset_ids[:10]},
                {"campaign_id": "camp_1", "adset_ids": adset_ids[10:]},
            ],
        )

    def test_worker_plan_uses_campaign_id_pod_name_for_single_batch(self) -> None:
        module = load_dag_module()

        plan = module.build_active_adset_worker_plan(
            [{"campaign_id": "52535307578056", "adset_ids": ["adset_1", "adset_2"]}],
            source="facebook",
        )

        self.assertEqual(plan[0]["name"], "52535307578056")
        self.assertNotIn("campaign_label", plan[0])
        self.assertIn("52535307578056", plan[0]["arguments"][0])
        self.assertIn("adset_1,adset_2", plan[0]["arguments"][0])
        self.assertIn("SOURCE=facebook", plan[0]["arguments"][0])
        self.assertNotIn("dag_run.conf", plan[0]["arguments"][0])

    def test_worker_plan_uses_campaign_id_for_split_batches(self) -> None:
        module = load_dag_module()

        adset_ids = [f"adset_{index}" for index in range(1, 12)]
        groups = module.manual_campaign_adset_groups(
            {"campaign_id": "52535307578056", "adset_ids": ",".join(adset_ids)}
        )
        plan = module.build_active_adset_worker_plan(groups)

        self.assertEqual(
            [entry["name"] for entry in plan],
            ["52535307578056", "52535307578056"],
        )

    def test_set_budget_worker_command_uses_set_budget_mode(self) -> None:
        module = load_dag_module()

        command = module.set_budget_adset_command("987")

        self.assertIn("ADSET_ID=987", command)
        self.assertIn("SOURCE=facebook", command)
        self.assertIn("MODE=set-budget", command)
        self.assertIn('ARGS+=(--date "$REPORT_DATE")', command)
        self.assertNotIn("dag_run.conf", command)

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
        self.assertIn('dag_id="meta_adset_set_budget_evaluation"', source)
        self.assertIn('schedule="0 0 * * *"', source)

    def test_dag_splits_preload_and_mapped_workers(self) -> None:
        dag_path = REPO / "airflow" / "dags" / "meta_adset_evaluation.py"
        source = dag_path.read_text(encoding="utf-8")

        self.assertIn('task_id="preload_campaign"', source)
        self.assertIn('task_id="evaluate_campaign_adsets"', source)
        self.assertIn('task_id="set_budget_adset"', source)
        self.assertIn('task_id="apply_budget_increases"', source)
        self.assertIn('map_index_template="{{ name }}"', source)
        self.assertIn(".expand_kwargs(", source)
        self.assertIn("wait_for_campaign_config >> workers >> apply_budget_increases", source)

    def test_pod_env_passes_inference_core_and_langfuse_settings(self) -> None:
        module = load_k8s_module()

        env_by_name = {env.name: env.value for env in module.meta_adset_evaluation_env()}

        self.assertIn("{{ var.value.get('openai_api_key', '') }}", env_by_name["INFERENCE_CONFIG__OPENAI_API_KEY"])
        self.assertEqual(env_by_name["INFERENCE_CONFIG__INFERENCE_PROJECT_NAME"], "meta_adset_evaluation_agent")
        self.assertEqual(env_by_name["PROMPT_LABEL_CONFIG__PROMPT_BACKEND"], "langfuse")
        self.assertIn("adset_budget_langfuse_public_key", env_by_name["LANGFUSE_CONFIG__LANGFUSE_PUBLIC_KEY"])
        self.assertIn("adset_budget_langfuse_secret_key", env_by_name["LANGFUSE_CONFIG__LANGFUSE_SECRET_KEY"])
        self.assertEqual(env_by_name["GLOBAL_ADSET_BUDGET_MAX"], "{{ var.value.get('global_adset_budget_max', '') }}")
        self.assertEqual(
            env_by_name["META_ADSET_EVALUATION_BUDGET_SPEND_THRESHOLD"],
            "{{ var.value.get('meta_adset_evaluation_budget_spend_threshold', '0.85') }}",
        )

    def test_pod_partial_preserves_log_settings_for_mapping(self) -> None:
        module = load_k8s_module()

        partial = module.meta_adset_evaluation_pod_partial(
            task_id="evaluate_adset",
            cmds=["bash", "-lc"],
            map_index_template="{{ name }}",
        )

        self.assertTrue(partial["get_logs"])
        self.assertEqual(partial["task_id"], "evaluate_adset")
        self.assertEqual(partial["map_index_template"], "{{ name }}")


if __name__ == "__main__":
    unittest.main()
