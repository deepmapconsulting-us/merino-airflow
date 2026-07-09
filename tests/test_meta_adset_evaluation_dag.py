from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path

import pendulum

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

    class FakeTask:
        def __init__(self, name: str) -> None:
            self.task_id = name
            self.operator = True

        def __rshift__(self, other: object) -> object:
            return other

    def task_decorator(fn=None, **_kwargs: object):
        def task_factory(*_args: object, **_factory_kwargs: object) -> FakeTask:
            assert fn is not None
            return FakeTask(fn.__name__)

        if fn is not None:
            task_factory.__name__ = fn.__name__
            return task_factory
        return lambda inner: task_decorator(inner)

    task_decorator.branch = lambda fn: task_decorator(fn)
    mock_airflow_sdk.task = task_decorator
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

    def test_worker_arguments_include_campaign_id_command(self) -> None:
        module = load_dag_module()

        worker_args = module.build_active_adset_worker_arguments(
            [{"campaign_id": "52535307578056", "adset_ids": ["adset_1", "adset_2"]}],
            source="facebook",
        )

        command = worker_args[0][0]
        self.assertEqual(module.campaign_id_from_worker_command(command), "52535307578056")
        self.assertIn("adset_1,adset_2", command)
        self.assertIn("SOURCE=facebook", command)
        self.assertNotIn("dag_run.conf", command)

    def test_worker_arguments_split_batches_keep_campaign_id(self) -> None:
        module = load_dag_module()

        adset_ids = [f"adset_{index}" for index in range(1, 12)]
        groups = module.manual_campaign_adset_groups(
            {"campaign_id": "52535307578056", "adset_ids": ",".join(adset_ids)}
        )
        worker_args = module.build_active_adset_worker_arguments(groups)

        self.assertEqual(len(worker_args), 2)
        self.assertEqual(
            [module.campaign_id_from_worker_command(args[0]) for args in worker_args],
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

    def test_set_budget_campaign_worker_command_uses_campaign_batch(self) -> None:
        module = load_dag_module()

        worker_args = module.build_campaign_budget_worker_arguments(
            [{"campaign_id": "52535307578056", "adset_ids": ["111", "222"]}],
            mode="set-budget",
            source="facebook",
            report_date="2026-07-08",
        )
        command = worker_args[0][0]

        self.assertIn("CAMPAIGN_ID=52535307578056", command)
        self.assertIn("ADSET_IDS=111,222", command)
        self.assertIn("MODE=set-budget", command)
        self.assertIn("REPORT_DATE=2026-07-08", command)

    def test_preload_campaign_worker_arguments_deduplicates_campaigns(self) -> None:
        module = load_dag_module()

        worker_args = module.preload_campaign_worker_arguments(
            [
                {"campaign_id": "52535307578056", "adset_ids": ["111"]},
                {"campaign_id": "52535307578056", "adset_ids": ["222"]},
            ],
            source="facebook",
            report_date="2026-07-08",
        )

        self.assertEqual(len(worker_args), 1)
        command = worker_args[0][0]
        self.assertIn("CAMPAIGN_ID=52535307578056", command)
        self.assertIn("ARGS=(--mode preload-campaign", command)
        self.assertIn("REPORT_DATE=2026-07-08", command)

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

    def test_evaluation_mode_uses_pt_hour_for_scheduled_runs(self) -> None:
        module = load_dag_module()
        midnight = pendulum.datetime(2026, 7, 8, 0, 0, tz=module.REPORT_TIMEZONE)
        morning = pendulum.datetime(2026, 7, 8, 3, 0, tz=module.REPORT_TIMEZONE)

        self.assertEqual(module.evaluation_mode({}, midnight), "set_budget")
        self.assertEqual(module.evaluation_mode({}, morning), "increase_budget")
        self.assertEqual(module.evaluation_mode({"mode": "set_budget"}, morning), "set_budget")
        self.assertEqual(module.evaluation_mode({"mode": "increase_budget"}, midnight), "increase_budget")

    def test_dag_schedule_covers_midnight_and_daytime_pt(self) -> None:
        dag_path = REPO / "airflow" / "dags" / "meta_adset_evaluation.py"
        source = dag_path.read_text(encoding="utf-8")

        self.assertIn('DAG_SCHEDULE = "0 0,3-22 * * *"', source)
        self.assertIn("schedule=DAG_SCHEDULE", source)
        self.assertNotIn("meta_adset_set_budget_evaluation()", source)

    def test_dag_branches_increase_and_set_budget_flows(self) -> None:
        dag_path = REPO / "airflow" / "dags" / "meta_adset_evaluation.py"
        source = dag_path.read_text(encoding="utf-8")

        self.assertIn("choose_evaluation_flow", source)
        self.assertIn('task_id="preload_set_budget_campaign"', source)
        self.assertIn('task_id="evaluate_campaign_adsets"', source)
        self.assertIn('task_id="set_budget_adset"', source)
        self.assertIn('task_id="apply_increase_budget_changes"', source)
        self.assertIn('task_id="apply_set_budget_changes"', source)
        self.assertIn('task_id="generate_ad_status_schedule"', source)
        self.assertIn("EVALUATE_CAMPAIGN_MAP_INDEX_TEMPLATE", source)
        self.assertIn('.expand(arguments=increase_worker_args)', source)
        self.assertIn(".expand(arguments=preload_args)", source)
        self.assertIn("arguments=set_budget_worker_args", source)
        self.assertIn("arguments=schedule_worker_args", source)
        self.assertIn('mode="schedule-parameter"', source)
        self.assertIn("branch >> increase_worker_args >> increase_workers >> apply_increase_budget_changes", source)
        self.assertIn(">> apply_set_budget_changes\n            >> generate_ad_status_schedule", source)
        self.assertIn('arguments=["--budget-change-type", "increase_budget"]', source)
        self.assertIn('arguments=["--budget-change-type", "set_budget"]', source)
        self.assertIn('trigger_rule="none_failed_min_one_success"', source)

    def test_pod_env_passes_inference_core_and_langfuse_settings(self) -> None:
        module = load_k8s_module()

        env_by_name = {env.name: env.value for env in module.meta_adset_evaluation_env()}

        self.assertIn("{{ var.value.get('openai_api_key', '') }}", env_by_name["INFERENCE_CONFIG__OPENAI_API_KEY"])
        self.assertEqual(env_by_name["INFERENCE_CONFIG__INFERENCE_PROJECT_NAME"], "meta_adset_evaluation_agent")
        self.assertEqual(env_by_name["PROMPT_LABEL_CONFIG__PROMPT_BACKEND"], "langfuse")
        self.assertIn("adset_budget_langfuse_public_key", env_by_name["LANGFUSE_PUBLIC_KEY"])
        self.assertIn("adset_budget_langfuse_secret_key", env_by_name["LANGFUSE_SECRET_KEY"])
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
            map_index_template='{{ task.arguments[0] }}',
        )

        self.assertTrue(partial["get_logs"])
        self.assertEqual(partial["task_id"], "evaluate_adset")
        self.assertEqual(partial["map_index_template"], "{{ task.arguments[0] }}")


if __name__ == "__main__":
    unittest.main()
