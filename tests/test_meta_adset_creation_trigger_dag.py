from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def load_dag_module() -> tuple[object, list[dict[str, object]]]:
    pod_calls: list[dict[str, object]] = []

    mock_k8s = types.ModuleType("meta_adset_evaluation_k8s")

    def fake_pod(**kwargs: object) -> dict[str, object]:
        pod_calls.append(dict(kwargs))
        return dict(kwargs)

    mock_k8s.meta_adset_evaluation_pod = fake_pod
    sys.modules["meta_adset_evaluation_k8s"] = mock_k8s

    mock_meta_gcs = types.ModuleType("meta_gcs")
    mock_meta_gcs.REPORT_TIMEZONE = "America/Los_Angeles"
    sys.modules["meta_gcs"] = mock_meta_gcs

    mock_airflow = types.ModuleType("airflow")
    mock_airflow_sdk = types.ModuleType("airflow.sdk")
    mock_airflow_sdk.dag = lambda **_kwargs: (lambda fn: fn)
    sys.modules.setdefault("airflow", mock_airflow)
    sys.modules["airflow.sdk"] = mock_airflow_sdk

    spec = importlib.util.spec_from_file_location(
        "meta_adset_creation_trigger_dag_for_test",
        REPO / "airflow" / "dags" / "meta_adset_creation_trigger.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, pod_calls


class MetaAdsetCreationTriggerDagTest(unittest.TestCase):
    def test_dag_id_and_schedule(self) -> None:
        module, _pod_calls = load_dag_module()

        self.assertEqual(module.DAG_ID, "meta_adset_creation_trigger")
        self.assertEqual(module.DAG_SCHEDULE, "*/10 * * * *")

    def test_budget_command_args(self) -> None:
        module, _pod_calls = load_dag_module()

        args = module.apply_budget_command_args("increase_budget")

        self.assertEqual(args["cmds"], ["python", "-m", "meta_adset_evaluation_agent.apply_budget_changes"])
        self.assertEqual(args["arguments"], ["--budget-change-type", "increase_budget"])

    def test_trigger_dag_runs_all_queue_consumers(self) -> None:
        _module, pod_calls = load_dag_module()

        calls_by_task_id = {str(call["task_id"]): call for call in pod_calls}
        self.assertEqual(
            set(calls_by_task_id),
            {
                "apply_increase_budget_changes",
                "apply_set_budget_changes",
                "apply_ad_status_schedules",
                "apply_adset_splits",
                "queue_ad_retirements",
            },
        )
        self.assertEqual(
            calls_by_task_id["apply_increase_budget_changes"]["arguments"],
            ["--budget-change-type", "increase_budget"],
        )
        self.assertEqual(
            calls_by_task_id["apply_set_budget_changes"]["arguments"],
            ["--budget-change-type", "set_budget"],
        )
        self.assertEqual(
            calls_by_task_id["apply_ad_status_schedules"]["cmds"],
            ["python", "-m", "meta_adset_evaluation_agent.apply_ad_status_schedules"],
        )
        self.assertEqual(
            calls_by_task_id["apply_adset_splits"]["cmds"],
            ["python", "-m", "meta_adset_evaluation_agent.apply_adset_splits"],
        )
        self.assertEqual(
            calls_by_task_id["queue_ad_retirements"]["cmds"],
            ["python", "-m", "meta_adset_evaluation_agent.queue_ad_retirements"],
        )


if __name__ == "__main__":
    unittest.main()
