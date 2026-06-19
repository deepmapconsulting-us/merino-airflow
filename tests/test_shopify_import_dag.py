from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path

import pendulum

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str((REPO / "airflow" / "dags").resolve()))


def load_dag_module():
    mock_k8s = types.ModuleType("shopify_k8s")
    mock_k8s.shopify_import_pod = lambda **kwargs: None
    sys.modules["shopify_k8s"] = mock_k8s

    spec = importlib.util.spec_from_file_location(
        "shopify_import_dag_for_test",
        REPO / "airflow" / "dags" / "shopify_import.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ShopifyImportDagTest(unittest.TestCase):
    def test_scheduled_run_uses_two_day_window(self) -> None:
        module = load_dag_module()
        logical_date = pendulum.datetime(2026, 6, 14, 12, 0, tz="UTC")

        args = module.shopify_run_arguments(logical_date=logical_date, dag_run_conf={})

        self.assertEqual(
            args,
            [
                "--from-date",
                "2026-06-13",
                "--to-date",
                "2026-06-14",
                "--partition-date",
                "2026-06-14",
            ],
        )

    def test_manual_conf_overrides_dates_and_adds_overwrite(self) -> None:
        module = load_dag_module()
        logical_date = pendulum.datetime(2026, 6, 14, 12, 0, tz="UTC")

        args = module.shopify_run_arguments(
            logical_date=logical_date,
            dag_run_conf={
                "from_date": "2026-06-01",
                "to_date": "2026-06-10",
                "partition_date": "2026-06-10",
                "overwrite": "true",
            },
        )

        self.assertEqual(
            args,
            [
                "--from-date",
                "2026-06-01",
                "--to-date",
                "2026-06-10",
                "--partition-date",
                "2026-06-10",
                "--overwrite",
            ],
        )

    def test_dag_schedule_is_every_twelve_hours(self) -> None:
        dag_path = REPO / "airflow" / "dags" / "shopify_import.py"
        source = dag_path.read_text(encoding="utf-8")
        self.assertIn('schedule="0 */12 * * *"', source)
