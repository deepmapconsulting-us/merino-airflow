from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

import pendulum

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str((REPO / "airflow" / "dags").resolve()))
sys.path.insert(0, str((REPO / "airflow" / "module" / "meta").resolve()))


def load_dag_module():
    spec = importlib.util.spec_from_file_location(
        "adset_creation_dag_for_test",
        REPO / "airflow" / "dags" / "adset_creation.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class AdsetCreationDagTest(unittest.TestCase):
    def test_planned_date_from_context_uses_manual_override(self) -> None:
        module = load_dag_module()
        dag_run = type("DagRun", (), {"conf": {"planned_date": "2026-07-02"}})()

        self.assertEqual(module.planned_date_from_context({"dag_run": dag_run}), "2026-07-02")

    def test_planned_date_from_context_uses_logical_date(self) -> None:
        module = load_dag_module()
        logical_date = pendulum.datetime(2026, 7, 2, 23, 0, tz="America/Los_Angeles")

        self.assertEqual(module.planned_date_from_context({"logical_date": logical_date}), "2026-07-02")

    def test_dry_run_from_conf_accepts_bool_and_text(self) -> None:
        module = load_dag_module()

        self.assertTrue(module.dry_run_from_conf({"dry_run": True}))
        self.assertTrue(module.dry_run_from_conf({"dry_run": "true"}))
        self.assertFalse(module.dry_run_from_conf({"dry_run": ""}))


if __name__ == "__main__":
    unittest.main()
