from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str((REPO / "airflow" / "dags").resolve()))
sys.path.insert(0, str((REPO / "airflow" / "module" / "meta").resolve()))


def load_dag_module():
    spec = importlib.util.spec_from_file_location(
        "meta_adset_metric_rollup_dag_for_test",
        REPO / "airflow" / "dags" / "meta_adset_metric_rollup.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class MetaAdsetMetricRollupDagTest(unittest.TestCase):
    def test_partition_date_from_conf_uses_manual_override(self) -> None:
        module = load_dag_module()

        self.assertEqual(
            module.partition_date_from_conf({"partition_date": "2026-07-01"}),
            "2026-07-01",
        )

    def test_partition_date_from_conf_allows_latest_partition_fallback(self) -> None:
        module = load_dag_module()

        self.assertIsNone(module.partition_date_from_conf({}))
        self.assertIsNone(module.partition_date_from_conf({"partition_date": ""}))
        self.assertIsNone(module.partition_date_from_conf(None))


if __name__ == "__main__":
    unittest.main()
