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
        "shopify_order_transactions_dag_for_test",
        REPO / "airflow" / "dags" / "shopify_order_transactions.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ShopifyOrderTransactionsDagTest(unittest.TestCase):
    def test_transaction_query_uses_updated_at_with_two_day_overlap(self) -> None:
        module = load_dag_module()
        interval_start = pendulum.datetime(2026, 7, 2, 6, 0, tz="UTC")
        interval_end = pendulum.datetime(2026, 7, 2, 12, 0, tz="UTC")

        query = module.transaction_order_query(
            data_interval_start=interval_start,
            data_interval_end=interval_end,
        )

        self.assertEqual(
            query,
            "updated_at:>=2026-06-30T06:00:00Z updated_at:<2026-07-02T12:00:00Z",
        )

    def test_transaction_command_runs_importer(self) -> None:
        module = load_dag_module()

        self.assertIn("python3 shopify/import_order_transactions.py", module.SHOPIFY_TRANSACTION_COMMAND)
        self.assertIn("--date-field updated_at", module.SHOPIFY_TRANSACTION_COMMAND)
        self.assertNotIn("financial_status:paid", module.SHOPIFY_TRANSACTION_COMMAND)

    def test_dag_allows_only_one_active_run(self) -> None:
        dag_path = REPO / "airflow" / "dags" / "shopify_order_transactions.py"
        source = dag_path.read_text(encoding="utf-8")
        self.assertIn("max_active_runs=1", source)
