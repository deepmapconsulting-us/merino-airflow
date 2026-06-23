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
    def test_incremental_queries_use_interval_with_overlap(self) -> None:
        module = load_dag_module()
        interval_start = pendulum.datetime(2026, 6, 14, 12, 0, tz="UTC")
        interval_end = pendulum.datetime(2026, 6, 15, 0, 0, tz="UTC")

        queries = module.shopify_incremental_queries(
            data_interval_start=interval_start,
            data_interval_end=interval_end,
        )

        self.assertEqual(
            queries["customer_query"],
            "updated_at:>=2026-06-14T11:30:00Z updated_at:<2026-06-15T00:00:00Z",
        )
        self.assertEqual(
            queries["order_query"],
            "updated_at:>=2026-06-14T11:30:00Z updated_at:<2026-06-15T00:00:00Z financial_status:paid",
        )

    def test_import_command_uses_query_overrides_and_legacy_dates(self) -> None:
        dag_path = REPO / "airflow" / "dags" / "shopify_import.py"
        source = dag_path.read_text(encoding="utf-8")
        self.assertIn('FROM_DATE=\'{{ dag_run.conf.get("from_date", "") }}\'', source)
        self.assertIn('ARGS+=(--customer-query "$CUSTOMER_QUERY" --order-query "$ORDER_QUERY")', source)
        self.assertIn('ARGS+=(--from-date "$FROM_DATE" --to-date "$TO_DATE")', source)

    def test_dag_schedule_is_every_twelve_hours(self) -> None:
        dag_path = REPO / "airflow" / "dags" / "shopify_import.py"
        source = dag_path.read_text(encoding="utf-8")
        self.assertIn('schedule="0 */12 * * *"', source)
