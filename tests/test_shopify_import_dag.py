from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import jinja2
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
    def test_incremental_queries_use_24_hour_order_window_and_transaction_overlap(self) -> None:
        module = load_dag_module()
        interval_start = pendulum.datetime(2026, 7, 2, 6, 0, tz="UTC")
        interval_end = pendulum.datetime(2026, 7, 2, 12, 0, tz="UTC")

        queries = module.shopify_incremental_queries(
            data_interval_start=interval_start,
            data_interval_end=interval_end,
        )

        self.assertEqual(
            queries["order_query"],
            "updated_at:>=2026-07-01T12:00:00Z updated_at:<2026-07-02T12:00:00Z financial_status:paid",
        )
        self.assertEqual(
            queries["transaction_query"],
            "updated_at:>=2026-06-30T06:00:00Z updated_at:<2026-07-02T12:00:00Z",
        )

    def test_import_command_runs_orders_transactions_and_inventory(self) -> None:
        dag_path = REPO / "airflow" / "dags" / "shopify_import.py"
        source = dag_path.read_text(encoding="utf-8")
        self.assertIn('FROM_DATE=\'{{ conf.get("from_date", "") }}\'', source)
        self.assertIn("data_interval_start is defined", source)
        self.assertIn("dag_run.run_after", source)
        self.assertIn('ARGS+=(--order-query "$ORDER_QUERY" --transaction-query "$TRANSACTION_QUERY")', source)
        self.assertNotIn('ARGS+=(--customer-query "$CUSTOMER_QUERY" --order-query "$ORDER_QUERY")', source)
        self.assertIn('ARGS+=(--from-date "$FROM_DATE" --to-date "$TO_DATE")', source)
        self.assertIn('ARGS+=(--include-customers)', source)
        self.assertIn("run_shopify_all.sh", source)

    def test_import_command_renders_without_data_interval(self) -> None:
        """Airflow 3 manual triggers omit data_interval_* from the Jinja context."""
        module = load_dag_module()
        env = jinja2.Environment(undefined=jinja2.StrictUndefined)
        env.globals["macros"] = SimpleNamespace(timedelta=timedelta)
        template = env.from_string(module.SHOPIFY_IMPORT_COMMAND)
        run_after = pendulum.datetime(2026, 7, 11, 19, 49, tz="UTC")

        rendered = template.render(
            dag_run=SimpleNamespace(conf={"from_date": "2026-06-01", "to_date": "2026-06-14"}, run_after=run_after),
        )

        self.assertIn('FROM_DATE=\'2026-06-01\'', rendered)
        self.assertIn('TO_DATE=\'2026-06-14\'', rendered)
        self.assertIn('PARTITION_DATE=\'2026-06-14\'', rendered)
        self.assertIn('ARGS+=(--from-date "$FROM_DATE" --to-date "$TO_DATE")', rendered)

    def test_import_command_defaults_to_24_hour_order_query_without_data_interval(self) -> None:
        module = load_dag_module()
        env = jinja2.Environment(undefined=jinja2.StrictUndefined)
        env.globals["macros"] = SimpleNamespace(timedelta=timedelta)
        template = env.from_string(module.SHOPIFY_IMPORT_COMMAND)
        run_after = pendulum.datetime(2026, 7, 11, 19, 49, tz="UTC")

        rendered = template.render(dag_run=SimpleNamespace(conf={}, run_after=run_after))

        self.assertIn(
            "ORDER_QUERY='updated_at:>=2026-07-10T19:49:00Z "
            "updated_at:<2026-07-11T19:49:00Z financial_status:paid'",
            rendered,
        )
        self.assertIn('ARGS+=(--order-query "$ORDER_QUERY" --transaction-query "$TRANSACTION_QUERY")', rendered)

    def test_dag_schedule_is_every_six_hours(self) -> None:
        dag_path = REPO / "airflow" / "dags" / "shopify_import.py"
        source = dag_path.read_text(encoding="utf-8")
        self.assertIn('schedule="0 */6 * * *"', source)
        self.assertIn("max_active_runs=1", source)
