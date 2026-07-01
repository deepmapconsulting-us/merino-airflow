from __future__ import annotations

import importlib.util
import sys
import unittest
from datetime import date
from pathlib import Path

import pendulum

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str((REPO / "airflow" / "dags").resolve()))
sys.path.insert(0, str((REPO / "airflow" / "module" / "ga4").resolve()))


def load_dag_module():
    spec = importlib.util.spec_from_file_location(
        "ga4_us_session_behavior_dag_for_test",
        REPO / "airflow" / "dags" / "ga4_us_session_behavior.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class GA4USSessionBehaviorDagTest(unittest.TestCase):
    def test_default_refresh_plan_runs_intraday_only_before_finalized_hour(self) -> None:
        module = load_dag_module()
        logical_date = pendulum.datetime(2026, 6, 14, 20, 0, tz="America/Los_Angeles")

        self.assertEqual(
            module.ga4_default_refresh_plan(logical_date),
            [
                {
                    "report_date": "2026-06-14",
                    "source_table_type": "intraday",
                },
            ],
        )

    def test_default_refresh_plan_adds_two_day_old_finalized_at_end_of_day(self) -> None:
        module = load_dag_module()
        logical_date = pendulum.datetime(2026, 6, 14, 22, 0, tz="America/Los_Angeles")

        self.assertEqual(
            module.ga4_default_refresh_plan(logical_date),
            [
                {
                    "report_date": "2026-06-14",
                    "source_table_type": "intraday",
                },
                {
                    "report_date": "2026-06-12",
                    "source_table_type": "finalized",
                },
            ],
        )

    def test_report_dates_for_run_uses_backfill_conf(self) -> None:
        module = load_dag_module()

        self.assertEqual(
            module.ga4_report_dates_for_run(
                logical_date=pendulum.datetime(2026, 6, 14, 8, 0, tz="America/Los_Angeles"),
                dag_run_conf={
                    "backfill_start": "2026-05-28",
                    "backfill_end": "2026-06-01",
                },
            ),
            [
                {"report_date": "2026-05-28", "source_table_type": "finalized"},
                {"report_date": "2026-05-29", "source_table_type": "finalized"},
                {"report_date": "2026-05-30", "source_table_type": "finalized"},
                {"report_date": "2026-05-31", "source_table_type": "finalized"},
                {"report_date": "2026-06-01", "source_table_type": "finalized"},
            ],
        )

    def test_resolve_ga4_source_table_uses_requested_intraday_table(self) -> None:
        module = load_dag_module()

        def table_exists(_client: object, table_id: str) -> bool:
            return table_id.endswith("events_intraday_20260613")

        self.assertEqual(
            module.resolve_ga4_source_table(
                client=object(),
                report_date=date(2026, 6, 13),
                source_table_type="intraday",
                table_exists=table_exists,
            ),
            (
                "merino-agent.analytics_370932876.events_intraday_20260613",
                "intraday",
            ),
        )

    def test_resolve_ga4_source_table_uses_requested_finalized_table(self) -> None:
        module = load_dag_module()

        def table_exists(_client: object, table_id: str) -> bool:
            return table_id.endswith("events_20260613")

        self.assertEqual(
            module.resolve_ga4_source_table(
                client=object(),
                report_date=date(2026, 6, 13),
                source_table_type="finalized",
                table_exists=table_exists,
            ),
            (
                "merino-agent.analytics_370932876.events_20260613",
                "finalized",
            ),
        )

    def test_refresh_skip_reason_only_requires_source_table(self) -> None:
        module = load_dag_module()

        reason = module.ga4_refresh_skip_reason(
            report_date=date(2026, 6, 12),
            resolved_source=(
                "merino-agent.analytics_370932876.events_20260612",
                "finalized",
            ),
        )

        self.assertIsNone(reason)

    def test_refresh_report_date_writes_all_us_session_aggregates(self) -> None:
        module = load_dag_module()

        merge_calls: list[tuple[str, int, date]] = []

        def merge_recorder(name: str):
            def record(rows, report_date):
                merge_calls.append((name, len(rows), report_date))

            return record

        module.merge_us_session_daily = merge_recorder("us_session_daily")
        module.merge_us_session_daily_state = merge_recorder("us_session_daily_state")
        module.merge_us_session_daily_hour = merge_recorder("us_session_daily_hour")
        module.merge_us_session_daily_state_hour = merge_recorder("us_session_daily_state_hour")

        class FakeRow:
            def __init__(self, payload: dict[str, object]) -> None:
                self.payload = payload

            def items(self):
                return self.payload.items()

        class FakeQueryJob:
            def __init__(self, rows: list[dict[str, object]]) -> None:
                self.rows = rows

            def result(self):
                return [FakeRow(row) for row in self.rows]

        class FakeClient:
            def __init__(self) -> None:
                self.queries: list[str] = []
                self.row_batches = [
                    [{"metric": "us_daily"}],
                    [{"state": "California"}, {"state": "New York"}],
                    [{"pacific_hour": 1}, {"pacific_hour": 2}, {"pacific_hour": 3}],
                    [{"state": "California", "pacific_hour": 1}],
                ]

            def get_table(self, table_id: str) -> object:
                if table_id.endswith("events_intraday_20260614"):
                    return object()
                raise AssertionError(f"unexpected table lookup: {table_id}")

            def query(self, sql: str) -> FakeQueryJob:
                self.queries.append(sql)
                return FakeQueryJob(self.row_batches[len(self.queries) - 1])

        client = FakeClient()

        result = module.refresh_ga4_us_session_report_date(
            client,
            date(2026, 6, 14),
            source_table_type="intraday",
        )

        self.assertEqual(len(client.queries), 4)
        self.assertEqual(result["source_table_type"], "intraday")
        self.assertEqual(result["us_session_daily_rows"], 1)
        self.assertEqual(result["us_session_daily_state_rows"], 2)
        self.assertEqual(result["us_session_daily_hour_rows"], 3)
        self.assertEqual(result["us_session_daily_state_hour_rows"], 1)
        self.assertIn(("us_session_daily", 1, date(2026, 6, 14)), merge_calls)
        self.assertIn(("us_session_daily_state", 2, date(2026, 6, 14)), merge_calls)
        self.assertIn(("us_session_daily_hour", 3, date(2026, 6, 14)), merge_calls)
        self.assertIn(("us_session_daily_state_hour", 1, date(2026, 6, 14)), merge_calls)


if __name__ == "__main__":
    unittest.main()
