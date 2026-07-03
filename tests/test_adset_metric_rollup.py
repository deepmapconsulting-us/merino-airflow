from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path
from typing import Any

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs import adset_metric_rollup  # noqa: E402  # type: ignore[import-not-found]


class FakeCursor:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.executed: list[tuple[str, Any]] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows.pop(0) if self.rows else None


class FakeConnection:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.cursor_obj = FakeCursor(rows)
        self.committed = False

    def cursor(self) -> FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.committed = True


class AdsetMetricRollupTest(unittest.TestCase):
    def test_latest_partition_reads_max_report_date(self) -> None:
        conn = FakeConnection([(date(2026, 7, 1),)])

        self.assertEqual(adset_metric_rollup.latest_adset_metric_partition(conn), date(2026, 7, 1))
        self.assertIn("SELECT MAX(report_date)", conn.cursor_obj.executed[0][0])

    def test_rollup_sql_uses_purchase_metrics_and_window_ratios(self) -> None:
        sql = adset_metric_rollup.adset_metric_rollup_sql()

        self.assertIn("marketing.meta_adset_metric_rollup_daily", sql)
        self.assertIn("MAX(COALESCE(s.spend, 0))::numeric AS spend", sql)
        self.assertIn("MAX(COALESCE((s.actions ->> 'purchase')::numeric, 0)) AS purchase_count", sql)
        self.assertIn("MAX(COALESCE((s.action_values ->> 'purchase')::numeric, 0)) AS purchase_value", sql)
        self.assertIn("s.report_date BETWEEN p.partition_date - INTERVAL '29 days' AND p.partition_date", sql)
        self.assertIn("WHERE d.report_date >= p.partition_date - INTERVAL '6 days'", sql)
        self.assertIn("SUM(d.clicks) / NULLIF(SUM(d.impressions), 0) * 100", sql)
        self.assertIn("SUM(d.spend) / NULLIF(SUM(d.purchase_count), 0)", sql)
        self.assertIn("SUM(d.purchase_value) / NULLIF(SUM(d.spend), 0)", sql)
        self.assertIn("ON CONFLICT (partition_date, source_account_id, campaign_id, adset_id)", sql)

    def test_refresh_uses_explicit_partition_and_returns_counts(self) -> None:
        conn = FakeConnection(
            [
                (
                    date(2026, 7, 1),
                    date(2026, 6, 2),
                    date(2026, 6, 25),
                    date(2026, 7, 1),
                    12,
                )
            ]
        )

        result = adset_metric_rollup.refresh_adset_metric_rollup(conn, partition_date="2026-07-01")

        self.assertTrue(conn.committed)
        self.assertEqual(result["partition_date"], "2026-07-01")
        self.assertEqual(result["source_window_start_30d"], "2026-06-02")
        self.assertEqual(result["source_window_start_7d"], "2026-06-25")
        self.assertEqual(result["source_window_end"], "2026-07-01")
        self.assertEqual(result["row_count"], 12)
        self.assertEqual(conn.cursor_obj.executed[0][1], {"partition_date": date(2026, 7, 1)})

    def test_refresh_skips_when_source_has_no_partitions(self) -> None:
        conn = FakeConnection([(None,)])

        result = adset_metric_rollup.refresh_adset_metric_rollup(conn)

        self.assertFalse(conn.committed)
        self.assertEqual(result["partition_date"], None)
        self.assertEqual(result["row_count"], 0)
        self.assertIn("skipped", result)


if __name__ == "__main__":
    unittest.main()
