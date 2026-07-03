"""Compute rolling adset purchase-performance metrics from daily snapshots."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

POSTGRES_CONN_ID = "merino_analytics"
ADSET_METRIC_ROLLUP_TABLE = "marketing.meta_adset_metric_rollup_daily"


def parse_partition_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def latest_adset_metric_partition(conn: Any) -> date | None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT MAX(report_date)
            FROM marketing.meta_adset_daily_snapshot
            """
        )
        row = cursor.fetchone()
    return row[0] if row and row[0] is not None else None


def adset_metric_rollup_sql() -> str:
    return f"""
WITH params AS (
    SELECT %(partition_date)s::date AS partition_date
),
source_daily AS (
    SELECT
        s.report_date,
        s.source_account_id,
        MAX(s.source_account_name) AS source_account_name,
        MAX(s.currency_code) AS currency_code,
        MAX(s.timezone_name) AS timezone_name,
        s.campaign_id,
        MAX(s.campaign_name) AS campaign_name,
        s.adset_id,
        MAX(s.adset_name) AS adset_name,
        MAX(COALESCE(s.spend, 0))::numeric AS spend,
        MAX(COALESCE(s.impressions, 0))::numeric AS impressions,
        MAX(COALESCE(s.clicks, 0))::numeric AS clicks,
        MAX(COALESCE((s.actions ->> 'purchase')::numeric, 0)) AS purchase_count,
        MAX(COALESCE((s.action_values ->> 'purchase')::numeric, 0)) AS purchase_value
    FROM marketing.meta_adset_daily_snapshot s
    JOIN params p
      ON s.report_date BETWEEN p.partition_date - INTERVAL '29 days' AND p.partition_date
    GROUP BY
        s.report_date,
        s.source_account_id,
        s.campaign_id,
        s.adset_id
),
rollup AS (
    SELECT
        p.partition_date,
        (p.partition_date - INTERVAL '29 days')::date AS source_window_start_30d,
        (p.partition_date - INTERVAL '6 days')::date AS source_window_start_7d,
        p.partition_date AS source_window_end,
        d.source_account_id,
        MAX(d.source_account_name) AS source_account_name,
        MAX(d.currency_code) AS currency_code,
        MAX(d.timezone_name) AS timezone_name,
        d.campaign_id,
        MAX(d.campaign_name) AS campaign_name,
        d.adset_id,
        MAX(d.adset_name) AS adset_name,

        COUNT(*) FILTER (
            WHERE d.report_date >= p.partition_date - INTERVAL '6 days'
        )::integer AS last_7d_day_count,
        ROUND(
            SUM(d.spend) FILTER (WHERE d.report_date >= p.partition_date - INTERVAL '6 days')
            / NULLIF(SUM(d.purchase_count) FILTER (WHERE d.report_date >= p.partition_date - INTERVAL '6 days'), 0),
            6
        ) AS last_7d_cost_per_result_avg,
        ROUND(
            SUM(d.purchase_value) FILTER (WHERE d.report_date >= p.partition_date - INTERVAL '6 days')
            / NULLIF(SUM(d.spend) FILTER (WHERE d.report_date >= p.partition_date - INTERVAL '6 days'), 0),
            6
        ) AS last_7d_roas_avg,
        ROUND(
            SUM(d.purchase_count) FILTER (WHERE d.report_date >= p.partition_date - INTERVAL '6 days')
            / NULLIF(COUNT(*) FILTER (WHERE d.report_date >= p.partition_date - INTERVAL '6 days'), 0),
            6
        ) AS last_7d_result_avg,
        ROUND(
            SUM(d.clicks) FILTER (WHERE d.report_date >= p.partition_date - INTERVAL '6 days')
            / NULLIF(SUM(d.impressions) FILTER (WHERE d.report_date >= p.partition_date - INTERVAL '6 days'), 0) * 100,
            6
        ) AS last_7d_ctr_avg,
        ROUND(
            SUM(d.impressions) FILTER (WHERE d.report_date >= p.partition_date - INTERVAL '6 days')
            / NULLIF(COUNT(*) FILTER (WHERE d.report_date >= p.partition_date - INTERVAL '6 days'), 0),
            6
        ) AS last_7d_impression_avg,
        ROUND(
            SUM(d.spend) FILTER (WHERE d.report_date >= p.partition_date - INTERVAL '6 days')
            / NULLIF(COUNT(*) FILTER (WHERE d.report_date >= p.partition_date - INTERVAL '6 days'), 0),
            6
        ) AS last_7d_spending_avg,

        COUNT(*)::integer AS last_30d_day_count,
        ROUND(SUM(d.spend) / NULLIF(SUM(d.purchase_count), 0), 6) AS last_30d_cost_per_result_avg,
        ROUND(SUM(d.purchase_value) / NULLIF(SUM(d.spend), 0), 6) AS last_30d_roas_avg,
        ROUND(SUM(d.purchase_count) / NULLIF(COUNT(*), 0), 6) AS last_30d_result_avg,
        ROUND(SUM(d.clicks) / NULLIF(SUM(d.impressions), 0) * 100, 6) AS last_30d_ctr_avg,
        ROUND(SUM(d.impressions) / NULLIF(COUNT(*), 0), 6) AS last_30d_impression_avg,
        ROUND(SUM(d.spend) / NULLIF(COUNT(*), 0), 6) AS last_30d_spending_avg
    FROM params p
    JOIN source_daily d ON TRUE
    GROUP BY
        p.partition_date,
        d.source_account_id,
        d.campaign_id,
        d.adset_id
),
upserted AS (
    INSERT INTO {ADSET_METRIC_ROLLUP_TABLE} AS target (
        partition_date,
        computed_at,
        source_window_start_30d,
        source_window_start_7d,
        source_window_end,
        source_account_id,
        source_account_name,
        currency_code,
        timezone_name,
        campaign_id,
        campaign_name,
        adset_id,
        adset_name,
        last_7d_day_count,
        last_7d_cost_per_result_avg,
        last_7d_roas_avg,
        last_7d_result_avg,
        last_7d_ctr_avg,
        last_7d_impression_avg,
        last_7d_spending_avg,
        last_30d_day_count,
        last_30d_cost_per_result_avg,
        last_30d_roas_avg,
        last_30d_result_avg,
        last_30d_ctr_avg,
        last_30d_impression_avg,
        last_30d_spending_avg
    )
    SELECT
        partition_date,
        now(),
        source_window_start_30d,
        source_window_start_7d,
        source_window_end,
        source_account_id,
        source_account_name,
        currency_code,
        timezone_name,
        campaign_id,
        campaign_name,
        adset_id,
        adset_name,
        last_7d_day_count,
        last_7d_cost_per_result_avg,
        last_7d_roas_avg,
        last_7d_result_avg,
        last_7d_ctr_avg,
        last_7d_impression_avg,
        last_7d_spending_avg,
        last_30d_day_count,
        last_30d_cost_per_result_avg,
        last_30d_roas_avg,
        last_30d_result_avg,
        last_30d_ctr_avg,
        last_30d_impression_avg,
        last_30d_spending_avg
    FROM rollup
    ON CONFLICT (partition_date, source_account_id, campaign_id, adset_id) DO UPDATE
    SET
        computed_at = EXCLUDED.computed_at,
        record_updated_at = now(),
        update_count = target.update_count + 1,
        source_window_start_30d = EXCLUDED.source_window_start_30d,
        source_window_start_7d = EXCLUDED.source_window_start_7d,
        source_window_end = EXCLUDED.source_window_end,
        source_account_name = EXCLUDED.source_account_name,
        currency_code = EXCLUDED.currency_code,
        timezone_name = EXCLUDED.timezone_name,
        campaign_name = EXCLUDED.campaign_name,
        adset_name = EXCLUDED.adset_name,
        last_7d_day_count = EXCLUDED.last_7d_day_count,
        last_7d_cost_per_result_avg = EXCLUDED.last_7d_cost_per_result_avg,
        last_7d_roas_avg = EXCLUDED.last_7d_roas_avg,
        last_7d_result_avg = EXCLUDED.last_7d_result_avg,
        last_7d_ctr_avg = EXCLUDED.last_7d_ctr_avg,
        last_7d_impression_avg = EXCLUDED.last_7d_impression_avg,
        last_7d_spending_avg = EXCLUDED.last_7d_spending_avg,
        last_30d_day_count = EXCLUDED.last_30d_day_count,
        last_30d_cost_per_result_avg = EXCLUDED.last_30d_cost_per_result_avg,
        last_30d_roas_avg = EXCLUDED.last_30d_roas_avg,
        last_30d_result_avg = EXCLUDED.last_30d_result_avg,
        last_30d_ctr_avg = EXCLUDED.last_30d_ctr_avg,
        last_30d_impression_avg = EXCLUDED.last_30d_impression_avg,
        last_30d_spending_avg = EXCLUDED.last_30d_spending_avg
    RETURNING 1
)
SELECT
    (SELECT partition_date FROM params) AS partition_date,
    (SELECT (partition_date - INTERVAL '29 days')::date FROM params) AS source_window_start_30d,
    (SELECT (partition_date - INTERVAL '6 days')::date FROM params) AS source_window_start_7d,
    (SELECT partition_date FROM params) AS source_window_end,
    COUNT(*)::integer AS row_count
FROM upserted
""".strip()


def refresh_adset_metric_rollup(conn: Any, partition_date: date | str | None = None) -> dict[str, Any]:
    resolved_partition = parse_partition_date(partition_date) if partition_date else latest_adset_metric_partition(conn)
    if resolved_partition is None:
        return {
            "partition_date": None,
            "row_count": 0,
            "skipped": "marketing.meta_adset_daily_snapshot has no report_date rows",
        }

    with conn.cursor() as cursor:
        cursor.execute(adset_metric_rollup_sql(), {"partition_date": resolved_partition})
        row = cursor.fetchone()
    conn.commit()

    return {
        "partition_date": row[0].isoformat() if row and row[0] else resolved_partition.isoformat(),
        "source_window_start_30d": row[1].isoformat() if row and row[1] else None,
        "source_window_start_7d": row[2].isoformat() if row and row[2] else None,
        "source_window_end": row[3].isoformat() if row and row[3] else resolved_partition.isoformat(),
        "row_count": int(row[4] if row else 0),
    }
