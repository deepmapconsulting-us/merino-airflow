"""GA4 daily purchaser session behavior queries and Cloud SQL upserts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

from merino_ga4_jobs.daily_analysis import (
    DATASET_ID,
    POSTGRES_CONN_ID,
    PROJECT_ID,
    ga4_is_finalized_sql,
    ga4_report_date_literal,
    ga4_source_table,
    ga4_source_table_type,
    merge_ga4_rows_for_report_date,
    replace_ga4_rows_for_report_date,
    upsert_ga4_rows,
)

DAILY_PURCHASER_BEHAVIOR_TABLE = "ga4.daily_purchaser_behavior"
DAILY_PURCHASER_BEHAVIOR_COLUMNS = (
    "report_date",
    "source_project_id",
    "source_dataset_id",
    "source_table",
    "source_table_type",
    "is_finalized",
    "user_pseudo_id",
    "ga_session_id",
    "session_start_at",
    "device_category",
    "device_operating_system",
    "device_browser",
    "traffic_channel_group",
    "traffic_source",
    "traffic_medium",
    "traffic_campaign",
    "traffic_placement",
    "is_meta_paid",
    "session_event_count",
    "session_seconds",
    "session_minutes",
    "seconds_to_purchase",
    "minutes_to_purchase",
    "purchase_revenue_usd",
    "transaction_id",
    "purchase_event_count",
    "first_page_view_step",
    "first_view_item_step",
    "first_add_to_cart_step",
    "begin_checkout_step",
    "purchase_step",
    "landing_page",
)
DAILY_PURCHASER_BEHAVIOR_CONFLICT_COLUMNS = ("report_date", "user_pseudo_id", "ga_session_id")
DAILY_PURCHASER_BEHAVIOR_CHANGE_COLUMNS = tuple(
    column
    for column in DAILY_PURCHASER_BEHAVIOR_COLUMNS
    if column not in DAILY_PURCHASER_BEHAVIOR_CONFLICT_COLUMNS
)


def daily_purchaser_behavior_query(
    report_date: date | str,
    *,
    source_table: str | None = None,
    source_table_type: str = "finalized",
) -> str:
    report_date_sql = ga4_report_date_literal(report_date)
    checked_source_table_type = ga4_source_table_type(source_table_type)
    resolved_source_table = source_table or ga4_source_table(
        report_date,
        intraday=checked_source_table_type == "intraday",
    )
    return f"""
WITH session_events AS (
  SELECT
    DATE '{report_date_sql}' AS report_date,
    user_pseudo_id,
    event_name,
    event_timestamp,
    event_bundle_sequence_id,
    batch_page_id,
    batch_ordering_id,
    batch_event_index,
    device.category AS device_category,
    device.operating_system AS device_operating_system,
    device.browser AS device_browser,
    session_traffic_source_last_click.cross_channel_campaign.default_channel_group AS traffic_channel_group,
    session_traffic_source_last_click.cross_channel_campaign.source AS session_source,
    session_traffic_source_last_click.cross_channel_campaign.medium AS session_medium,
    session_traffic_source_last_click.cross_channel_campaign.campaign_name AS session_campaign,
    collected_traffic_source.manual_source,
    collected_traffic_source.manual_medium,
    collected_traffic_source.manual_campaign_name,
    collected_traffic_source.manual_term,
    ecommerce.transaction_id,
    ecommerce.purchase_revenue_in_usd,
    (
      SELECT value.int_value
      FROM UNNEST(event_params)
      WHERE key = 'ga_session_id'
    ) AS ga_session_id,
    (
      SELECT value.string_value
      FROM UNNEST(event_params)
      WHERE key = 'page_location'
    ) AS page_location
  FROM `{resolved_source_table}`
  WHERE user_pseudo_id IS NOT NULL
),

ordered_events AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY user_pseudo_id, ga_session_id
      ORDER BY
        event_timestamp,
        event_bundle_sequence_id,
        batch_page_id,
        batch_ordering_id,
        batch_event_index
    ) AS event_step
  FROM session_events
  WHERE ga_session_id IS NOT NULL
),

sessions AS (
  SELECT
    report_date,
    user_pseudo_id,
    ga_session_id,
    COUNT(*) AS session_event_count,
    MIN(event_timestamp) AS first_event_ts,
    MAX(event_timestamp) AS last_event_ts,
    MIN(IF(event_step = 1, device_category, NULL)) AS device_category,
    MIN(IF(event_step = 1, device_operating_system, NULL)) AS device_operating_system,
    MIN(IF(event_step = 1, device_browser, NULL)) AS device_browser,
    MIN(IF(event_step = 1, traffic_channel_group, NULL)) AS traffic_channel_group,
    COALESCE(
      MIN(IF(event_step = 1, session_source, NULL)),
      MIN(IF(event_step = 1, manual_source, NULL))
    ) AS traffic_source,
    COALESCE(
      MIN(IF(event_step = 1, session_medium, NULL)),
      MIN(IF(event_step = 1, manual_medium, NULL))
    ) AS traffic_medium,
    COALESCE(
      MIN(IF(event_step = 1, session_campaign, NULL)),
      MIN(IF(event_step = 1, manual_campaign_name, NULL))
    ) AS traffic_campaign,
    COALESCE(
      ARRAY_AGG(page_location IGNORE NULLS ORDER BY event_step LIMIT 1)[SAFE_OFFSET(0)],
      '(not set)'
    ) AS landing_page,
    COUNTIF(event_name = 'purchase') AS purchase_event_count,
    SUM(IF(event_name = 'purchase', purchase_revenue_in_usd, 0)) AS purchase_revenue_usd,
    MIN(IF(event_name = 'purchase', transaction_id, NULL)) AS transaction_id,
    MIN(IF(event_name = 'purchase', event_step, NULL)) AS purchase_step,
    MIN(IF(event_name = 'purchase', event_timestamp, NULL)) AS first_purchase_ts,
    MIN(IF(event_name = 'page_view', event_step, NULL)) AS first_page_view_step,
    MIN(IF(event_name = 'view_item', event_step, NULL)) AS first_view_item_step,
    MIN(IF(event_name = 'add_to_cart', event_step, NULL)) AS first_add_to_cart_step,
    MIN(IF(event_name = 'begin_checkout', event_step, NULL)) AS begin_checkout_step,
    COALESCE(
      MIN(IF(event_step = 1, manual_term, NULL)),
      REGEXP_EXTRACT(
        ARRAY_AGG(page_location IGNORE NULLS ORDER BY event_step LIMIT 1)[SAFE_OFFSET(0)],
        r'[?&]placement=([^&]+)'
      )
    ) AS traffic_placement
  FROM ordered_events
  GROUP BY 1, 2, 3
)

SELECT
  report_date,
  '{PROJECT_ID}' AS source_project_id,
  '{DATASET_ID}' AS source_dataset_id,
  '{resolved_source_table}' AS source_table,
  '{checked_source_table_type}' AS source_table_type,
  {ga4_is_finalized_sql(checked_source_table_type)} AS is_finalized,
  user_pseudo_id,
  ga_session_id,
  TIMESTAMP(DATETIME(TIMESTAMP_MICROS(first_event_ts), 'America/Los_Angeles')) AS session_start_at,
  device_category,
  device_operating_system,
  device_browser,
  traffic_channel_group,
  traffic_source,
  traffic_medium,
  traffic_campaign,
  traffic_placement,
  COALESCE(
    (
      LOWER(COALESCE(traffic_source, '')) IN ('facebook', 'fb', 'meta', 'instagram', 'ig')
      OR traffic_channel_group = 'Paid Social'
      OR (
        LOWER(COALESCE(traffic_medium, '')) IN ('paid_social', 'cpc', 'paid')
        AND LOWER(COALESCE(traffic_source, '')) IN ('facebook', 'fb', 'meta', 'instagram', 'ig')
      )
    ),
    FALSE
  ) AS is_meta_paid,
  CAST(session_event_count AS INT64) AS session_event_count,
  ROUND((last_event_ts - first_event_ts) / 1000000, 6) AS session_seconds,
  ROUND((last_event_ts - first_event_ts) / 1000000 / 60, 6) AS session_minutes,
  ROUND((first_purchase_ts - first_event_ts) / 1000000, 6) AS seconds_to_purchase,
  ROUND((first_purchase_ts - first_event_ts) / 1000000 / 60, 6) AS minutes_to_purchase,
  ROUND(purchase_revenue_usd, 6) AS purchase_revenue_usd,
  transaction_id,
  CAST(purchase_event_count AS INT64) AS purchase_event_count,
  first_page_view_step,
  first_view_item_step,
  first_add_to_cart_step,
  begin_checkout_step,
  purchase_step,
  landing_page
FROM sessions
WHERE purchase_event_count > 0
ORDER BY session_start_at, user_pseudo_id, ga_session_id
""".strip()


def daily_purchaser_behavior_row(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(column) for column in DAILY_PURCHASER_BEHAVIOR_COLUMNS)


def upsert_daily_purchaser_behavior(
    rows: Sequence[Mapping[str, Any]],
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Any | None = None,
) -> None:
    upsert_ga4_rows(
        DAILY_PURCHASER_BEHAVIOR_TABLE,
        DAILY_PURCHASER_BEHAVIOR_COLUMNS,
        DAILY_PURCHASER_BEHAVIOR_CONFLICT_COLUMNS,
        DAILY_PURCHASER_BEHAVIOR_CHANGE_COLUMNS,
        [daily_purchaser_behavior_row(row) for row in rows],
        postgres_conn_id=postgres_conn_id,
        postgres_hook_factory=postgres_hook_factory,
    )


def replace_daily_purchaser_behavior(
    rows: Sequence[Mapping[str, Any]],
    report_date: date | str,
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Any | None = None,
) -> None:
    replace_ga4_rows_for_report_date(
        DAILY_PURCHASER_BEHAVIOR_TABLE,
        DAILY_PURCHASER_BEHAVIOR_COLUMNS,
        [daily_purchaser_behavior_row(row) for row in rows],
        report_date,
        postgres_conn_id=postgres_conn_id,
        postgres_hook_factory=postgres_hook_factory,
    )


def merge_daily_purchaser_behavior(
    rows: Sequence[Mapping[str, Any]],
    report_date: date | str,
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Any | None = None,
) -> None:
    merge_ga4_rows_for_report_date(
        DAILY_PURCHASER_BEHAVIOR_TABLE,
        DAILY_PURCHASER_BEHAVIOR_COLUMNS,
        DAILY_PURCHASER_BEHAVIOR_CONFLICT_COLUMNS,
        DAILY_PURCHASER_BEHAVIOR_CHANGE_COLUMNS,
        [daily_purchaser_behavior_row(row) for row in rows],
        report_date,
        postgres_conn_id=postgres_conn_id,
        postgres_hook_factory=postgres_hook_factory,
    )
