"""GA4 session behavior aggregate queries and Cloud SQL upserts."""

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
)

US_SESSION_DAILY_TABLE = "ga4.us_session_daily"
US_SESSION_DAILY_STATE_TABLE = "ga4.us_session_daily_state"
US_SESSION_DAILY_HOUR_TABLE = "ga4.us_session_daily_hour"
US_SESSION_DAILY_STATE_HOUR_TABLE = "ga4.us_session_daily_state_hour"

BASE_COLUMNS = (
    "report_date",
    "country",
    "source_project_id",
    "source_dataset_id",
    "source_table",
    "source_table_type",
    "is_finalized",
)

SESSION_METRIC_COLUMNS = (
    "session_count",
    "user_count",
    "event_count",
    "avg_events_per_session",
    "avg_session_seconds",
    "min_session_seconds",
    "max_session_seconds",
    "median_session_seconds",
    "p90_session_seconds",
    "add_to_cart_session_count",
    "add_to_cart_user_count",
    "purchase_session_count",
    "purchaser_count",
    "returning_session_count",
    "returning_user_count",
    "facebook_session_count",
    "google_session_count",
    "referral_session_count",
    "direct_session_count",
)

PURCHASER_AVERAGE_COLUMNS = (
    "purchaser_avg_session_seconds",
    "purchaser_avg_events_per_session",
)

US_SESSION_DAILY_COLUMNS = (
    *BASE_COLUMNS,
    *SESSION_METRIC_COLUMNS,
    *PURCHASER_AVERAGE_COLUMNS,
)
US_SESSION_DAILY_STATE_COLUMNS = (
    "report_date",
    "country",
    "state",
    "source_project_id",
    "source_dataset_id",
    "source_table",
    "source_table_type",
    "is_finalized",
    *SESSION_METRIC_COLUMNS,
    *PURCHASER_AVERAGE_COLUMNS,
)
US_SESSION_DAILY_HOUR_COLUMNS = (
    "report_date",
    "country",
    "pacific_hour",
    "pacific_hour_start",
    "source_project_id",
    "source_dataset_id",
    "source_table",
    "source_table_type",
    "is_finalized",
    *SESSION_METRIC_COLUMNS,
)
US_SESSION_DAILY_STATE_HOUR_COLUMNS = (
    "report_date",
    "country",
    "pacific_hour",
    "pacific_hour_start",
    "state",
    "source_project_id",
    "source_dataset_id",
    "source_table",
    "source_table_type",
    "is_finalized",
    *SESSION_METRIC_COLUMNS,
)

US_SESSION_DAILY_CONFLICT_COLUMNS = ("report_date", "country")
US_SESSION_DAILY_STATE_CONFLICT_COLUMNS = ("report_date", "country", "state")
US_SESSION_DAILY_HOUR_CONFLICT_COLUMNS = ("report_date", "country", "pacific_hour")
US_SESSION_DAILY_STATE_HOUR_CONFLICT_COLUMNS = (
    "report_date",
    "country",
    "pacific_hour",
    "state",
)

US_SESSION_DAILY_CHANGE_COLUMNS = tuple(
    column for column in US_SESSION_DAILY_COLUMNS if column not in US_SESSION_DAILY_CONFLICT_COLUMNS
)
US_SESSION_DAILY_STATE_CHANGE_COLUMNS = tuple(
    column for column in US_SESSION_DAILY_STATE_COLUMNS if column not in US_SESSION_DAILY_STATE_CONFLICT_COLUMNS
)
US_SESSION_DAILY_HOUR_CHANGE_COLUMNS = tuple(
    column for column in US_SESSION_DAILY_HOUR_COLUMNS if column not in US_SESSION_DAILY_HOUR_CONFLICT_COLUMNS
)
US_SESSION_DAILY_STATE_HOUR_CHANGE_COLUMNS = tuple(
    column
    for column in US_SESSION_DAILY_STATE_HOUR_COLUMNS
    if column not in US_SESSION_DAILY_STATE_HOUR_CONFLICT_COLUMNS
)

COUNTRY_CODES = {
    "United States": "US",
    "Australia": "AU",
    "Isle of Man": "IM",
    "Canada": "CA",
    "Germany": "DE",
    "United Kingdom": "GB",
    "China": "CN",
    "Spain": "ES",
    "Israel": "IL",
    "India": "IN",
    "Singapore": "SG",
    "Netherlands": "NL",
    "Sweden": "SE",
    "France": "FR",
    "Romania": "RO",
    "Mexico": "MX",
    "Italy": "IT",
    "New Zealand": "NZ",
    "Poland": "PL",
    "Ireland": "IE",
    "Hong Kong": "HK",
    "Belgium": "BE",
    "Austria": "AT",
    "Puerto Rico": "PR",
    "Indonesia": "ID",
    "Dominican Republic": "DO",
    "Croatia": "HR",
    "Sint Maarten": "SX",
    "Chile": "CL",
    "Norway": "NO",
    "Portugal": "PT",
    "Türkiye": "TR",
    "Argentina": "AR",
    "Brazil": "BR",
    "Switzerland": "CH",
    "Iraq": "IQ",
    "Philippines": "PH",
    "Mozambique": "MZ",
    "Saudi Arabia": "SA",
    "Thailand": "TH",
    "Ukraine": "UA",
    "Pakistan": "PK",
    "Vietnam": "VN",
    "Japan": "JP",
    "Hungary": "HU",
    "Denmark": "DK",
    "United Arab Emirates": "AE",
    "Peru": "PE",
    "South Korea": "KR",
    "Colombia": "CO",
    "Malaysia": "MY",
    "South Africa": "ZA",
    "Uruguay": "UY",
    "Costa Rica": "CR",
    "Bahamas": "BS",
    "Slovenia": "SI",
    "Jamaica": "JM",
    "Cambodia": "KH",
    "Egypt": "EG",
    "Latvia": "LV",
    "Taiwan": "TW",
    "Lithuania": "LT",
    "Czechia": "CZ",
    "Luxembourg": "LU",
    "Greece": "GR",
    "Svalbard & Jan Mayen": "SJ",
    "Finland": "FI",
    "Bulgaria": "BG",
    "Kazakhstan": "KZ",
    "Ecuador": "EC",
    "Guernsey": "GG",
    "Trinidad & Tobago": "TT",
    "Russia": "RU",
    "Panama": "PA",
    "Bangladesh": "BD",
    "Venezuela": "VE",
    "Mongolia": "MN",
    "Bolivia": "BO",
    "Guatemala": "GT",
    "Montenegro": "ME",
    "Cyprus": "CY",
    "Andorra": "AD",
    "Martinique": "MQ",
    "Estonia": "EE",
    "Albania": "AL",
    "Fiji": "FJ",
    "Morocco": "MA",
    "Palau": "PW",
    "Slovakia": "SK",
    "Georgia": "GE",
    "Kenya": "KE",
    "North Macedonia": "MK",
    "Serbia": "RS",
    "Azerbaijan": "AZ",
    "Jordan": "JO",
    "Guam": "GU",
    "Kuwait": "KW",
    "Paraguay": "PY",
    "Liechtenstein": "LI",
    "Qatar": "QA",
    "Iceland": "IS",
    "Syria": "SY",
    "Nicaragua": "NI",
    "Uganda": "UG",
    "Tanzania": "TZ",
    "Nigeria": "NG",
    "Honduras": "HN",
    "Sri Lanka": "LK",
    "Uzbekistan": "UZ",
    "Palestine": "PS",
    "Belize": "BZ",
    "Bosnia & Herzegovina": "BA",
    "Algeria": "DZ",
    "Equatorial Guinea": "GQ",
    "Zimbabwe": "ZW",
    "Iran": "IR",
    "Belarus": "BY",
    "Namibia": "NA",
    "El Salvador": "SV",
    "Kyrgyzstan": "KG",
    "Cayman Islands": "KY",
    "Togo": "TG",
    "Angola": "AO",
    "Congo - Brazzaville": "CG",
    "Gabon": "GA",
    "Rwanda": "RW",
    "Nepal": "NP",
    "Armenia": "AM",
}


def entry_country_sql(field: str = "geo.country") -> str:
    cases = "\n".join(
        f"      WHEN {field} = '{country}' THEN '{code}'"
        for country, code in COUNTRY_CODES.items()
    )
    return f"""CASE
{cases}
      ELSE COALESCE(NULLIF({field}, ''), '(not set)')
    END"""


def us_session_behavior_base_query(
    report_date: date | str,
    *,
    source_table: str | None = None,
    source_table_type: str = "finalized",
) -> tuple[str, str, str, str]:
    report_date_sql = ga4_report_date_literal(report_date)
    checked_source_table_type = ga4_source_table_type(source_table_type)
    resolved_source_table = source_table or ga4_source_table(
        report_date,
        intraday=checked_source_table_type == "intraday",
    )
    metadata_columns = f"""
  '{PROJECT_ID}' AS source_project_id,
  '{DATASET_ID}' AS source_dataset_id,
  '{resolved_source_table}' AS source_table,
  '{checked_source_table_type}' AS source_table_type,
  {ga4_is_finalized_sql(checked_source_table_type)} AS is_finalized,""".rstrip()
    return report_date_sql, checked_source_table_type, resolved_source_table, metadata_columns


def us_session_behavior_cte(
    report_date: date | str,
    *,
    source_table: str | None = None,
    source_table_type: str = "finalized",
) -> tuple[str, str, str]:
    report_date_sql, _, resolved_source_table, metadata_columns = us_session_behavior_base_query(
        report_date,
        source_table=source_table,
        source_table_type=source_table_type,
    )
    cte = f"""
WITH session_events AS (
  SELECT
    user_pseudo_id,
    (
      SELECT value.int_value
      FROM UNNEST(event_params)
      WHERE key = 'ga_session_id'
    ) AS ga_session_id,
    (
      SELECT value.int_value
      FROM UNNEST(event_params)
      WHERE key = 'ga_session_number'
    ) AS ga_session_number,
    event_name,
    event_timestamp,
    event_bundle_sequence_id,
    batch_page_id,
    batch_ordering_id,
    batch_event_index,
    {entry_country_sql()} AS country,
    COALESCE(NULLIF(geo.region, ''), '(not set)') AS state,
    session_traffic_source_last_click.cross_channel_campaign.default_channel_group AS session_channel_group,
    session_traffic_source_last_click.cross_channel_campaign.source AS session_source,
    session_traffic_source_last_click.cross_channel_campaign.medium AS session_medium,
    collected_traffic_source.manual_source,
    collected_traffic_source.manual_medium,
    traffic_source.source AS first_user_source,
    traffic_source.medium AS first_user_medium
  FROM `{resolved_source_table}`
  WHERE user_pseudo_id IS NOT NULL
),

session_rollup AS (
  SELECT
    user_pseudo_id,
    ga_session_id,
    MIN(ga_session_number) AS ga_session_number,
    ARRAY_AGG(country IGNORE NULLS ORDER BY
      event_timestamp,
      event_bundle_sequence_id,
      batch_page_id,
      batch_ordering_id,
      batch_event_index
      LIMIT 1
    )[SAFE_OFFSET(0)] AS entry_country,
    ARRAY_AGG(state IGNORE NULLS ORDER BY
      event_timestamp,
      event_bundle_sequence_id,
      batch_page_id,
      batch_ordering_id,
      batch_event_index
      LIMIT 1
    )[SAFE_OFFSET(0)] AS entry_state,
    ARRAY_AGG(
      COALESCE(session_source, manual_source, first_user_source) IGNORE NULLS ORDER BY
      event_timestamp,
      event_bundle_sequence_id,
      batch_page_id,
      batch_ordering_id,
      batch_event_index
      LIMIT 1
    )[SAFE_OFFSET(0)] AS entry_source,
    ARRAY_AGG(
      COALESCE(session_medium, manual_medium, first_user_medium) IGNORE NULLS ORDER BY
      event_timestamp,
      event_bundle_sequence_id,
      batch_page_id,
      batch_ordering_id,
      batch_event_index
      LIMIT 1
    )[SAFE_OFFSET(0)] AS entry_medium,
    ARRAY_AGG(session_channel_group IGNORE NULLS ORDER BY
      event_timestamp,
      event_bundle_sequence_id,
      batch_page_id,
      batch_ordering_id,
      batch_event_index
      LIMIT 1
    )[SAFE_OFFSET(0)] AS entry_channel_group,
    MIN(event_timestamp) AS first_event_ts,
    MAX(event_timestamp) AS last_event_ts,
    COUNT(*) AS event_count,
    COUNTIF(event_name = 'add_to_cart') AS add_to_cart_events,
    COUNTIF(event_name = 'purchase') AS purchase_events
  FROM session_events
  WHERE ga_session_id IS NOT NULL
  GROUP BY 1, 2
),

user_rollup AS (
  SELECT
    user_pseudo_id,
    COUNT(*) AS sessions_in_range,
    MAX(ga_session_number) AS max_ga_session_number
  FROM session_rollup
  GROUP BY 1
),

country_sessions AS (
  SELECT
    s.*,
    u.sessions_in_range,
    u.max_ga_session_number,
    (u.sessions_in_range > 1 OR u.max_ga_session_number > 1) AS did_come_back,
    (s.last_event_ts - s.first_event_ts) / 1000000 AS session_seconds
  FROM session_rollup s
  JOIN user_rollup u USING (user_pseudo_id)
)""".strip()
    return cte, report_date_sql, metadata_columns


def us_session_metrics_sql(*, include_purchaser_averages: bool) -> str:
    purchaser_columns = ""
    if include_purchaser_averages:
        purchaser_columns = """,
  COALESCE(ROUND(AVG(IF(purchase_events > 0, session_seconds, NULL)), 6), 0) AS purchaser_avg_session_seconds,
  COALESCE(ROUND(AVG(IF(purchase_events > 0, event_count, NULL)), 6), 0) AS purchaser_avg_events_per_session"""

    return f"""  CAST(COUNT(*) AS INT64) AS session_count,
  CAST(COUNT(DISTINCT user_pseudo_id) AS INT64) AS user_count,
  CAST(SUM(event_count) AS INT64) AS event_count,
  COALESCE(ROUND(AVG(event_count), 6), 0) AS avg_events_per_session,
  COALESCE(ROUND(AVG(session_seconds), 6), 0) AS avg_session_seconds,
  COALESCE(ROUND(MIN(session_seconds), 6), 0) AS min_session_seconds,
  COALESCE(ROUND(MAX(session_seconds), 6), 0) AS max_session_seconds,
  COALESCE(ROUND(APPROX_QUANTILES(session_seconds, 100)[OFFSET(50)], 6), 0) AS median_session_seconds,
  COALESCE(ROUND(APPROX_QUANTILES(session_seconds, 100)[OFFSET(90)], 6), 0) AS p90_session_seconds,
  CAST(COUNTIF(add_to_cart_events > 0) AS INT64) AS add_to_cart_session_count,
  CAST(COUNT(DISTINCT IF(add_to_cart_events > 0, user_pseudo_id, NULL)) AS INT64) AS add_to_cart_user_count,
  CAST(COUNTIF(purchase_events > 0) AS INT64) AS purchase_session_count,
  CAST(COUNT(DISTINCT IF(purchase_events > 0, user_pseudo_id, NULL)) AS INT64) AS purchaser_count,
  CAST(COUNTIF(did_come_back) AS INT64) AS returning_session_count,
  CAST(COUNT(DISTINCT IF(did_come_back, user_pseudo_id, NULL)) AS INT64) AS returning_user_count,
  CAST(COUNTIF(
    REGEXP_CONTAINS(LOWER(COALESCE(entry_source, '')), r'(facebook|\\bfb\\b|meta|instagram|\\big\\b)')
  ) AS INT64) AS facebook_session_count,
  CAST(COUNTIF(
    REGEXP_CONTAINS(LOWER(COALESCE(entry_source, '')), r'google')
  ) AS INT64) AS google_session_count,
  CAST(COUNTIF(
    LOWER(COALESCE(entry_medium, '')) = 'referral'
    OR LOWER(COALESCE(entry_channel_group, '')) = 'referral'
  ) AS INT64) AS referral_session_count,
  CAST(COUNTIF(
    LOWER(COALESCE(entry_source, '')) IN ('(direct)', 'direct')
    OR LOWER(COALESCE(entry_medium, '')) IN ('(none)', 'none', 'direct')
    OR LOWER(COALESCE(entry_channel_group, '')) = 'direct'
  ) AS INT64) AS direct_session_count{purchaser_columns}"""


def us_session_daily_query(
    report_date: date | str,
    *,
    source_table: str | None = None,
    source_table_type: str = "finalized",
) -> str:
    cte, report_date_sql, metadata_columns = us_session_behavior_cte(
        report_date,
        source_table=source_table,
        source_table_type=source_table_type,
    )
    return f"""
{cte}

SELECT
  DATE '{report_date_sql}' AS report_date,
  entry_country AS country,
{metadata_columns}
{us_session_metrics_sql(include_purchaser_averages=True)}
FROM country_sessions
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY report_date DESC, country
""".strip()


def us_session_daily_state_query(
    report_date: date | str,
    *,
    source_table: str | None = None,
    source_table_type: str = "finalized",
) -> str:
    cte, report_date_sql, metadata_columns = us_session_behavior_cte(
        report_date,
        source_table=source_table,
        source_table_type=source_table_type,
    )
    return f"""
{cte}

SELECT
  DATE '{report_date_sql}' AS report_date,
  entry_country AS country,
  entry_state AS state,
  {metadata_columns.strip()}
{us_session_metrics_sql(include_purchaser_averages=True)}
FROM country_sessions
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
ORDER BY report_date DESC, country, session_count DESC, state
""".strip()


def us_session_daily_hour_query(
    report_date: date | str,
    *,
    source_table: str | None = None,
    source_table_type: str = "finalized",
) -> str:
    cte, report_date_sql, metadata_columns = us_session_behavior_cte(
        report_date,
        source_table=source_table,
        source_table_type=source_table_type,
    )
    return f"""
{cte}

SELECT
  DATE '{report_date_sql}' AS report_date,
  entry_country AS country,
  EXTRACT(HOUR FROM DATETIME(TIMESTAMP_MICROS(first_event_ts), 'America/Los_Angeles')) AS pacific_hour,
  DATETIME_TRUNC(DATETIME(TIMESTAMP_MICROS(first_event_ts), 'America/Los_Angeles'), HOUR) AS pacific_hour_start,
  {metadata_columns.strip()}
{us_session_metrics_sql(include_purchaser_averages=False)}
FROM country_sessions
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
ORDER BY report_date DESC, country, pacific_hour DESC
""".strip()


def us_session_daily_state_hour_query(
    report_date: date | str,
    *,
    source_table: str | None = None,
    source_table_type: str = "finalized",
) -> str:
    cte, report_date_sql, metadata_columns = us_session_behavior_cte(
        report_date,
        source_table=source_table,
        source_table_type=source_table_type,
    )
    return f"""
{cte}

SELECT
  DATE '{report_date_sql}' AS report_date,
  entry_country AS country,
  EXTRACT(HOUR FROM DATETIME(TIMESTAMP_MICROS(first_event_ts), 'America/Los_Angeles')) AS pacific_hour,
  DATETIME_TRUNC(DATETIME(TIMESTAMP_MICROS(first_event_ts), 'America/Los_Angeles'), HOUR) AS pacific_hour_start,
  entry_state AS state,
  {metadata_columns.strip()}
{us_session_metrics_sql(include_purchaser_averages=False)}
FROM country_sessions
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
ORDER BY report_date DESC, country, pacific_hour DESC, session_count DESC, state
""".strip()


def us_session_daily_row(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(column) for column in US_SESSION_DAILY_COLUMNS)


def us_session_daily_state_row(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(column) for column in US_SESSION_DAILY_STATE_COLUMNS)


def us_session_daily_hour_row(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(column) for column in US_SESSION_DAILY_HOUR_COLUMNS)


def us_session_daily_state_hour_row(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(column) for column in US_SESSION_DAILY_STATE_HOUR_COLUMNS)


def merge_us_session_daily(
    rows: Sequence[Mapping[str, Any]],
    report_date: date | str,
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Any | None = None,
) -> None:
    merge_ga4_rows_for_report_date(
        US_SESSION_DAILY_TABLE,
        US_SESSION_DAILY_COLUMNS,
        US_SESSION_DAILY_CONFLICT_COLUMNS,
        US_SESSION_DAILY_CHANGE_COLUMNS,
        [us_session_daily_row(row) for row in rows],
        report_date,
        postgres_conn_id=postgres_conn_id,
        postgres_hook_factory=postgres_hook_factory,
    )


def merge_us_session_daily_state(
    rows: Sequence[Mapping[str, Any]],
    report_date: date | str,
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Any | None = None,
) -> None:
    merge_ga4_rows_for_report_date(
        US_SESSION_DAILY_STATE_TABLE,
        US_SESSION_DAILY_STATE_COLUMNS,
        US_SESSION_DAILY_STATE_CONFLICT_COLUMNS,
        US_SESSION_DAILY_STATE_CHANGE_COLUMNS,
        [us_session_daily_state_row(row) for row in rows],
        report_date,
        postgres_conn_id=postgres_conn_id,
        postgres_hook_factory=postgres_hook_factory,
    )


def merge_us_session_daily_hour(
    rows: Sequence[Mapping[str, Any]],
    report_date: date | str,
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Any | None = None,
) -> None:
    merge_ga4_rows_for_report_date(
        US_SESSION_DAILY_HOUR_TABLE,
        US_SESSION_DAILY_HOUR_COLUMNS,
        US_SESSION_DAILY_HOUR_CONFLICT_COLUMNS,
        US_SESSION_DAILY_HOUR_CHANGE_COLUMNS,
        [us_session_daily_hour_row(row) for row in rows],
        report_date,
        postgres_conn_id=postgres_conn_id,
        postgres_hook_factory=postgres_hook_factory,
    )


def merge_us_session_daily_state_hour(
    rows: Sequence[Mapping[str, Any]],
    report_date: date | str,
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Any | None = None,
) -> None:
    merge_ga4_rows_for_report_date(
        US_SESSION_DAILY_STATE_HOUR_TABLE,
        US_SESSION_DAILY_STATE_HOUR_COLUMNS,
        US_SESSION_DAILY_STATE_HOUR_CONFLICT_COLUMNS,
        US_SESSION_DAILY_STATE_HOUR_CHANGE_COLUMNS,
        [us_session_daily_state_hour_row(row) for row in rows],
        report_date,
        postgres_conn_id=postgres_conn_id,
        postgres_hook_factory=postgres_hook_factory,
    )


def us_session_report_date_is_finalized(
    report_date: date | str,
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Any | None = None,
) -> bool:
    """Return True when any daily session aggregate is finalized for the date."""
    if postgres_hook_factory is None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-not-found]

        postgres_hook_factory = PostgresHook

    hook = postgres_hook_factory(postgres_conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM ga4.us_session_daily
                    WHERE report_date = %s
                      AND is_finalized = TRUE
                )
                """,
                (ga4_report_date_literal(report_date),),
            )
            row = cursor.fetchone()
            return bool(row[0]) if row else False
    finally:
        conn.close()
