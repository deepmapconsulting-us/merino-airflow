"""GA4 daily session analysis queries and Cloud SQL upserts."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timedelta
from typing import Any

POSTGRES_CONN_ID = "merino_analytics"
PROJECT_ID = "merino-agent"
DATASET_ID = "analytics_370932876"
DAILY_ANALYSIS_TABLE = "ga4.daily_analysis"
LANDING_PAGE_DAILY_ANALYSIS_TABLE = "ga4.landing_page_daily_analysis"

DAILY_ANALYSIS_COLUMNS = (
    "report_date",
    "source_project_id",
    "source_dataset_id",
    "source_table",
    "source_table_type",
    "is_finalized",
    "event_count",
    "session_count",
    "user_count",
    "purchase_count",
    "purchaser_count",
    "avg_session_steps",
    "min_session_steps",
    "max_session_steps",
    "avg_session_seconds",
    "min_session_seconds",
    "max_session_seconds",
    "avg_session_minutes",
    "min_session_minutes",
    "max_session_minutes",
    "avg_purchase_step",
    "min_purchase_step",
    "max_purchase_step",
    "avg_seconds_to_purchase",
    "min_seconds_to_purchase",
    "max_seconds_to_purchase",
    "avg_minutes_to_purchase",
    "min_minutes_to_purchase",
    "max_minutes_to_purchase",
)
LANDING_PAGE_DAILY_ANALYSIS_COLUMNS = (
    "report_date",
    "landing_page",
    "source_project_id",
    "source_dataset_id",
    "source_table",
    "source_table_type",
    "is_finalized",
    "session_count",
    "user_count",
    "purchase_count",
    "purchaser_count",
    "avg_session_steps",
    "min_session_steps",
    "max_session_steps",
    "avg_session_seconds",
    "min_session_seconds",
    "max_session_seconds",
    "avg_session_minutes",
    "min_session_minutes",
    "max_session_minutes",
    "avg_purchase_step",
    "min_purchase_step",
    "max_purchase_step",
    "avg_seconds_to_purchase",
    "min_seconds_to_purchase",
    "max_seconds_to_purchase",
    "avg_minutes_to_purchase",
    "min_minutes_to_purchase",
    "max_minutes_to_purchase",
)
DAILY_ANALYSIS_CONFLICT_COLUMNS = ("report_date",)
LANDING_PAGE_DAILY_ANALYSIS_CONFLICT_COLUMNS = ("report_date", "landing_page")
DAILY_ANALYSIS_CHANGE_COLUMNS = tuple(column for column in DAILY_ANALYSIS_COLUMNS if column != "report_date")
LANDING_PAGE_DAILY_ANALYSIS_CHANGE_COLUMNS = tuple(
    column
    for column in LANDING_PAGE_DAILY_ANALYSIS_COLUMNS
    if column not in LANDING_PAGE_DAILY_ANALYSIS_CONFLICT_COLUMNS
)


def ga4_report_date(logical_date: datetime | date, delay_days: int = 2) -> date:
    """Return the finalized GA4 export date for an Airflow logical date."""
    return logical_date.date() - timedelta(days=delay_days) if isinstance(logical_date, datetime) else logical_date - timedelta(days=delay_days)


def ga4_table_suffix(report_date: date | str) -> str:
    if isinstance(report_date, date):
        return report_date.strftime("%Y%m%d")

    if re.fullmatch(r"\d{8}", report_date):
        return report_date

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", report_date):
        return report_date.replace("-", "")

    raise ValueError(f"GA4 report date must be YYYY-MM-DD or YYYYMMDD: {report_date!r}")


def ga4_report_date_literal(report_date: date | str) -> str:
    if isinstance(report_date, date):
        return report_date.isoformat()

    if re.fullmatch(r"\d{8}", report_date):
        return f"{report_date[0:4]}-{report_date[4:6]}-{report_date[6:8]}"

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", report_date):
        return report_date

    raise ValueError(f"GA4 report date must be YYYY-MM-DD or YYYYMMDD: {report_date!r}")


def ga4_source_table(report_date: date | str, *, intraday: bool = False) -> str:
    prefix = "events_intraday" if intraday else "events"
    return f"{PROJECT_ID}.{DATASET_ID}.{prefix}_{ga4_table_suffix(report_date)}"


def ga4_source_table_type(source_table_type: str) -> str:
    if source_table_type not in {"finalized", "intraday"}:
        raise ValueError(f"GA4 source table type must be finalized or intraday: {source_table_type!r}")
    return source_table_type


def ga4_is_finalized_sql(source_table_type: str) -> str:
    return "TRUE" if ga4_source_table_type(source_table_type) == "finalized" else "FALSE"


def report_date_is_finalized(
    report_date: date | str,
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Callable[[str], Any] | None = None,
) -> bool:
    """Return True when Cloud SQL already has finalized rows for this report date."""
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
                    FROM ga4.daily_analysis
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


def daily_analysis_query(
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
    COUNT(*) AS event_count,
    MIN(event_timestamp) AS first_event_ts,
    MAX(event_timestamp) AS last_event_ts,
    COUNTIF(event_name = 'purchase') AS purchase_count,
    MIN(IF(event_name = 'purchase', event_step, NULL)) AS first_purchase_step,
    MIN(IF(event_name = 'purchase', event_timestamp, NULL)) AS first_purchase_ts
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
  CAST(SUM(event_count) AS INT64) AS event_count,
  CAST(COUNT(*) AS INT64) AS session_count,
  CAST(COUNT(DISTINCT user_pseudo_id) AS INT64) AS user_count,
  CAST(SUM(purchase_count) AS INT64) AS purchase_count,
  CAST(COUNT(DISTINCT IF(purchase_count > 0, user_pseudo_id, NULL)) AS INT64) AS purchaser_count,
  ROUND(AVG(event_count), 6) AS avg_session_steps,
  CAST(MIN(event_count) AS NUMERIC) AS min_session_steps,
  CAST(MAX(event_count) AS NUMERIC) AS max_session_steps,
  ROUND(AVG((last_event_ts - first_event_ts) / 1000000), 6) AS avg_session_seconds,
  ROUND(MIN((last_event_ts - first_event_ts) / 1000000), 6) AS min_session_seconds,
  ROUND(MAX((last_event_ts - first_event_ts) / 1000000), 6) AS max_session_seconds,
  ROUND(AVG((last_event_ts - first_event_ts) / 1000000 / 60), 6) AS avg_session_minutes,
  ROUND(MIN((last_event_ts - first_event_ts) / 1000000 / 60), 6) AS min_session_minutes,
  ROUND(MAX((last_event_ts - first_event_ts) / 1000000 / 60), 6) AS max_session_minutes,
  ROUND(AVG(IF(purchase_count > 0, first_purchase_step, NULL)), 6) AS avg_purchase_step,
  CAST(MIN(IF(purchase_count > 0, first_purchase_step, NULL)) AS NUMERIC) AS min_purchase_step,
  CAST(MAX(IF(purchase_count > 0, first_purchase_step, NULL)) AS NUMERIC) AS max_purchase_step,
  ROUND(AVG(IF(first_purchase_ts IS NULL, NULL, (first_purchase_ts - first_event_ts) / 1000000)), 6)
    AS avg_seconds_to_purchase,
  ROUND(MIN(IF(first_purchase_ts IS NULL, NULL, (first_purchase_ts - first_event_ts) / 1000000)), 6)
    AS min_seconds_to_purchase,
  ROUND(MAX(IF(first_purchase_ts IS NULL, NULL, (first_purchase_ts - first_event_ts) / 1000000)), 6)
    AS max_seconds_to_purchase,
  ROUND(AVG(IF(first_purchase_ts IS NULL, NULL, (first_purchase_ts - first_event_ts) / 1000000 / 60)), 6)
    AS avg_minutes_to_purchase,
  ROUND(MIN(IF(first_purchase_ts IS NULL, NULL, (first_purchase_ts - first_event_ts) / 1000000 / 60)), 6)
    AS min_minutes_to_purchase,
  ROUND(MAX(IF(first_purchase_ts IS NULL, NULL, (first_purchase_ts - first_event_ts) / 1000000 / 60)), 6)
    AS max_minutes_to_purchase
FROM sessions
GROUP BY 1, 2, 3, 4, 5, 6
""".strip()


def landing_page_daily_analysis_query(
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
    COALESCE(
      ARRAY_AGG(page_location IGNORE NULLS ORDER BY event_step LIMIT 1)[SAFE_OFFSET(0)],
      '(not set)'
    ) AS landing_page,
    COUNT(*) AS event_count,
    MIN(event_timestamp) AS first_event_ts,
    MAX(event_timestamp) AS last_event_ts,
    COUNTIF(event_name = 'purchase') AS purchase_count,
    MIN(IF(event_name = 'purchase', event_step, NULL)) AS first_purchase_step,
    MIN(IF(event_name = 'purchase', event_timestamp, NULL)) AS first_purchase_ts
  FROM ordered_events
  GROUP BY 1, 2, 3
)

SELECT
  report_date,
  landing_page,
  '{PROJECT_ID}' AS source_project_id,
  '{DATASET_ID}' AS source_dataset_id,
  '{resolved_source_table}' AS source_table,
  '{checked_source_table_type}' AS source_table_type,
  {ga4_is_finalized_sql(checked_source_table_type)} AS is_finalized,
  CAST(COUNT(*) AS INT64) AS session_count,
  CAST(COUNT(DISTINCT user_pseudo_id) AS INT64) AS user_count,
  CAST(SUM(purchase_count) AS INT64) AS purchase_count,
  CAST(COUNT(DISTINCT IF(purchase_count > 0, user_pseudo_id, NULL)) AS INT64) AS purchaser_count,
  ROUND(AVG(event_count), 6) AS avg_session_steps,
  CAST(MIN(event_count) AS NUMERIC) AS min_session_steps,
  CAST(MAX(event_count) AS NUMERIC) AS max_session_steps,
  ROUND(AVG((last_event_ts - first_event_ts) / 1000000), 6) AS avg_session_seconds,
  ROUND(MIN((last_event_ts - first_event_ts) / 1000000), 6) AS min_session_seconds,
  ROUND(MAX((last_event_ts - first_event_ts) / 1000000), 6) AS max_session_seconds,
  ROUND(AVG((last_event_ts - first_event_ts) / 1000000 / 60), 6) AS avg_session_minutes,
  ROUND(MIN((last_event_ts - first_event_ts) / 1000000 / 60), 6) AS min_session_minutes,
  ROUND(MAX((last_event_ts - first_event_ts) / 1000000 / 60), 6) AS max_session_minutes,
  ROUND(AVG(IF(purchase_count > 0, first_purchase_step, NULL)), 6) AS avg_purchase_step,
  CAST(MIN(IF(purchase_count > 0, first_purchase_step, NULL)) AS NUMERIC) AS min_purchase_step,
  CAST(MAX(IF(purchase_count > 0, first_purchase_step, NULL)) AS NUMERIC) AS max_purchase_step,
  ROUND(AVG(IF(first_purchase_ts IS NULL, NULL, (first_purchase_ts - first_event_ts) / 1000000)), 6)
    AS avg_seconds_to_purchase,
  ROUND(MIN(IF(first_purchase_ts IS NULL, NULL, (first_purchase_ts - first_event_ts) / 1000000)), 6)
    AS min_seconds_to_purchase,
  ROUND(MAX(IF(first_purchase_ts IS NULL, NULL, (first_purchase_ts - first_event_ts) / 1000000)), 6)
    AS max_seconds_to_purchase,
  ROUND(AVG(IF(first_purchase_ts IS NULL, NULL, (first_purchase_ts - first_event_ts) / 1000000 / 60)), 6)
    AS avg_minutes_to_purchase,
  ROUND(MIN(IF(first_purchase_ts IS NULL, NULL, (first_purchase_ts - first_event_ts) / 1000000 / 60)), 6)
    AS min_minutes_to_purchase,
  ROUND(MAX(IF(first_purchase_ts IS NULL, NULL, (first_purchase_ts - first_event_ts) / 1000000 / 60)), 6)
    AS max_minutes_to_purchase
FROM sessions
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY session_count DESC, landing_page
""".strip()


def daily_analysis_row(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(column) for column in DAILY_ANALYSIS_COLUMNS)


def landing_page_daily_analysis_row(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(column) for column in LANDING_PAGE_DAILY_ANALYSIS_COLUMNS)


def upsert_daily_analysis(rows: Sequence[Mapping[str, Any]], *, postgres_conn_id: str = POSTGRES_CONN_ID) -> None:
    upsert_ga4_rows(
        DAILY_ANALYSIS_TABLE,
        DAILY_ANALYSIS_COLUMNS,
        DAILY_ANALYSIS_CONFLICT_COLUMNS,
        DAILY_ANALYSIS_CHANGE_COLUMNS,
        [daily_analysis_row(row) for row in rows],
        postgres_conn_id=postgres_conn_id,
    )


def upsert_landing_page_daily_analysis(
    rows: Sequence[Mapping[str, Any]],
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
) -> None:
    upsert_ga4_rows(
        LANDING_PAGE_DAILY_ANALYSIS_TABLE,
        LANDING_PAGE_DAILY_ANALYSIS_COLUMNS,
        LANDING_PAGE_DAILY_ANALYSIS_CONFLICT_COLUMNS,
        LANDING_PAGE_DAILY_ANALYSIS_CHANGE_COLUMNS,
        [landing_page_daily_analysis_row(row) for row in rows],
        postgres_conn_id=postgres_conn_id,
    )


def replace_daily_analysis(
    rows: Sequence[Mapping[str, Any]],
    report_date: date | str,
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Callable[[str], Any] | None = None,
) -> None:
    replace_ga4_rows_for_report_date(
        DAILY_ANALYSIS_TABLE,
        DAILY_ANALYSIS_COLUMNS,
        [daily_analysis_row(row) for row in rows],
        report_date,
        postgres_conn_id=postgres_conn_id,
        postgres_hook_factory=postgres_hook_factory,
    )


def replace_landing_page_daily_analysis(
    rows: Sequence[Mapping[str, Any]],
    report_date: date | str,
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Callable[[str], Any] | None = None,
) -> None:
    replace_ga4_rows_for_report_date(
        LANDING_PAGE_DAILY_ANALYSIS_TABLE,
        LANDING_PAGE_DAILY_ANALYSIS_COLUMNS,
        [landing_page_daily_analysis_row(row) for row in rows],
        report_date,
        postgres_conn_id=postgres_conn_id,
        postgres_hook_factory=postgres_hook_factory,
    )


def merge_daily_analysis(
    rows: Sequence[Mapping[str, Any]],
    report_date: date | str,
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Callable[[str], Any] | None = None,
) -> None:
    merge_ga4_rows_for_report_date(
        DAILY_ANALYSIS_TABLE,
        DAILY_ANALYSIS_COLUMNS,
        DAILY_ANALYSIS_CONFLICT_COLUMNS,
        DAILY_ANALYSIS_CHANGE_COLUMNS,
        [daily_analysis_row(row) for row in rows],
        report_date,
        postgres_conn_id=postgres_conn_id,
        postgres_hook_factory=postgres_hook_factory,
    )


def merge_landing_page_daily_analysis(
    rows: Sequence[Mapping[str, Any]],
    report_date: date | str,
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Callable[[str], Any] | None = None,
) -> None:
    merge_ga4_rows_for_report_date(
        LANDING_PAGE_DAILY_ANALYSIS_TABLE,
        LANDING_PAGE_DAILY_ANALYSIS_COLUMNS,
        LANDING_PAGE_DAILY_ANALYSIS_CONFLICT_COLUMNS,
        LANDING_PAGE_DAILY_ANALYSIS_CHANGE_COLUMNS,
        [landing_page_daily_analysis_row(row) for row in rows],
        report_date,
        postgres_conn_id=postgres_conn_id,
        postgres_hook_factory=postgres_hook_factory,
    )


def upsert_ga4_sql(
    table_name: str,
    column_names: tuple[str, ...],
    conflict_columns: tuple[str, ...],
    change_columns: tuple[str, ...],
) -> str:
    columns = ", ".join(column_names)
    placeholders = ", ".join(["%s"] * len(column_names))
    conflict_target = ", ".join(conflict_columns)
    key_column_names = set(conflict_columns)
    update_columns = [column for column in column_names if column not in key_column_names and column != "report_date"]
    assignments = ",\n            ".join(
        [
            "record_updated_at = now()",
            "update_count = target.update_count + 1",
            *[f"{column} = EXCLUDED.{column}" for column in update_columns],
        ]
    )
    changed = "\n            OR ".join(
        f"target.{column} IS DISTINCT FROM EXCLUDED.{column}" for column in change_columns
    )
    return f"""
        INSERT INTO {table_name} AS target ({columns})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_target}) DO UPDATE
        SET {assignments}
        WHERE {changed}
    """


def upsert_ga4_rows_on_cursor(
    cursor: Any,
    table_name: str,
    column_names: tuple[str, ...],
    conflict_columns: tuple[str, ...],
    change_columns: tuple[str, ...],
    rows: Sequence[tuple[Any, ...]],
) -> None:
    if not rows:
        return

    cursor.executemany(
        upsert_ga4_sql(table_name, column_names, conflict_columns, change_columns),
        rows,
    )


def delete_ga4_orphan_rows_on_cursor(
    cursor: Any,
    table_name: str,
    column_names: tuple[str, ...],
    conflict_columns: tuple[str, ...],
    report_date: date | str,
    rows: Sequence[tuple[Any, ...]],
) -> None:
    report_date_literal = ga4_report_date_literal(report_date)
    orphan_key_columns = tuple(column for column in conflict_columns if column != "report_date")

    if not rows:
        cursor.execute(f"DELETE FROM {table_name} WHERE report_date = %s", (report_date_literal,))
        return

    if not orphan_key_columns:
        return

    key_indices = [column_names.index(column) for column in orphan_key_columns]
    orphan_tuple = ", ".join(orphan_key_columns)
    value_groups = ", ".join(f"({', '.join(['%s'] * len(orphan_key_columns))})" for _ in rows)
    params: list[Any] = [report_date_literal]
    for row in rows:
        params.extend(row[index] for index in key_indices)

    cursor.execute(
        f"""
        DELETE FROM {table_name}
        WHERE report_date = %s
          AND ({orphan_tuple}) NOT IN ({value_groups})
        """,
        tuple(params),
    )


def merge_ga4_rows_for_report_date(
    table_name: str,
    column_names: tuple[str, ...],
    conflict_columns: tuple[str, ...],
    change_columns: tuple[str, ...],
    rows: Sequence[tuple[Any, ...]],
    report_date: date | str,
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Callable[[str], Any] | None = None,
) -> None:
    if postgres_hook_factory is None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-not-found]

        postgres_hook_factory = PostgresHook

    hook = postgres_hook_factory(postgres_conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cursor:
            upsert_ga4_rows_on_cursor(
                cursor,
                table_name,
                column_names,
                conflict_columns,
                change_columns,
                rows,
            )
            delete_ga4_orphan_rows_on_cursor(
                cursor,
                table_name,
                column_names,
                conflict_columns,
                report_date,
                rows,
            )
        conn.commit()
    finally:
        conn.close()


def upsert_ga4_rows(
    table_name: str,
    column_names: tuple[str, ...],
    conflict_columns: tuple[str, ...],
    change_columns: tuple[str, ...],
    rows: Sequence[tuple[Any, ...]],
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Callable[[str], Any] | None = None,
) -> None:
    if not rows:
        return

    if postgres_hook_factory is None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-not-found]

        postgres_hook_factory = PostgresHook

    hook = postgres_hook_factory(postgres_conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cursor:
            upsert_ga4_rows_on_cursor(
                cursor,
                table_name,
                column_names,
                conflict_columns,
                change_columns,
                rows,
            )
        conn.commit()
    finally:
        conn.close()


def replace_ga4_rows_for_report_date(
    table_name: str,
    column_names: tuple[str, ...],
    rows: Sequence[tuple[Any, ...]],
    report_date: date | str,
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Callable[[str], Any] | None = None,
) -> None:
    if postgres_hook_factory is None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-not-found]

        postgres_hook_factory = PostgresHook

    columns = ", ".join(column_names)
    placeholders = ", ".join(["%s"] * len(column_names))
    insert_sql = f"""
        INSERT INTO {table_name} ({columns})
        VALUES ({placeholders})
    """
    hook = postgres_hook_factory(postgres_conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"DELETE FROM {table_name} WHERE report_date = %s", (ga4_report_date_literal(report_date),))
            if rows:
                cursor.executemany(insert_sql, rows)
        conn.commit()
    finally:
        conn.close()
