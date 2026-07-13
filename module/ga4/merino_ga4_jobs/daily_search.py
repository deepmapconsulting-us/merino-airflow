"""GA4 daily site search queries and Cloud SQL upserts."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
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

DAILY_SEARCH_TABLE = "ga4.daily_search"
DAILY_SEARCH_CONTENT_TABLE = "ga4.daily_search_content"

DAILY_SEARCH_COLUMNS = (
    "report_date",
    "source_project_id",
    "source_dataset_id",
    "source_table",
    "source_table_type",
    "is_finalized",
    "search_count",
    "search_session_count",
    "search_user_count",
    "reached_results_count",
    "reach_rate",
    "positive_results_count",
    "zero_results_count",
    "unknown_results_count",
    "purchase_after_search_session_count",
    "purchase_after_search_rate",
)
DAILY_SEARCH_CONTENT_COLUMNS = (
    "report_date",
    "search_content",
    "source_project_id",
    "source_dataset_id",
    "source_table",
    "source_table_type",
    "is_finalized",
    "search_count",
    "search_session_count",
    "search_user_count",
    "reached_results_count",
    "reach_rate",
    "positive_results_count",
    "zero_results_count",
    "unknown_results_count",
    "avg_results_found",
    "max_results_found",
    "purchase_after_search_session_count",
    "purchase_after_search_rate",
)

DAILY_SEARCH_CONFLICT_COLUMNS = ("report_date",)
DAILY_SEARCH_CONTENT_CONFLICT_COLUMNS = ("report_date", "search_content")
DAILY_SEARCH_CHANGE_COLUMNS = tuple(column for column in DAILY_SEARCH_COLUMNS if column != "report_date")
DAILY_SEARCH_CONTENT_CHANGE_COLUMNS = tuple(
    column
    for column in DAILY_SEARCH_CONTENT_COLUMNS
    if column not in DAILY_SEARCH_CONTENT_CONFLICT_COLUMNS
)


def _search_session_ctes(
    report_date: date | str,
    *,
    source_table: str | None,
    source_table_type: str,
) -> tuple[str, str]:
    report_date_sql = ga4_report_date_literal(report_date)
    checked_source_table_type = ga4_source_table_type(source_table_type)
    resolved_source_table = source_table or ga4_source_table(
        report_date,
        intraday=checked_source_table_type == "intraday",
    )
    ctes = f"""
WITH event_rows AS (
  SELECT
    DATE '{report_date_sql}' AS report_date,
    user_pseudo_id,
    event_name,
    event_timestamp,
    (
      SELECT value.int_value
      FROM UNNEST(event_params)
      WHERE key = 'ga_session_id'
    ) AS ga_session_id,
    (
      SELECT value.string_value
      FROM UNNEST(event_params)
      WHERE key = 'page_title'
    ) AS page_title,
    LOWER(TRIM(COALESCE(
      NULLIF((
        SELECT value.string_value
        FROM UNNEST(event_params)
        WHERE key = 'search_term'
      ), ''),
      NULLIF((
        SELECT value.string_value
        FROM UNNEST(event_params)
        WHERE key = 'term'
      ), '')
    ))) AS event_search_term
  FROM `{resolved_source_table}`
  WHERE user_pseudo_id IS NOT NULL
),

search_events AS (
  SELECT
    report_date,
    user_pseudo_id,
    ga_session_id,
    event_name,
    event_timestamp,
    COALESCE(
      LOWER(TRIM(REGEXP_EXTRACT(
        page_title,
        r'(?i)Search:\\s*\\d+\\s+results?\\s+found\\s+for\\s+"([^"]+)"'
      ))),
      event_search_term
    ) AS search_content,
    SAFE_CAST(REGEXP_EXTRACT(page_title, r'(?i)Search:\\s*(\\d+)\\s+results?\\s+found') AS INT64)
      AS results_found
  FROM event_rows
  WHERE event_name IN ('search', 'view_search_results')
    AND ga_session_id IS NOT NULL
),

search_term_sessions AS (
  SELECT
    report_date,
    user_pseudo_id,
    ga_session_id,
    search_content,
    MIN(event_timestamp) AS first_search_ts,
    LOGICAL_OR(event_name = 'view_search_results') AS saw_results_page,
    MAX(results_found) AS results_found
  FROM search_events
  WHERE search_content IS NOT NULL
    AND search_content != ''
  GROUP BY 1, 2, 3, 4
),

search_term_outcomes AS (
  SELECT
    search_term_sessions.*,
    LOGICAL_OR(event_rows.event_name = 'purchase') AS purchased_after_search
  FROM search_term_sessions
  LEFT JOIN event_rows
    ON event_rows.user_pseudo_id = search_term_sessions.user_pseudo_id
   AND event_rows.ga_session_id = search_term_sessions.ga_session_id
   AND event_rows.event_timestamp >= search_term_sessions.first_search_ts
  GROUP BY 1, 2, 3, 4, 5, 6, 7
),

search_sessions AS (
  SELECT
    report_date,
    user_pseudo_id,
    ga_session_id,
    MIN(first_search_ts) AS first_search_ts
  FROM search_term_sessions
  GROUP BY 1, 2, 3
),

search_session_outcomes AS (
  SELECT
    search_sessions.*,
    LOGICAL_OR(event_rows.event_name = 'purchase') AS purchased_after_search
  FROM search_sessions
  LEFT JOIN event_rows
    ON event_rows.user_pseudo_id = search_sessions.user_pseudo_id
   AND event_rows.ga_session_id = search_sessions.ga_session_id
   AND event_rows.event_timestamp >= search_sessions.first_search_ts
  GROUP BY 1, 2, 3, 4
)
""".strip()
    return ctes, resolved_source_table


def daily_search_query(
    report_date: date | str,
    *,
    source_table: str | None = None,
    source_table_type: str = "finalized",
) -> str:
    checked_source_table_type = ga4_source_table_type(source_table_type)
    ctes, resolved_source_table = _search_session_ctes(
        report_date,
        source_table=source_table,
        source_table_type=checked_source_table_type,
    )
    return f"""
{ctes}

SELECT
  search_term_outcomes.report_date,
  '{PROJECT_ID}' AS source_project_id,
  '{DATASET_ID}' AS source_dataset_id,
  '{resolved_source_table}' AS source_table,
  '{checked_source_table_type}' AS source_table_type,
  {ga4_is_finalized_sql(checked_source_table_type)} AS is_finalized,
  CAST(COUNT(*) AS INT64) AS search_count,
  CAST(COUNT(DISTINCT CONCAT(
    search_term_outcomes.user_pseudo_id,
    '#',
    CAST(search_term_outcomes.ga_session_id AS STRING)
  )) AS INT64) AS search_session_count,
  CAST(COUNT(DISTINCT search_term_outcomes.user_pseudo_id) AS INT64) AS search_user_count,
  CAST(COUNTIF(saw_results_page) AS INT64) AS reached_results_count,
  ROUND(SAFE_DIVIDE(COUNTIF(saw_results_page), COUNT(*)), 6) AS reach_rate,
  CAST(COUNTIF(results_found > 0) AS INT64) AS positive_results_count,
  CAST(COUNTIF(results_found = 0) AS INT64) AS zero_results_count,
  CAST(COUNTIF(results_found IS NULL) AS INT64) AS unknown_results_count,
  CAST(COUNT(DISTINCT IF(
    search_session_outcomes.purchased_after_search,
    CONCAT(
      search_session_outcomes.user_pseudo_id,
      '#',
      CAST(search_session_outcomes.ga_session_id AS STRING)
    ),
    NULL
  )) AS INT64) AS purchase_after_search_session_count,
  ROUND(SAFE_DIVIDE(
    COUNT(DISTINCT IF(
      search_session_outcomes.purchased_after_search,
      CONCAT(
        search_session_outcomes.user_pseudo_id,
        '#',
        CAST(search_session_outcomes.ga_session_id AS STRING)
      ),
      NULL
    )),
    COUNT(DISTINCT CONCAT(
      search_session_outcomes.user_pseudo_id,
      '#',
      CAST(search_session_outcomes.ga_session_id AS STRING)
    ))
  ), 6) AS purchase_after_search_rate
FROM search_term_outcomes
JOIN search_session_outcomes
  ON search_session_outcomes.report_date = search_term_outcomes.report_date
 AND search_session_outcomes.user_pseudo_id = search_term_outcomes.user_pseudo_id
 AND search_session_outcomes.ga_session_id = search_term_outcomes.ga_session_id
GROUP BY 1, 2, 3, 4, 5, 6
""".strip()


def daily_search_content_query(
    report_date: date | str,
    *,
    source_table: str | None = None,
    source_table_type: str = "finalized",
) -> str:
    checked_source_table_type = ga4_source_table_type(source_table_type)
    ctes, resolved_source_table = _search_session_ctes(
        report_date,
        source_table=source_table,
        source_table_type=checked_source_table_type,
    )
    return f"""
{ctes}

SELECT
  report_date,
  search_content,
  '{PROJECT_ID}' AS source_project_id,
  '{DATASET_ID}' AS source_dataset_id,
  '{resolved_source_table}' AS source_table,
  '{checked_source_table_type}' AS source_table_type,
  {ga4_is_finalized_sql(checked_source_table_type)} AS is_finalized,
  CAST(COUNT(*) AS INT64) AS search_count,
  CAST(COUNT(DISTINCT CONCAT(user_pseudo_id, '#', CAST(ga_session_id AS STRING))) AS INT64)
    AS search_session_count,
  CAST(COUNT(DISTINCT user_pseudo_id) AS INT64) AS search_user_count,
  CAST(COUNTIF(saw_results_page) AS INT64) AS reached_results_count,
  ROUND(SAFE_DIVIDE(COUNTIF(saw_results_page), COUNT(*)), 6) AS reach_rate,
  CAST(COUNTIF(results_found > 0) AS INT64) AS positive_results_count,
  CAST(COUNTIF(results_found = 0) AS INT64) AS zero_results_count,
  CAST(COUNTIF(results_found IS NULL) AS INT64) AS unknown_results_count,
  ROUND(AVG(IF(results_found IS NOT NULL, results_found, NULL)), 6) AS avg_results_found,
  CAST(MAX(results_found) AS INT64) AS max_results_found,
  CAST(COUNTIF(purchased_after_search) AS INT64) AS purchase_after_search_session_count,
  ROUND(SAFE_DIVIDE(COUNTIF(purchased_after_search), COUNT(*)), 6) AS purchase_after_search_rate
FROM search_term_outcomes
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY search_count DESC, search_content
""".strip()


def daily_search_row(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(column) for column in DAILY_SEARCH_COLUMNS)


def daily_search_content_row(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(column) for column in DAILY_SEARCH_CONTENT_COLUMNS)


def upsert_daily_search(
    rows: Sequence[Mapping[str, Any]],
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Callable[[str], Any] | None = None,
) -> None:
    upsert_ga4_rows(
        DAILY_SEARCH_TABLE,
        DAILY_SEARCH_COLUMNS,
        DAILY_SEARCH_CONFLICT_COLUMNS,
        DAILY_SEARCH_CHANGE_COLUMNS,
        [daily_search_row(row) for row in rows],
        postgres_conn_id=postgres_conn_id,
        postgres_hook_factory=postgres_hook_factory,
    )


def replace_daily_search(
    rows: Sequence[Mapping[str, Any]],
    report_date: date | str,
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Callable[[str], Any] | None = None,
) -> None:
    replace_ga4_rows_for_report_date(
        DAILY_SEARCH_TABLE,
        DAILY_SEARCH_COLUMNS,
        [daily_search_row(row) for row in rows],
        report_date,
        postgres_conn_id=postgres_conn_id,
        postgres_hook_factory=postgres_hook_factory,
    )


def merge_daily_search(
    rows: Sequence[Mapping[str, Any]],
    report_date: date | str,
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Callable[[str], Any] | None = None,
) -> None:
    merge_ga4_rows_for_report_date(
        DAILY_SEARCH_TABLE,
        DAILY_SEARCH_COLUMNS,
        DAILY_SEARCH_CONFLICT_COLUMNS,
        DAILY_SEARCH_CHANGE_COLUMNS,
        [daily_search_row(row) for row in rows],
        report_date,
        postgres_conn_id=postgres_conn_id,
        postgres_hook_factory=postgres_hook_factory,
    )


def merge_daily_search_content(
    rows: Sequence[Mapping[str, Any]],
    report_date: date | str,
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Callable[[str], Any] | None = None,
) -> None:
    merge_ga4_rows_for_report_date(
        DAILY_SEARCH_CONTENT_TABLE,
        DAILY_SEARCH_CONTENT_COLUMNS,
        DAILY_SEARCH_CONTENT_CONFLICT_COLUMNS,
        DAILY_SEARCH_CONTENT_CHANGE_COLUMNS,
        [daily_search_content_row(row) for row in rows],
        report_date,
        postgres_conn_id=postgres_conn_id,
        postgres_hook_factory=postgres_hook_factory,
    )
