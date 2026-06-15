"""Export ordered GA4 session event steps from BigQuery into ga4.experiments."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

from merino_ga4_jobs.daily_analysis import (
    DATASET_ID,
    POSTGRES_CONN_ID,
    PROJECT_ID,
    ga4_report_date_literal,
    ga4_table_suffix,
    upsert_ga4_rows,
)

EXPERIMENTS_TABLE = "ga4.experiments"
EXPERIMENTS_COLUMNS = (
    "experiment_name",
    "report_date",
    "source_project_id",
    "source_dataset_id",
    "source_table",
    "user_pseudo_id",
    "ga_session_id",
    "session_event_count",
    "session_purchase_count",
    "first_purchase_step",
    "event_step",
    "event_name",
    "event_timestamp_micros",
    "event_at",
    "page_location",
    "page_path",
)
EXPERIMENTS_CONFLICT_COLUMNS = ("experiment_name", "user_pseudo_id", "ga_session_id", "event_step")
EXPERIMENTS_CHANGE_COLUMNS = tuple(
    column for column in EXPERIMENTS_COLUMNS if column not in EXPERIMENTS_CONFLICT_COLUMNS
)


def ga4_source_table(report_date: date | str, *, intraday: bool = False) -> str:
    prefix = "events_intraday" if intraday else "events"
    return f"{PROJECT_ID}.{DATASET_ID}.{prefix}_{ga4_table_suffix(report_date)}"


def _sql_string(value: str) -> str:
    if "\x00" in value:
        raise ValueError("GA4 string literals cannot contain NUL bytes")
    return value.replace("\\", "\\\\").replace("'", "''")


def session_event_steps_query(
    *,
    experiment_name: str,
    report_date: date | str,
    source_table: str | None = None,
    user_pseudo_id: str | None = None,
    ga_session_id: int | None = None,
    min_session_events: int = 1,
    session_limit: int | None = None,
) -> str:
    """Return BigQuery SQL for ordered session events to load into ga4.experiments."""
    if not experiment_name.strip():
        raise ValueError("experiment_name is required")

    report_date_sql = ga4_report_date_literal(report_date)
    resolved_source_table = source_table or ga4_source_table(report_date)
    user_filter = ""
    if user_pseudo_id is not None:
        user_filter = f"AND user_pseudo_id = '{_sql_string(user_pseudo_id)}'"
    session_filter = ""
    if ga_session_id is not None:
        session_filter = f"AND ga_session_id = {int(ga_session_id)}"
    session_limit_sql = ""
    if session_limit is not None:
        session_limit_sql = f"LIMIT {int(session_limit)}"

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
  {user_filter}
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
    COUNTIF(event_name = 'purchase') AS session_purchase_count,
    MIN(IF(event_name = 'purchase', event_step, NULL)) AS first_purchase_step
  FROM ordered_events
  GROUP BY 1, 2, 3
  HAVING COUNT(*) >= {int(min_session_events)}
),

selected_sessions AS (
  SELECT *
  FROM sessions
  WHERE 1 = 1
  {session_filter}
  ORDER BY session_purchase_count DESC, session_event_count DESC
  {session_limit_sql}
)

SELECT
  '{_sql_string(experiment_name)}' AS experiment_name,
  s.report_date,
  '{PROJECT_ID}' AS source_project_id,
  '{DATASET_ID}' AS source_dataset_id,
  '{resolved_source_table}' AS source_table,
  o.user_pseudo_id,
  o.ga_session_id,
  s.session_event_count,
  s.session_purchase_count,
  s.first_purchase_step,
  o.event_step,
  o.event_name,
  o.event_timestamp AS event_timestamp_micros,
  TIMESTAMP(DATETIME(TIMESTAMP_MICROS(o.event_timestamp), 'America/Los_Angeles')) AS event_at,
  o.page_location,
  REGEXP_EXTRACT(o.page_location, r'https?://[^/]+(/[^?#]*)') AS page_path
FROM ordered_events o
JOIN selected_sessions s
  ON o.user_pseudo_id = s.user_pseudo_id
 AND o.ga_session_id = s.ga_session_id
ORDER BY o.user_pseudo_id, o.ga_session_id, o.event_step
""".strip()


def experiment_row(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(column) for column in EXPERIMENTS_COLUMNS)


def upsert_experiments(
    rows: Sequence[Mapping[str, Any]],
    *,
    postgres_conn_id: str = POSTGRES_CONN_ID,
    postgres_hook_factory: Any | None = None,
    database_url: str | None = None,
) -> None:
    payload = [experiment_row(row) for row in rows]
    if database_url:
        import psycopg  # type: ignore[import-not-found]

        columns = ", ".join(EXPERIMENTS_COLUMNS)
        placeholders = ", ".join(["%s"] * len(EXPERIMENTS_COLUMNS))
        conflict_target = ", ".join(EXPERIMENTS_CONFLICT_COLUMNS)
        update_columns = [
            column
            for column in EXPERIMENTS_COLUMNS
            if column not in EXPERIMENTS_CONFLICT_COLUMNS and column != "report_date"
        ]
        assignments = ",\n            ".join(
            [
                "record_updated_at = now()",
                "update_count = ga4.experiments.update_count + 1",
                *[f"{column} = EXCLUDED.{column}" for column in update_columns],
            ]
        )
        changed = "\n            OR ".join(
            f"ga4.experiments.{column} IS DISTINCT FROM EXCLUDED.{column}"
            for column in EXPERIMENTS_CHANGE_COLUMNS
        )
        sql = f"""
            INSERT INTO {EXPERIMENTS_TABLE} ({columns})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_target}) DO UPDATE
            SET {assignments}
            WHERE {changed}
        """
        with psycopg.connect(database_url, autocommit=False) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, payload)
            conn.commit()
        return

    upsert_ga4_rows(
        EXPERIMENTS_TABLE,
        EXPERIMENTS_COLUMNS,
        EXPERIMENTS_CONFLICT_COLUMNS,
        EXPERIMENTS_CHANGE_COLUMNS,
        payload,
        postgres_conn_id=postgres_conn_id,
        postgres_hook_factory=postgres_hook_factory,
    )
