#!/usr/bin/env python3
"""Load ordered GA4 session event steps from BigQuery into ga4.experiments."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any

MODULE_PATH = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_PATH))

from merino_ga4_jobs.daily_analysis import PROJECT_ID  # noqa: E402
from merino_ga4_jobs.experiments import (  # noqa: E402
    ga4_source_table,
    session_event_steps_query,
    upsert_experiments,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-name", required=True)
    parser.add_argument("--report-date", required=True, help="YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--intraday", action="store_true")
    parser.add_argument("--source-table", default=None)
    parser.add_argument("--user-pseudo-id", default=None)
    parser.add_argument("--ga-session-id", type=int, default=None)
    parser.add_argument("--min-session-events", type=int, default=1)
    parser.add_argument("--session-limit", type=int, default=None)
    parser.add_argument("--postgres-conn-id", default="merino_analytics")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def parse_report_date(value: str) -> date:
    if len(value) == 8 and value.isdigit():
        return date(int(value[0:4]), int(value[4:6]), int(value[6:8]))
    return date.fromisoformat(value)


def run_bq_query(sql: str) -> list[dict[str, Any]]:
    command = [
        "bq",
        "query",
        "--use_legacy_sql=false",
        "--format=json",
        "--max_rows=100000",
        "--project_id",
        PROJECT_ID,
        sql,
    ]
    completed = subprocess.run(command, check=True, capture_output=True, text=True)
    if not completed.stdout.strip():
        return []
    return json.loads(completed.stdout)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report_date = parse_report_date(args.report_date)
    source_table = args.source_table or ga4_source_table(report_date, intraday=args.intraday)
    sql = session_event_steps_query(
        experiment_name=args.experiment_name,
        report_date=report_date,
        source_table=source_table,
        user_pseudo_id=args.user_pseudo_id,
        ga_session_id=args.ga_session_id,
        min_session_events=args.min_session_events,
        session_limit=args.session_limit,
    )

    if args.dry_run:
        print(sql)
        return 0

    rows = run_bq_query(sql)
    if not rows:
        print(f"No rows returned for experiment_name={args.experiment_name}")
        return 0

    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        upsert_experiments(rows, database_url=database_url)
    else:
        from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-not-found]

        upsert_experiments(
            rows,
            postgres_conn_id=args.postgres_conn_id,
            postgres_hook_factory=PostgresHook,
        )

    session_keys = {
        (row["user_pseudo_id"], row["ga_session_id"], row["session_event_count"])
        for row in rows
    }
    print(
        f"Loaded experiment_name={args.experiment_name} "
        f"source_table={source_table} rows={len(rows)} sessions={len(session_keys)}"
    )
    for user_pseudo_id, ga_session_id, session_event_count in sorted(session_keys):
        print(
            f"  user_pseudo_id={user_pseudo_id} ga_session_id={ga_session_id} "
            f"session_event_count={session_event_count}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
