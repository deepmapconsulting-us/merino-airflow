"""Backfill Meta region daily snapshots from dimension-table work plans.

Manual DAG. Example conf:

{
  "start_date": "2026-01-01",
  "end_date": "2026-06-10",
  "account_ids": ["act_4157857287789311"],
  "levels": ["campaign", "adset", "ad"],
  "force": false
}

The DAG is independent: it reads existing campaign/adset/ad dimension rows from
Postgres and uses `created_at` plus the requested date range to plan work.
"""

from __future__ import annotations

import os
import sys
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag, task  # type: ignore[import-not-found]

from meta_gcs import REPORT_TIMEZONE, meta_access_token, report_datetime

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
if MODULE_PATH.exists():
    sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs.region_snapshot_backfill import (  # noqa: E402  # type: ignore[import-not-found]
    DEFAULT_CHUNK_SIZE,
    REGION_BACKFILL_LEVELS,
    existing_region_keys,
    load_dimension_rows,
    plan_region_backfill,
)
from merino_meta_jobs.traffic import (  # noqa: E402  # type: ignore[import-not-found]
    ad_region_daily_snapshot,
    adset_region_daily_snapshot,
    campaign_region_daily_snapshot,
)
from merino_meta_jobs.traffic_snapshot_rows import (  # noqa: E402  # type: ignore[import-not-found]
    ADSET_REGION_CONFLICT_COLUMNS,
    ADSET_REGION_DAILY_TABLE,
    ADSET_REGION_INSERT_COLUMNS,
    AD_REGION_CONFLICT_COLUMNS,
    AD_REGION_DAILY_TABLE,
    AD_REGION_INSERT_COLUMNS,
    CAMPAIGN_REGION_CONFLICT_COLUMNS,
    CAMPAIGN_REGION_DAILY_TABLE,
    CAMPAIGN_REGION_INSERT_COLUMNS,
    ad_region_row,
    adset_by_ad_id,
    adset_region_row,
    campaign_region_row,
    upsert_daily_rows,
)

DAG_ID = "meta_region_snapshot_backfill"
POSTGRES_CONN_ID = "merino_analytics"
DEFAULT_META_PAGE_LIMIT = 500


class BackfillStatusResolver:
    """Backfill rows are planned from dimension rows, not historical status logs."""

    def campaign_status(self, _report_date: str, _campaign_id: str) -> str:
        return "active"

    def adset_status(self, _report_date: str, _campaign_id: str, _adset_id: str) -> str:
        return "active"

    def ad_status(self, _report_date: str, _campaign_id: str, _adset_id: str, _ad_id: str) -> str:
        return "active"


@dag(
    dag_id=DAG_ID,
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz=REPORT_TIMEZONE),
    catchup=False,
    tags=["meta", "traffic", "region", "backfill"],
    default_args={
        "owner": "data-platform",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    doc_md=__doc__,
)
def meta_region_snapshot_backfill():
    @task
    def plan_backfill_work() -> dict[str, list[dict[str, Any]]]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]
        from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-not-found]

        context = get_current_context()
        conf = dict(getattr(context.get("dag_run"), "conf", None) or {})
        start_date = str(conf.get("start_date") or "").strip()
        if not start_date:
            raise ValueError("DAG conf must include start_date (YYYY-MM-DD)")

        end_date = str(conf.get("end_date") or "").strip()
        if not end_date:
            end_date = report_datetime().subtract(days=1).format("YYYY-MM-DD")

        account_ids = conf.get("account_ids") or []
        levels = conf.get("levels") or list(REGION_BACKFILL_LEVELS)
        force = bool(conf.get("force", False))

        campaign_chunk_size = int(os.environ.get("META_REGION_BACKFILL_CAMPAIGN_CHUNK_SIZE", DEFAULT_CHUNK_SIZE))
        adset_chunk_size = int(os.environ.get("META_REGION_BACKFILL_ADSET_CHUNK_SIZE", DEFAULT_CHUNK_SIZE))
        ad_chunk_size = int(os.environ.get("META_REGION_BACKFILL_AD_CHUNK_SIZE", DEFAULT_CHUNK_SIZE))

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        try:
            dimensions = load_dimension_rows(conn, account_ids=account_ids)
            existing = {} if force else existing_region_keys(conn, start_date, end_date)
        finally:
            conn.close()

        plan = plan_region_backfill(
            campaigns=dimensions["campaigns"],
            adsets=dimensions["adsets"],
            ads=dimensions["ads"],
            existing=existing,
            start_date=start_date,
            end_date=end_date,
            levels=levels,
            account_ids=account_ids,
            campaign_chunk_size=campaign_chunk_size,
            adset_chunk_size=adset_chunk_size,
            ad_chunk_size=ad_chunk_size,
            force=force,
        )
        print(
            f"{DAG_ID}: planned campaign_batches={len(plan['campaign_batches'])} "
            f"adset_batches={len(plan['adset_batches'])} ad_batches={len(plan['ad_batches'])} "
            f"start_date={start_date} end_date={end_date} levels={levels} force={force}"
        )
        return plan

    @task
    def run_campaign_region_batch(batch: dict[str, Any]) -> dict[str, Any]:
        access_token = meta_access_token()
        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        report_date = str(batch["report_date"])
        account = batch["account"]
        campaign_ids = [str(campaign_id) for campaign_id in batch.get("campaign_ids", [])]
        snapshot = campaign_region_daily_snapshot(
            access_token,
            account["id"],
            campaign_ids,
            report_date,
            page_limit=page_limit,
        )
        snapshot["config_snapshot_uri"] = f"{DAG_ID}:{report_date}"
        rows = [
            campaign_region_row(snapshot, insight, account, _snapshot_run_id("campaign_region", account["id"], report_date), report_date, BackfillStatusResolver())
            for insight in snapshot.get("insights", [])
            if insight.get("campaign_id") and insight.get("region")
        ]
        upsert_daily_rows(
            CAMPAIGN_REGION_DAILY_TABLE,
            CAMPAIGN_REGION_INSERT_COLUMNS,
            CAMPAIGN_REGION_CONFLICT_COLUMNS,
            rows,
        )
        print(f"{DAG_ID}: campaign region batch {report_date} {account['id']} rows={len(rows)}")
        return {"level": "campaign_region", "report_date": report_date, "row_count": len(rows)}

    @task
    def run_adset_region_batch(batch: dict[str, Any]) -> dict[str, Any]:
        access_token = meta_access_token()
        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        report_date = str(batch["report_date"])
        account = batch["account"]
        campaign = batch["campaign"]
        adset_ids = [str(adset_id) for adset_id in batch.get("adset_ids", [])]
        snapshot = adset_region_daily_snapshot(
            access_token,
            account["id"],
            campaign["id"],
            adset_ids,
            report_date,
            page_limit=page_limit,
        )
        snapshot["config_snapshot_uri"] = f"{DAG_ID}:{report_date}"
        rows = [
            adset_region_row(snapshot, insight, account, campaign, _snapshot_run_id("adset_region", account["id"], campaign["id"], report_date), report_date, BackfillStatusResolver())
            for insight in snapshot.get("insights", [])
            if insight.get("adset_id") and insight.get("region")
        ]
        upsert_daily_rows(
            ADSET_REGION_DAILY_TABLE,
            ADSET_REGION_INSERT_COLUMNS,
            ADSET_REGION_CONFLICT_COLUMNS,
            rows,
        )
        print(f"{DAG_ID}: adset region batch {report_date} {account['id']} {campaign['id']} rows={len(rows)}")
        return {"level": "adset_region", "report_date": report_date, "row_count": len(rows)}

    @task
    def run_ad_region_batch(batch: dict[str, Any]) -> dict[str, Any]:
        access_token = meta_access_token()
        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        report_date = str(batch["report_date"])
        account = batch["account"]
        campaign = batch["campaign"]
        ad_ids = [str(ad_id) for ad_id in batch.get("ad_ids", [])]
        snapshot = ad_region_daily_snapshot(
            access_token,
            account["id"],
            campaign["id"],
            ad_ids,
            report_date,
            page_limit=page_limit,
        )
        snapshot["config_snapshot_uri"] = f"{DAG_ID}:{report_date}"
        adset_by_id = adset_by_ad_id(campaign)
        rows = [
            ad_region_row(
                snapshot,
                insight,
                account,
                campaign,
                adset_by_id,
                _snapshot_run_id("ad_region", account["id"], campaign["id"], report_date),
                report_date,
                BackfillStatusResolver(),
            )
            for insight in snapshot.get("insights", [])
            if insight.get("ad_id") in adset_by_id and insight.get("region")
        ]
        upsert_daily_rows(AD_REGION_DAILY_TABLE, AD_REGION_INSERT_COLUMNS, AD_REGION_CONFLICT_COLUMNS, rows)
        print(f"{DAG_ID}: ad region batch {report_date} {account['id']} {campaign['id']} rows={len(rows)}")
        return {"level": "ad_region", "report_date": report_date, "row_count": len(rows)}

    @task
    def campaign_batches(plan: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
        return plan["campaign_batches"]

    @task
    def adset_batches(plan: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
        return plan["adset_batches"]

    @task
    def ad_batches(plan: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
        return plan["ad_batches"]

    plan = plan_backfill_work()
    run_campaign_region_batch.expand(batch=campaign_batches(plan))
    run_adset_region_batch.expand(batch=adset_batches(plan))
    run_ad_region_batch.expand(batch=ad_batches(plan))


def _snapshot_run_id(level: str, *ids: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, ":".join([DAG_ID, level, *ids])))


meta_region_snapshot_backfill()
