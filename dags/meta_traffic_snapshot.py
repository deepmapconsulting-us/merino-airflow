"""Sync daily Meta traffic snapshots from campaign-config-driven work.

Meta access token is read from Airflow Variable `meta_access_token` (GSM:
`airflow-variables-meta_access_token`), with fallback to env `META_ACCESS_TOKEN`.
The visible DAG hierarchy is built from the latest successful
`facebook_campaign_config_update` GCS snapshot. Each run writes current-day and
yesterday daily snapshots to Postgres and updates rows only when metrics change.
"""

from __future__ import annotations

import os
import re
import sys
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum  # type: ignore[import-not-found]
from pendulum.parsing.exceptions import ParserError  # type: ignore[import-not-found]
from airflow.sdk import dag, task  # type: ignore[import-not-found]

try:
    from airflow.providers.standard.sensors.external_task import ExternalTaskSensor  # type: ignore[import-not-found]
except ImportError:
    from airflow.sensors.external_task import ExternalTaskSensor  # type: ignore[import-not-found,no-redef]

from meta_gcs import (
    REPORT_TIMEZONE,
    campaign_config_logical_date,
    gcs_console_link,
    gcs_uri,
    latest_object_name,
    meta_access_token,
    read_json_from_gcs,
    read_latest_snapshot_pointer,
    report_datetime,
    env_config_value,
)
from meta_status import DailyStatusResolver

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
if MODULE_PATH.exists():
    sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs.traffic import (  # noqa: E402  # type: ignore[import-not-found]
    DEFAULT_TRAFFIC_LOOKUP_WINDOW_DAYS,
    ad_daily_snapshot,
    ad_gender_age_daily_snapshot,
    ad_ids_from_config,
    ad_region_daily_snapshot,
    adset_daily_snapshot,
    adset_region_daily_snapshot,
    campaign_daily_snapshot,
    campaign_region_daily_snapshot,
    delivered_ad_hierarchy,
    traffic_accounts_from_config,
)
from merino_meta_jobs.traffic_snapshot_rows import (  # noqa: E402  # type: ignore[import-not-found]
    ADSET_CONFLICT_COLUMNS,
    ADSET_DAILY_TABLE,
    ADSET_INSERT_COLUMNS,
    ADSET_REGION_CONFLICT_COLUMNS,
    ADSET_REGION_DAILY_TABLE,
    ADSET_REGION_INSERT_COLUMNS,
    AD_CONFLICT_COLUMNS,
    AD_DAILY_TABLE,
    AD_GENDER_AGE_CONFLICT_COLUMNS,
    AD_GENDER_AGE_DAILY_TABLE,
    AD_GENDER_AGE_INSERT_COLUMNS,
    AD_INSERT_COLUMNS,
    AD_REGION_CONFLICT_COLUMNS,
    AD_REGION_DAILY_TABLE,
    AD_REGION_INSERT_COLUMNS,
    CAMPAIGN_CONFLICT_COLUMNS,
    CAMPAIGN_DAILY_TABLE,
    CAMPAIGN_INSERT_COLUMNS,
    CAMPAIGN_REGION_CONFLICT_COLUMNS,
    CAMPAIGN_REGION_DAILY_TABLE,
    CAMPAIGN_REGION_INSERT_COLUMNS,
    ad_gender_age_row as _ad_gender_age_row,
    ad_region_row as _ad_region_row,
    ad_row as _ad_row,
    adset_by_ad_id as _adset_by_ad_id,
    adset_region_row as _adset_region_row,
    adset_row as _adset_row,
    campaign_region_row as _campaign_region_row,
    campaign_row as _campaign_row,
    upsert_daily_rows as _upsert_daily_rows,
)

DAG_ID = "meta_traffic_snapshot"
CAMPAIGN_CONFIG_DAG_ID = "facebook_campaign_config_update"
CONFIG_GCS_PREFIX = "facebook_campaign_config_update"
ACTIVE_ACCOUNTS_ENV = "FACEBOOK_ACTIVE_ACCOUNTS"
LOOKUP_WINDOW_ENV = "FACEBOOK_TRAFFIC_LOOKUP_WINDOWS"
DEFAULT_META_PAGE_LIMIT = 500


@dag(
    dag_id=DAG_ID,
    schedule="0 2,14 * * *",
    start_date=pendulum.datetime(2026, 1, 1, 2, 0, tz=REPORT_TIMEZONE),
    catchup=False,
    tags=["meta", "traffic", "daily-snapshot"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    doc_md=__doc__,
)
def meta_traffic_snapshot():
    config_source = _campaign_config_for_display()
    config_log = _config_log_payload(config_source)

    @task
    def log_campaign_config_source(source: dict[str, Any]) -> None:
        print(f"{DAG_ID}: config pointer: {source['pointer_uri']}")
        print(f"{DAG_ID}: config pointer link: {source['pointer_link']}")
        if source.get("snapshot_uri"):
            print(f"{DAG_ID}: config snapshot: {source['snapshot_uri']}")
            print(f"{DAG_ID}: config snapshot link: {source['snapshot_link']}")
        if source.get("error"):
            print(f"{DAG_ID}: campaign config unavailable during DAG parse: {source['error']}")
        else:
            print(
                f"{DAG_ID}: displaying {source['account_count']} accounts, "
                f"{source.get('campaign_count', 0)} campaigns, {source['adset_count']} adsets "
                "from campaign config"
            )

    @task
    def no_campaigns_from_campaign_config(source: dict[str, Any]) -> None:
        print(
            f"{DAG_ID}: no campaign traffic tasks were created. "
            f"pointer={source['pointer_uri']} snapshot={source.get('snapshot_uri') or '<none>'}"
        )
        if source.get("error"):
            raise RuntimeError(source["error"])

    @task
    def sync_daily_reports_from_insights(source: dict[str, Any]) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        access_token = meta_access_token()
        context = get_current_context()
        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        report_dates = _report_dates(_context_logical_date(context))
        accounts = delivered_ad_hierarchy(
            access_token,
            report_dates,
            active_accounts_value=env_config_value(ACTIVE_ACCOUNTS_ENV),
            page_limit=page_limit,
        )
        if not accounts:
            print(f"{DAG_ID}: no delivered ads found for report_dates={report_dates}")
            return {"account_count": 0, "row_count": 0}

        status_resolver = _daily_status_resolver()
        total_rows = 0
        for account in accounts:
            campaign_ids = [campaign["id"] for campaign in account.get("campaigns", []) if campaign.get("id")]
            campaign_snapshots = [
                campaign_daily_snapshot(
                    access_token,
                    account["id"],
                    campaign_ids,
                    report_date,
                    page_limit=page_limit,
                )
                for report_date in report_dates
            ]
            for snapshot in campaign_snapshots:
                snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
            campaign_rows = [
                _campaign_row(
                    snapshot,
                    insight,
                    account,
                    _snapshot_run_id(context["run_id"], "campaign", account["id"]),
                    _snapshot_report_date(_context_logical_date(context)),
                    status_resolver,
                )
                for snapshot in campaign_snapshots
                for insight in snapshot.get("insights", [])
                if insight.get("campaign_id")
            ]
            _upsert_daily_rows(CAMPAIGN_DAILY_TABLE, CAMPAIGN_INSERT_COLUMNS, CAMPAIGN_CONFLICT_COLUMNS, campaign_rows)
            total_rows += len(campaign_rows)

            campaign_region_snapshots = [
                campaign_region_daily_snapshot(
                    access_token,
                    account["id"],
                    campaign_ids,
                    report_date,
                    page_limit=page_limit,
                )
                for report_date in report_dates
            ]
            for snapshot in campaign_region_snapshots:
                snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
            campaign_region_rows = [
                _campaign_region_row(
                    snapshot,
                    insight,
                    account,
                    _snapshot_run_id(context["run_id"], "campaign_region", account["id"]),
                    _snapshot_report_date(_context_logical_date(context)),
                    status_resolver,
                )
                for snapshot in campaign_region_snapshots
                for insight in snapshot.get("insights", [])
                if insight.get("campaign_id") and insight.get("region")
            ]
            _upsert_daily_rows(
                CAMPAIGN_REGION_DAILY_TABLE,
                CAMPAIGN_REGION_INSERT_COLUMNS,
                CAMPAIGN_REGION_CONFLICT_COLUMNS,
                campaign_region_rows,
            )
            total_rows += len(campaign_region_rows)

            for campaign in account.get("campaigns", []):
                adset_ids = [adset["id"] for adset in campaign.get("adsets", []) if adset.get("id")]
                ad_ids = [
                    ad_id
                    for adset in campaign.get("adsets", [])
                    for ad_id in ad_ids_from_config(adset)
                ]
                report_date = _snapshot_report_date(_context_logical_date(context))

                adset_snapshots = [
                    adset_daily_snapshot(
                        access_token,
                        account["id"],
                        campaign["id"],
                        adset_ids,
                        metric_date,
                        page_limit=page_limit,
                    )
                    for metric_date in report_dates
                ]
                for snapshot in adset_snapshots:
                    snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
                adset_rows = [
                    _adset_row(
                        snapshot,
                        insight,
                        account,
                        campaign,
                        _snapshot_run_id(context["run_id"], "adset", account["id"], campaign["id"]),
                        report_date,
                        status_resolver,
                    )
                    for snapshot in adset_snapshots
                    for insight in snapshot.get("insights", [])
                    if insight.get("adset_id")
                ]
                _upsert_daily_rows(ADSET_DAILY_TABLE, ADSET_INSERT_COLUMNS, ADSET_CONFLICT_COLUMNS, adset_rows)
                total_rows += len(adset_rows)

                adset_region_snapshots = [
                    adset_region_daily_snapshot(
                        access_token,
                        account["id"],
                        campaign["id"],
                        adset_ids,
                        metric_date,
                        page_limit=page_limit,
                    )
                    for metric_date in report_dates
                ]
                for snapshot in adset_region_snapshots:
                    snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
                adset_region_rows = [
                    _adset_region_row(
                        snapshot,
                        insight,
                        account,
                        campaign,
                        _snapshot_run_id(context["run_id"], "adset_region", account["id"], campaign["id"]),
                        report_date,
                        status_resolver,
                    )
                    for snapshot in adset_region_snapshots
                    for insight in snapshot.get("insights", [])
                    if insight.get("adset_id") and insight.get("region")
                ]
                _upsert_daily_rows(
                    ADSET_REGION_DAILY_TABLE,
                    ADSET_REGION_INSERT_COLUMNS,
                    ADSET_REGION_CONFLICT_COLUMNS,
                    adset_region_rows,
                )
                total_rows += len(adset_region_rows)

                adset_by_ad_id = _adset_by_ad_id(campaign)
                ad_snapshots = [
                    ad_daily_snapshot(
                        access_token,
                        account["id"],
                        campaign["id"],
                        ad_ids,
                        metric_date,
                        page_limit=page_limit,
                    )
                    for metric_date in report_dates
                ]
                for snapshot in ad_snapshots:
                    snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
                ad_rows = [
                    _ad_row(
                        snapshot,
                        insight,
                        account,
                        campaign,
                        adset_by_ad_id,
                        _snapshot_run_id(context["run_id"], "ad", account["id"], campaign["id"]),
                        report_date,
                        status_resolver,
                    )
                    for snapshot in ad_snapshots
                    for insight in snapshot.get("insights", [])
                    if insight.get("ad_id") in adset_by_ad_id
                ]
                _upsert_daily_rows(AD_DAILY_TABLE, AD_INSERT_COLUMNS, AD_CONFLICT_COLUMNS, ad_rows)
                total_rows += len(ad_rows)

                ad_region_snapshots = [
                    ad_region_daily_snapshot(
                        access_token,
                        account["id"],
                        campaign["id"],
                        ad_ids,
                        metric_date,
                        page_limit=page_limit,
                    )
                    for metric_date in report_dates
                ]
                for snapshot in ad_region_snapshots:
                    snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
                ad_region_rows = [
                    _ad_region_row(
                        snapshot,
                        insight,
                        account,
                        campaign,
                        adset_by_ad_id,
                        _snapshot_run_id(context["run_id"], "ad_region", account["id"], campaign["id"]),
                        report_date,
                        status_resolver,
                    )
                    for snapshot in ad_region_snapshots
                    for insight in snapshot.get("insights", [])
                    if insight.get("ad_id") in adset_by_ad_id and insight.get("region")
                ]
                _upsert_daily_rows(
                    AD_REGION_DAILY_TABLE,
                    AD_REGION_INSERT_COLUMNS,
                    AD_REGION_CONFLICT_COLUMNS,
                    ad_region_rows,
                )
                total_rows += len(ad_region_rows)

                ad_gender_age_snapshots = [
                    ad_gender_age_daily_snapshot(
                        access_token,
                        account["id"],
                        campaign["id"],
                        ad_ids,
                        metric_date,
                        page_limit=page_limit,
                    )
                    for metric_date in report_dates
                ]
                for snapshot in ad_gender_age_snapshots:
                    snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
                ad_gender_age_rows = [
                    _ad_gender_age_row(
                        snapshot,
                        insight,
                        account,
                        campaign,
                        adset_by_ad_id,
                        _snapshot_run_id(context["run_id"], "ad_gender_age", account["id"], campaign["id"]),
                        report_date,
                        status_resolver,
                    )
                    for snapshot in ad_gender_age_snapshots
                    for insight in snapshot.get("insights", [])
                    if insight.get("ad_id") in adset_by_ad_id
                    and insight.get("age")
                    and insight.get("gender")
                ]
                _upsert_daily_rows(
                    AD_GENDER_AGE_DAILY_TABLE,
                    AD_GENDER_AGE_INSERT_COLUMNS,
                    AD_GENDER_AGE_CONFLICT_COLUMNS,
                    ad_gender_age_rows,
                )
                total_rows += len(ad_gender_age_rows)

        print(
            f"{DAG_ID}: synced {total_rows} daily report rows from "
            f"{_campaign_count(accounts)} delivered campaigns and {_adset_count(accounts)} delivered adsets"
        )
        return {"account_count": len(accounts), "row_count": total_rows}

    @task
    def pull_campaign_snapshots(account: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        access_token = meta_access_token()
        context = get_current_context()
        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        campaign_ids = [campaign["id"] for campaign in account.get("campaigns", []) if campaign.get("id")]
        snapshots = [
            campaign_daily_snapshot(
                access_token,
                account["id"],
                campaign_ids,
                report_date,
                page_limit=page_limit,
            )
            for report_date in _report_dates(_context_logical_date(context))
        ]
        for snapshot in snapshots:
            snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
        print(
            f"{DAG_ID}: pulled {sum(len(snapshot['insights']) for snapshot in snapshots)} "
            f"campaign daily rows for account={account['id']}"
        )
        return {"snapshots": snapshots}

    @task
    def write_campaign_snapshots(
        campaign_snapshots: dict[str, Any],
        account: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        context = get_current_context()
        snapshot_run_id = _snapshot_run_id(context["run_id"], "campaign", account["id"])
        report_date = _snapshot_report_date(_context_logical_date(context))
        status_resolver = _daily_status_resolver()
        rows = [
            _campaign_row(snapshot, insight, account, snapshot_run_id, report_date, status_resolver)
            for snapshot in campaign_snapshots.get("snapshots", [])
            for insight in snapshot.get("insights", [])
            if insight.get("campaign_id")
        ]
        _upsert_daily_rows(CAMPAIGN_DAILY_TABLE, CAMPAIGN_INSERT_COLUMNS, CAMPAIGN_CONFLICT_COLUMNS, rows)
        print(f"{DAG_ID}: upserted {len(rows)} campaign daily rows for account={account['id']}")
        return {"level": "campaign", "row_count": len(rows), "account_id": account["id"]}

    @task
    def pull_campaign_region_snapshots(account: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        access_token = meta_access_token()
        context = get_current_context()
        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        campaign_ids = [campaign["id"] for campaign in account.get("campaigns", []) if campaign.get("id")]
        snapshots = [
            campaign_region_daily_snapshot(
                access_token,
                account["id"],
                campaign_ids,
                report_date,
                page_limit=page_limit,
            )
            for report_date in _report_dates(_context_logical_date(context))
        ]
        for snapshot in snapshots:
            snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
        print(
            f"{DAG_ID}: pulled {sum(len(snapshot['insights']) for snapshot in snapshots)} "
            f"campaign region daily rows for account={account['id']}"
        )
        return {"snapshots": snapshots}

    @task
    def write_campaign_region_snapshots(
        campaign_region_snapshots: dict[str, Any],
        account: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        context = get_current_context()
        snapshot_run_id = _snapshot_run_id(context["run_id"], "campaign_region", account["id"])
        report_date = _snapshot_report_date(_context_logical_date(context))
        status_resolver = _daily_status_resolver()
        rows = [
            _campaign_region_row(snapshot, insight, account, snapshot_run_id, report_date, status_resolver)
            for snapshot in campaign_region_snapshots.get("snapshots", [])
            for insight in snapshot.get("insights", [])
            if insight.get("campaign_id") and insight.get("region")
        ]
        _upsert_daily_rows(
            CAMPAIGN_REGION_DAILY_TABLE,
            CAMPAIGN_REGION_INSERT_COLUMNS,
            CAMPAIGN_REGION_CONFLICT_COLUMNS,
            rows,
        )
        print(f"{DAG_ID}: upserted {len(rows)} campaign region daily rows for account={account['id']}")
        return {"level": "campaign_region", "row_count": len(rows), "account_id": account["id"]}

    @task
    def pull_adset_snapshots(
        account: dict[str, Any],
        campaign: dict[str, Any],
        source: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        access_token = meta_access_token()
        context = get_current_context()
        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        adset_ids = [adset["id"] for adset in campaign.get("adsets", []) if adset.get("id")]
        snapshots = [
            adset_daily_snapshot(
                access_token,
                account["id"],
                campaign["id"],
                adset_ids,
                report_date,
                page_limit=page_limit,
            )
            for report_date in _report_dates(_context_logical_date(context))
        ]
        for snapshot in snapshots:
            snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
        print(
            f"{DAG_ID}: pulled {sum(len(snapshot['insights']) for snapshot in snapshots)} "
            f"adset daily rows for account={account['id']} campaign={campaign['id']}"
        )
        return {"snapshots": snapshots}

    @task
    def write_adset_snapshots(
        adset_snapshots: dict[str, Any],
        account: dict[str, Any],
        campaign: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        context = get_current_context()
        snapshot_run_id = _snapshot_run_id(context["run_id"], "adset", account["id"], campaign["id"])
        report_date = _snapshot_report_date(_context_logical_date(context))
        status_resolver = _daily_status_resolver()
        rows = [
            _adset_row(snapshot, insight, account, campaign, snapshot_run_id, report_date, status_resolver)
            for snapshot in adset_snapshots.get("snapshots", [])
            for insight in snapshot.get("insights", [])
            if insight.get("adset_id")
        ]
        _upsert_daily_rows(ADSET_DAILY_TABLE, ADSET_INSERT_COLUMNS, ADSET_CONFLICT_COLUMNS, rows)
        print(
            f"{DAG_ID}: upserted {len(rows)} adset daily rows for "
            f"account={account['id']} campaign={campaign['id']}"
        )
        return {"level": "adset", "row_count": len(rows), "account_id": account["id"], "campaign_id": campaign["id"]}

    @task
    def pull_adset_region_snapshots(
        account: dict[str, Any],
        campaign: dict[str, Any],
        source: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        access_token = meta_access_token()
        context = get_current_context()
        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        adset_ids = [adset["id"] for adset in campaign.get("adsets", []) if adset.get("id")]
        snapshots = [
            adset_region_daily_snapshot(
                access_token,
                account["id"],
                campaign["id"],
                adset_ids,
                report_date,
                page_limit=page_limit,
            )
            for report_date in _report_dates(_context_logical_date(context))
        ]
        for snapshot in snapshots:
            snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
        print(
            f"{DAG_ID}: pulled {sum(len(snapshot['insights']) for snapshot in snapshots)} "
            f"adset region daily rows for account={account['id']} campaign={campaign['id']}"
        )
        return {"snapshots": snapshots}

    @task
    def write_adset_region_snapshots(
        adset_region_snapshots: dict[str, Any],
        account: dict[str, Any],
        campaign: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        context = get_current_context()
        snapshot_run_id = _snapshot_run_id(context["run_id"], "adset_region", account["id"], campaign["id"])
        report_date = _snapshot_report_date(_context_logical_date(context))
        status_resolver = _daily_status_resolver()
        rows = [
            _adset_region_row(snapshot, insight, account, campaign, snapshot_run_id, report_date, status_resolver)
            for snapshot in adset_region_snapshots.get("snapshots", [])
            for insight in snapshot.get("insights", [])
            if insight.get("adset_id") and insight.get("region")
        ]
        _upsert_daily_rows(
            ADSET_REGION_DAILY_TABLE,
            ADSET_REGION_INSERT_COLUMNS,
            ADSET_REGION_CONFLICT_COLUMNS,
            rows,
        )
        print(
            f"{DAG_ID}: upserted {len(rows)} adset region daily rows for "
            f"account={account['id']} campaign={campaign['id']}"
        )
        return {"level": "adset_region", "row_count": len(rows), "account_id": account["id"], "campaign_id": campaign["id"]}

    @task
    def pull_ad_snapshots(
        account: dict[str, Any],
        campaign: dict[str, Any],
        source: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        access_token = meta_access_token()
        context = get_current_context()
        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        ad_ids = [
            ad_id
            for adset in campaign.get("adsets", [])
            for ad_id in ad_ids_from_config(adset)
        ]
        snapshots = [
            ad_daily_snapshot(
                access_token,
                account["id"],
                campaign["id"],
                ad_ids,
                report_date,
                page_limit=page_limit,
            )
            for report_date in _report_dates(_context_logical_date(context))
        ]
        for snapshot in snapshots:
            snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
        print(
            f"{DAG_ID}: pulled {sum(len(snapshot['insights']) for snapshot in snapshots)} "
            f"ad daily rows for account={account['id']} campaign={campaign['id']}"
        )
        return {"snapshots": snapshots}

    @task
    def write_ad_snapshots(
        ad_snapshots: dict[str, Any],
        account: dict[str, Any],
        campaign: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        context = get_current_context()
        snapshot_run_id = _snapshot_run_id(context["run_id"], "ad", account["id"], campaign["id"])
        report_date = _snapshot_report_date(_context_logical_date(context))
        adset_by_ad_id = _adset_by_ad_id(campaign)
        status_resolver = _daily_status_resolver()
        rows = [
            _ad_row(snapshot, insight, account, campaign, adset_by_ad_id, snapshot_run_id, report_date, status_resolver)
            for snapshot in ad_snapshots.get("snapshots", [])
            for insight in snapshot.get("insights", [])
            if insight.get("ad_id") in adset_by_ad_id
        ]
        _upsert_daily_rows(AD_DAILY_TABLE, AD_INSERT_COLUMNS, AD_CONFLICT_COLUMNS, rows)
        print(
            f"{DAG_ID}: upserted {len(rows)} ad daily rows for "
            f"account={account['id']} campaign={campaign['id']}"
        )
        return {"level": "ad", "row_count": len(rows), "account_id": account["id"], "campaign_id": campaign["id"]}

    @task
    def pull_ad_region_snapshots(
        account: dict[str, Any],
        campaign: dict[str, Any],
        source: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        access_token = meta_access_token()
        context = get_current_context()
        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        ad_ids = [
            ad_id
            for adset in campaign.get("adsets", [])
            for ad_id in ad_ids_from_config(adset)
        ]
        snapshots = [
            ad_region_daily_snapshot(
                access_token,
                account["id"],
                campaign["id"],
                ad_ids,
                report_date,
                page_limit=page_limit,
            )
            for report_date in _report_dates(_context_logical_date(context))
        ]
        for snapshot in snapshots:
            snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
        print(
            f"{DAG_ID}: pulled {sum(len(snapshot['insights']) for snapshot in snapshots)} "
            f"ad region daily rows for account={account['id']} campaign={campaign['id']}"
        )
        return {"snapshots": snapshots}

    @task
    def write_ad_region_snapshots(
        ad_region_snapshots: dict[str, Any],
        account: dict[str, Any],
        campaign: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        context = get_current_context()
        snapshot_run_id = _snapshot_run_id(context["run_id"], "ad_region", account["id"], campaign["id"])
        report_date = _snapshot_report_date(_context_logical_date(context))
        adset_by_ad_id = _adset_by_ad_id(campaign)
        status_resolver = _daily_status_resolver()
        rows = [
            _ad_region_row(
                snapshot,
                insight,
                account,
                campaign,
                adset_by_ad_id,
                snapshot_run_id,
                report_date,
                status_resolver,
            )
            for snapshot in ad_region_snapshots.get("snapshots", [])
            for insight in snapshot.get("insights", [])
            if insight.get("ad_id") in adset_by_ad_id and insight.get("region")
        ]
        _upsert_daily_rows(AD_REGION_DAILY_TABLE, AD_REGION_INSERT_COLUMNS, AD_REGION_CONFLICT_COLUMNS, rows)
        print(
            f"{DAG_ID}: upserted {len(rows)} ad region daily rows for "
            f"account={account['id']} campaign={campaign['id']}"
        )
        return {"level": "ad_region", "row_count": len(rows), "account_id": account["id"], "campaign_id": campaign["id"]}

    @task
    def pull_ad_gender_age_snapshots(
        account: dict[str, Any],
        campaign: dict[str, Any],
        source: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        access_token = meta_access_token()
        context = get_current_context()
        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        ad_ids = [
            ad_id
            for adset in campaign.get("adsets", [])
            for ad_id in ad_ids_from_config(adset)
        ]
        snapshots = [
            ad_gender_age_daily_snapshot(
                access_token,
                account["id"],
                campaign["id"],
                ad_ids,
                report_date,
                page_limit=page_limit,
            )
            for report_date in _report_dates(_context_logical_date(context))
        ]
        for snapshot in snapshots:
            snapshot["config_snapshot_uri"] = source.get("snapshot_uri")
        print(
            f"{DAG_ID}: pulled {sum(len(snapshot['insights']) for snapshot in snapshots)} "
            f"ad gender/age daily rows for account={account['id']} campaign={campaign['id']}"
        )
        return {"snapshots": snapshots}

    @task
    def write_ad_gender_age_snapshots(
        ad_gender_age_snapshots: dict[str, Any],
        account: dict[str, Any],
        campaign: dict[str, Any],
    ) -> dict[str, Any]:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        context = get_current_context()
        snapshot_run_id = _snapshot_run_id(context["run_id"], "ad_gender_age", account["id"], campaign["id"])
        report_date = _snapshot_report_date(_context_logical_date(context))
        adset_by_ad_id = _adset_by_ad_id(campaign)
        status_resolver = _daily_status_resolver()
        rows = [
            _ad_gender_age_row(
                snapshot,
                insight,
                account,
                campaign,
                adset_by_ad_id,
                snapshot_run_id,
                report_date,
                status_resolver,
            )
            for snapshot in ad_gender_age_snapshots.get("snapshots", [])
            for insight in snapshot.get("insights", [])
            if insight.get("ad_id") in adset_by_ad_id
            and insight.get("age")
            and insight.get("gender")
        ]
        _upsert_daily_rows(
            AD_GENDER_AGE_DAILY_TABLE,
            AD_GENDER_AGE_INSERT_COLUMNS,
            AD_GENDER_AGE_CONFLICT_COLUMNS,
            rows,
        )
        print(
            f"{DAG_ID}: upserted {len(rows)} ad gender/age daily rows for "
            f"account={account['id']} campaign={campaign['id']}"
        )
        return {
            "level": "ad_gender_age",
            "row_count": len(rows),
            "account_id": account["id"],
            "campaign_id": campaign["id"],
        }

    wait_for_campaign_config = ExternalTaskSensor(
        task_id="wait_for_facebook_campaign_config_update",
        external_dag_id=CAMPAIGN_CONFIG_DAG_ID,
        external_task_id=None,
        execution_date_fn=campaign_config_logical_date,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=3 * 60 * 60,
    )
    wait_for_object_property = ExternalTaskSensor(
        task_id="wait_for_meta_object_property_sync",
        external_dag_id="meta_object_property_sync",
        external_task_id="sync_object_properties",
        execution_date_fn=campaign_config_logical_date,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=3 * 60 * 60,
    )
    config_task = log_campaign_config_source(config_log)
    wait_for_campaign_config >> wait_for_object_property >> config_task
    config_task >> sync_daily_reports_from_insights(config_log)


def _campaign_config_for_display() -> dict[str, Any]:
    pointer_uri = gcs_uri("airflow-run-us-west2", latest_object_name(CONFIG_GCS_PREFIX))
    source: dict[str, Any] = {
        "pointer_uri": pointer_uri,
        "pointer_link": gcs_console_link(pointer_uri),
        "snapshot_uri": "",
        "snapshot_link": "",
        "accounts": [],
        "account_count": 0,
        "campaign_count": 0,
        "adset_count": 0,
        "error": "",
    }
    try:
        import google.auth  # type: ignore[import-not-found]
        from google.cloud import storage  # type: ignore[import-not-found]

        credentials, _project_id = google.auth.default()
        storage_client = storage.Client(credentials=credentials)
        pointer_uri, pointer = read_latest_snapshot_pointer(storage_client, CONFIG_GCS_PREFIX)
        snapshot_uri = str(pointer["final_output"])
        snapshot = read_json_from_gcs(storage_client, snapshot_uri)
        lookup_window_days = int(
            env_config_value(LOOKUP_WINDOW_ENV, str(DEFAULT_TRAFFIC_LOOKUP_WINDOW_DAYS))
        )
        active_accounts = env_config_value(ACTIVE_ACCOUNTS_ENV)
        accounts = traffic_accounts_from_config(
            snapshot,
            active_accounts_value=active_accounts,
            lookup_window_days=lookup_window_days,
        )
        source.update(
            {
                "pointer_uri": pointer_uri,
                "pointer_link": gcs_console_link(pointer_uri),
                "snapshot_uri": snapshot_uri,
                "snapshot_link": gcs_console_link(snapshot_uri),
                "accounts": accounts,
                "account_count": len(accounts),
                "campaign_count": _campaign_count(accounts),
                "adset_count": _adset_count(accounts),
            }
        )
    except Exception as exc:
        source["error"] = str(exc)
    return source


def _config_log_payload(source: dict[str, Any]) -> dict[str, Any]:
    return {
        "pointer_uri": source["pointer_uri"],
        "pointer_link": source["pointer_link"],
        "snapshot_uri": source.get("snapshot_uri", ""),
        "snapshot_link": source.get("snapshot_link", ""),
        "account_count": source.get("account_count", 0),
        "campaign_count": source.get("campaign_count", 0),
        "adset_count": source.get("adset_count", 0),
        "error": source.get("error", ""),
    }


def _campaign_count(accounts: list[dict[str, Any]]) -> int:
    return sum(len(account.get("campaigns", [])) for account in accounts)


def _adset_count(accounts: list[dict[str, Any]]) -> int:
    return sum(
        len(campaign.get("adsets", []))
        for account in accounts
        for campaign in account.get("campaigns", [])
    )


def _daily_status_resolver() -> DailyStatusResolver:
    import google.auth  # type: ignore[import-not-found]
    from google.cloud import storage  # type: ignore[import-not-found]

    credentials, _project_id = google.auth.default()
    return DailyStatusResolver(storage.Client(credentials=credentials), CONFIG_GCS_PREFIX)


def _snapshot_run_id(run_id: str, level: str, *ids: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, ":".join([DAG_ID, level, run_id, *ids])))


def _snapshot_report_date(logical_date: Any) -> str:
    return report_datetime(logical_date).format("YYYY-MM-DD")


def _report_dates(logical_date: Any) -> list[str]:
    current = report_datetime(logical_date)
    return [current.format("YYYY-MM-DD"), current.subtract(days=1).format("YYYY-MM-DD")]


def _context_logical_date(context: dict[str, Any]) -> Any:
    if context.get("logical_date"):
        return context["logical_date"]
    for key in ("data_interval_start", "execution_date", "ts"):
        if context.get(key):
            return context[key]
    for source in (context.get("dag_run"), context.get("task_instance"), context.get("ti")):
        if source is None:
            continue
        for attr in ("logical_date", "data_interval_start", "execution_date", "run_after", "start_date"):
            value = getattr(source, attr, None)
            if value:
                return value
    run_id = str(context.get("run_id") or getattr(context.get("dag_run"), "run_id", ""))
    if "__" in run_id:
        try:
            return pendulum.parse(run_id.split("__", 1)[1])
        except ParserError:
            pass
    raise KeyError(f"No logical date found in Airflow context keys: {sorted(context)}")


def _airflow_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_") or "unknown"


meta_traffic_snapshot()
