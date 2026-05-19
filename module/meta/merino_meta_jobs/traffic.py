"""Hourly Meta ad insights snapshots for traffic ingestion."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

from merino_meta_jobs.account_snapshot import account_ids_from_env
from merino_meta_jobs.facebook_graph import MetaGraphClient, ensure_act_prefix

AD_ACCOUNT_FIELDS = "id,account_id,account_status"
INSIGHT_FIELDS = (
    "ad_id,ad_name,campaign_id,adset_id,"
    "impressions,clicks,spend,reach,frequency,ctr,cpc,cpm,"
    "actions,action_values,cost_per_action_type,conversions"
)
FACEBOOK_ACTIVE_ACCOUNTS_ENV = "FACEBOOK_ACTIVE_ACCOUNTS"
FACEBOOK_TRAFFIC_LOOKUP_WINDOWS_ENV = "FACEBOOK_TRAFFIC_LOOKUP_WINDOWS"
DEFAULT_TRAFFIC_LOOKUP_WINDOW_DAYS = 3


def traffic_hourly_snapshot(
    access_token: str,
    metric_date: str,
    *,
    account_ids: list[str] | None = None,
    page_limit: int = 500,
) -> dict[str, Any]:
    """Fetch ad-level insights for one calendar day (one row set per account)."""
    client = MetaGraphClient(access_token)
    accounts = account_ids or account_ids_from_env()
    account_rows = (
        _explicit_account_rows(client, accounts)
        if accounts
        else _discover_account_rows(client, page_limit)
    )
    time_range = {"since": metric_date, "until": metric_date}

    snapshot_accounts: dict[str, Any] = {}
    for account in account_rows:
        account_id = ensure_act_prefix(str(account.get("id") or account.get("account_id") or ""))
        if not account_id:
            continue
        insights = client.get_all(
            f"{account_id}/insights",
            {
                "level": "ad",
                "fields": INSIGHT_FIELDS,
                "time_range": time_range,
                "limit": page_limit,
            },
        )
        snapshot_accounts[account_id] = {
            "id": account.get("id") or account_id,
            "metric_date": metric_date,
            "insights": insights,
        }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metric_date": metric_date,
        "accounts": snapshot_accounts,
    }


def adset_traffic_hourly_snapshot(
    access_token: str,
    account_id: str,
    adset_id: str,
    metric_date: str,
    *,
    page_limit: int = 500,
) -> dict[str, Any]:
    """Fetch ad-level insights for one adset and one calendar day."""
    client = MetaGraphClient(access_token)
    insights = client.get_all(
        f"{adset_id}/insights",
        {
            "level": "ad",
            "fields": INSIGHT_FIELDS,
            "time_range": {"since": metric_date, "until": metric_date},
            "limit": page_limit,
        },
    )
    account_id = ensure_act_prefix(account_id)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metric_date": metric_date,
        "account_id": account_id,
        "adset_id": adset_id,
        "insights": insights,
    }


def traffic_accounts_from_config(
    config_snapshot: dict[str, Any],
    *,
    active_accounts_value: str | None = None,
    lookup_window_days: int | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Select account/adset work from a campaign config snapshot."""
    active_accounts = {
        ensure_act_prefix(account_id)
        for account_id in account_ids_from_text(
            active_accounts_value
            if active_accounts_value is not None
            else os.environ.get(FACEBOOK_ACTIVE_ACCOUNTS_ENV, "")
        )
    }
    if lookup_window_days is None:
        lookup_window_days = int(
            os.environ.get(
                FACEBOOK_TRAFFIC_LOOKUP_WINDOWS_ENV,
                DEFAULT_TRAFFIC_LOOKUP_WINDOW_DAYS,
            )
        )
    cutoff = (now or datetime.now(timezone.utc)) - timedelta(days=lookup_window_days)

    selected_accounts: list[dict[str, Any]] = []
    for account_id, account in sorted(config_snapshot.get("accounts", {}).items()):
        account_id = ensure_act_prefix(str(account.get("id") or account_id))
        if active_accounts and account_id not in active_accounts:
            continue

        selected_adsets: list[dict[str, Any]] = []
        for campaign in account.get("campaigns", []):
            campaign_id = str(campaign.get("id") or "")
            for adset in campaign.get("adsets", []):
                if _adset_should_import(adset, cutoff):
                    selected_adsets.append(
                        {
                            "id": str(adset.get("id")),
                            "status": adset.get("status"),
                            "updated_at": adset.get("updated_at"),
                            "campaign_id": campaign_id,
                        }
                    )

        if selected_adsets:
            selected_accounts.append(
                {
                    "id": account_id,
                    "status": account.get("status"),
                    "adsets": selected_adsets,
                }
            )

    return selected_accounts


def account_ids_from_text(value: str) -> list[str]:
    return [account_id.strip() for account_id in value.split(",") if account_id.strip()]


def _discover_account_rows(client: MetaGraphClient, page_limit: int) -> list[dict[str, Any]]:
    return client.get_all("me/adaccounts", {"fields": AD_ACCOUNT_FIELDS, "limit": page_limit})


def _explicit_account_rows(client: MetaGraphClient, account_ids: list[str]) -> list[dict[str, Any]]:
    return [
        client.get(ensure_act_prefix(account_id), {"fields": AD_ACCOUNT_FIELDS})
        for account_id in account_ids
    ]


def _adset_should_import(adset: dict[str, Any], cutoff: datetime) -> bool:
    status = str(adset.get("status") or "").upper()
    if status == "ACTIVE":
        return True

    updated_at = _meta_datetime(adset.get("updated_at"))
    return updated_at is not None and updated_at >= cutoff


def _meta_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Pull Meta ad insights for one day.")
    parser.add_argument("--date", required=True, help="Metric date (YYYY-MM-DD).")
    parser.add_argument("--page-limit", type=int, default=500)
    args = parser.parse_args(argv)

    from merino_meta_jobs.facebook_graph import access_token_from_env

    snapshot = traffic_hourly_snapshot(
        access_token_from_env(),
        args.date,
        page_limit=args.page_limit,
    )
    json.dump(snapshot, sys.stdout, separators=(",", ":"), sort_keys=True)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
