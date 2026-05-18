"""Snapshot current Meta ad account objects for Airflow Variables."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from merino_meta_jobs.facebook_graph import MetaGraphClient, ensure_act_prefix

AD_ACCOUNT_FIELDS = "id,account_id,account_status"
AD_FIELDS = (
    "id,status,effective_status,"
    "campaign{id,status,effective_status},"
    "adset{id,status,effective_status,campaign{id,status,effective_status}},"
    "creative{id,status}"
)


def account_ids_from_env() -> list[str]:
    """Read optional comma-separated account ids from `META_AD_ACCOUNT_IDS`."""
    value = os.environ.get("META_AD_ACCOUNT_IDS", "")
    return [account_id.strip() for account_id in value.split(",") if account_id.strip()]


def current_ad_object_snapshot(
    access_token: str,
    *,
    account_ids: list[str] | None = None,
    page_limit: int = 500,
) -> dict[str, Any]:
    """Fetch a minimal nested campaign → adset → ad snapshot from account ads."""
    client = MetaGraphClient(access_token)
    accounts = account_ids or account_ids_from_env()
    account_rows = _explicit_account_rows(client, accounts) if accounts else _discover_account_rows(client, page_limit)

    snapshot_accounts: dict[str, Any] = {}
    for account in account_rows:
        account_id = ensure_act_prefix(str(account.get("id") or account.get("account_id") or ""))
        if not account_id:
            continue
        snapshot_accounts[account_id] = _account_snapshot(client, account_id, account, page_limit)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "accounts": snapshot_accounts,
    }


def _discover_account_rows(client: MetaGraphClient, page_limit: int) -> list[dict[str, Any]]:
    return client.get_all("me/adaccounts", {"fields": AD_ACCOUNT_FIELDS, "limit": page_limit})


def _explicit_account_rows(client: MetaGraphClient, account_ids: list[str]) -> list[dict[str, Any]]:
    return [
        client.get(ensure_act_prefix(account_id), {"fields": AD_ACCOUNT_FIELDS})
        for account_id in account_ids
    ]


def _account_snapshot(
    client: MetaGraphClient,
    account_id: str,
    account: dict[str, Any],
    page_limit: int,
) -> dict[str, Any]:
    ads = client.get_all(f"{account_id}/ads", {"fields": AD_FIELDS, "limit": page_limit})

    return {
        "id": account.get("id") or account_id,
        "status": account.get("account_status"),
        "campaigns": _campaign_tree(ads),
    }


def _campaign_tree(ads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    campaigns: dict[str, dict[str, Any]] = {}
    adsets_by_campaign: dict[str, dict[str, dict[str, Any]]] = {}

    for ad in ads:
        adset = ad.get("adset") if isinstance(ad.get("adset"), dict) else {}
        campaign = _campaign_from_ad(ad, adset)
        campaign_id = str(campaign.get("id") or "")
        if not campaign_id:
            continue

        campaigns.setdefault(campaign_id, {"id": campaign.get("id"), "status": _status(campaign), "adsets": []})
        adsets_for_campaign = adsets_by_campaign.setdefault(campaign_id, {})

        adset_id = str(adset.get("id") or "_unknown_adset")
        adset_node = adsets_for_campaign.setdefault(
            adset_id,
            {
                "id": adset.get("id"),
                "status": _status(adset),
                "ads": [],
            },
        )

        adset_node["ads"].append(_ad_node(ad))

    for campaign_id, campaign in campaigns.items():
        campaign["adsets"] = list(adsets_by_campaign.get(campaign_id, {}).values())

    return list(campaigns.values())


def _campaign_from_ad(ad: dict[str, Any], adset: dict[str, Any]) -> dict[str, Any]:
    if isinstance(ad.get("campaign"), dict):
        return ad["campaign"]
    if isinstance(adset.get("campaign"), dict):
        return adset["campaign"]
    return {}


def _ad_node(ad: dict[str, Any]) -> dict[str, Any]:
    node = {"id": ad.get("id"), "status": _status(ad)}
    creative = ad.get("creative") if isinstance(ad.get("creative"), dict) else {}
    if creative.get("id"):
        node["creative"] = {"id": creative.get("id"), "status": _status(creative)}
    return node


def _status(row: dict[str, Any]) -> Any:
    return row.get("effective_status") or row.get("status")
