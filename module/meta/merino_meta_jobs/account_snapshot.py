"""Snapshot current Meta ad account objects for Airflow Variables."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from merino_meta_jobs.facebook_graph import MetaGraphClient, ensure_act_prefix

AD_ACCOUNT_FIELDS = "id,account_id,account_status"
# Timestamps on campaigns/adsets (same field names as meta-ads-mcp get_campaigns / get_adsets).
OBJECT_FIELDS = "id,status,effective_status,created_time,updated_time"
ADSET_LIST_FIELDS = f"{OBJECT_FIELDS},campaign_id"
# Account /ads: flat fields only. Nested campaign/adset/creative expansions reject created_time.
AD_FIELDS = "id,status,effective_status,adset_id,campaign_id"


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
    """Fetch campaign → adset → ad snapshot with created_at/updated_at on each object."""
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
    list_params = {"fields": OBJECT_FIELDS, "limit": page_limit}
    campaigns_by_id = _rows_by_id(
        client.get_all(f"{account_id}/campaigns", list_params),
    )
    adsets_by_id = _rows_by_id(
        client.get_all(
            f"{account_id}/adsets",
            {"fields": ADSET_LIST_FIELDS, "limit": page_limit},
        ),
    )
    ads = client.get_all(f"{account_id}/ads", {"fields": AD_FIELDS, "limit": page_limit})

    return {
        "id": account.get("id") or account_id,
        "status": account.get("account_status"),
        "campaigns": _campaign_tree(
            ads,
            campaigns_by_id=campaigns_by_id,
            adsets_by_id=adsets_by_id,
        ),
    }


def _rows_by_id(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_id = row.get("id")
        if row_id:
            indexed[str(row_id)] = row
    return indexed


def _campaign_tree(
    ads: list[dict[str, Any]],
    *,
    campaigns_by_id: dict[str, dict[str, Any]] | None = None,
    adsets_by_id: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    campaigns_by_id = campaigns_by_id or {}
    adsets_by_id = adsets_by_id or {}
    campaigns: dict[str, dict[str, Any]] = {}
    adsets_by_campaign: dict[str, dict[str, dict[str, Any]]] = {}

    for ad in ads:
        adset_id = str(ad.get("adset_id") or "")
        adset_row = adsets_by_id.get(adset_id, {})
        campaign_id = str(ad.get("campaign_id") or adset_row.get("campaign_id") or "")
        if not campaign_id:
            continue

        if campaign_id not in campaigns:
            campaigns[campaign_id] = {}
        if campaign_id in campaigns_by_id:
            _merge_object_fields(campaigns[campaign_id], campaigns_by_id[campaign_id])

        adsets_for_campaign = adsets_by_campaign.setdefault(campaign_id, {})
        if not adset_id:
            adset_id = "_unknown_adset"
        if adset_id not in adsets_for_campaign:
            adsets_for_campaign[adset_id] = {}
        if adset_id in adsets_by_id:
            _merge_object_fields(adsets_for_campaign[adset_id], adsets_by_id[adset_id])

        adsets_for_campaign[adset_id].setdefault("ads", []).append(_ad_node(ad))

    for campaign_id, row in campaigns_by_id.items():
        if campaign_id not in campaigns:
            campaigns[campaign_id] = {}
        _merge_object_fields(campaigns[campaign_id], row)
        adsets_by_campaign.setdefault(campaign_id, {})

    for adset_id, row in adsets_by_id.items():
        campaign_id = str(row.get("campaign_id") or "")
        if not campaign_id:
            continue
        if campaign_id not in campaigns:
            campaigns[campaign_id] = {}
        if campaign_id in campaigns_by_id:
            _merge_object_fields(campaigns[campaign_id], campaigns_by_id[campaign_id])
        adsets_for_campaign = adsets_by_campaign.setdefault(campaign_id, {})
        if adset_id not in adsets_for_campaign:
            adsets_for_campaign[adset_id] = {}
        _merge_object_fields(adsets_for_campaign[adset_id], row)

    for campaign_id, campaign in campaigns.items():
        campaign["adsets"] = list(adsets_by_campaign.get(campaign_id, {}).values())

    return list(campaigns.values())


def _ad_node(ad: dict[str, Any]) -> dict[str, Any]:
    return _object_node(ad)


def _object_node(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row.get("id"),
        "status": _status(row),
        "created_at": row.get("created_time"),
        "updated_at": row.get("updated_time"),
    }


def _merge_object_fields(node: dict[str, Any], row: dict[str, Any]) -> None:
    """Merge Graph row fields; never replace a non-null value with null."""
    for key, value in _object_node(row).items():
        if value is not None or key not in node:
            node[key] = value


def _status(row: dict[str, Any]) -> Any:
    return row.get("effective_status") or row.get("status")
