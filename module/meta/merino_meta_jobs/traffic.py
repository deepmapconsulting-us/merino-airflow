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
COMMON_INSIGHT_FIELDS = (
    "impressions,clicks,spend,reach,frequency,ctr,cpc,cpm,"
    "unique_clicks,actions,action_values,cost_per_action_type,conversions,"
    "attribution_setting,video_avg_time_watched_actions"
)
CAMPAIGN_INSIGHT_FIELDS = f"account_id,account_name,campaign_id,campaign_name,{COMMON_INSIGHT_FIELDS}"
ADSET_INSIGHT_FIELDS = f"account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,{COMMON_INSIGHT_FIELDS}"
AD_INSIGHT_FIELDS = f"account_id,account_name,ad_id,ad_name,campaign_id,campaign_name,adset_id,adset_name,{COMMON_INSIGHT_FIELDS}"
FACEBOOK_ACTIVE_ACCOUNTS_ENV = "FACEBOOK_ACTIVE_ACCOUNTS"
FACEBOOK_TRAFFIC_LOOKUP_WINDOWS_ENV = "FACEBOOK_TRAFFIC_LOOKUP_WINDOWS"
DEFAULT_TRAFFIC_LOOKUP_WINDOW_DAYS = 3
JSON_INSIGHT_COLUMNS = (
    "actions",
    "action_values",
    "cost_per_action_type",
    "conversions",
    "video_avg_time_watched_actions",
)
ACTION_TYPE_ALIASES = {
    "link_clicks": ("link_click",),
    "landing_page_views": ("landing_page_view",),
    "page_engagement": ("page_engagement",),
    "post_reactions": ("post_reaction",),
    "post_comments": ("comment", "post_comment"),
    "post_saves": ("post_save", "onsite_conversion.post_save"),
    "post_shares": ("post", "share", "post_share"),
    "facebook_likes": ("like", "page_like"),
    "instagram_follows": (
        "instagram_profile_follow",
        "onsite_conversion.instagram_profile_follow",
        "ig_profile_follow",
    ),
    "app_installs": ("app_install", "mobile_app_install"),
    "mobile_app_installs": ("mobile_app_install", "app_install"),
    "results": (
        "purchase",
        "lead",
        "complete_registration",
        "app_install",
        "mobile_app_install",
        "link_click",
        "landing_page_view",
        "page_engagement",
        "post_engagement",
    ),
}


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
                "fields": AD_INSIGHT_FIELDS,
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
    return ad_traffic_snapshot(
        access_token,
        account_id,
        adset_id,
        metric_date,
        page_limit=page_limit,
    )


def campaign_traffic_snapshot(
    access_token: str,
    account_id: str,
    campaign_id: str,
    metric_date: str,
    *,
    page_limit: int = 500,
) -> dict[str, Any]:
    """Fetch campaign-level insights for one campaign and one calendar day."""
    client = MetaGraphClient(access_token)
    insights = client.get_all(
        f"{campaign_id}/insights",
        {
            "level": "campaign",
            "fields": CAMPAIGN_INSIGHT_FIELDS,
            "time_range": {"since": metric_date, "until": metric_date},
            "limit": page_limit,
        },
    )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metric_date": metric_date,
        "account_id": ensure_act_prefix(account_id),
        "campaign_id": str(campaign_id),
        "insights": insights,
    }


def campaign_daily_snapshot(
    access_token: str,
    account_id: str,
    campaign_ids: list[str],
    metric_date: str,
    *,
    page_limit: int = 500,
) -> dict[str, Any]:
    """Fetch campaign-level daily insights for selected campaigns in one account."""
    account_id = ensure_act_prefix(account_id)
    if not campaign_ids:
        return _empty_daily_snapshot(account_id, metric_date)

    client = MetaGraphClient(access_token)
    insights = client.get_all(
        f"{account_id}/insights",
        _insight_params(
            level="campaign",
            fields=CAMPAIGN_INSIGHT_FIELDS,
            metric_date=metric_date,
            page_limit=page_limit,
            id_field="campaign.id",
            ids=campaign_ids,
        ),
    )
    return _daily_snapshot(account_id, metric_date, insights)


def adset_traffic_snapshot(
    access_token: str,
    account_id: str,
    adset_id: str,
    metric_date: str,
    *,
    page_limit: int = 500,
) -> dict[str, Any]:
    """Fetch adset-level insights for one adset and one calendar day."""
    client = MetaGraphClient(access_token)
    insights = client.get_all(
        f"{adset_id}/insights",
        {
            "level": "adset",
            "fields": ADSET_INSIGHT_FIELDS,
            "time_range": {"since": metric_date, "until": metric_date},
            "limit": page_limit,
        },
    )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metric_date": metric_date,
        "account_id": ensure_act_prefix(account_id),
        "adset_id": str(adset_id),
        "insights": insights,
    }


def adset_daily_snapshot(
    access_token: str,
    account_id: str,
    campaign_id: str,
    adset_ids: list[str],
    metric_date: str,
    *,
    page_limit: int = 500,
) -> dict[str, Any]:
    """Fetch adset-level daily insights for selected adsets in one campaign."""
    account_id = ensure_act_prefix(account_id)
    if not adset_ids:
        return _empty_daily_snapshot(account_id, metric_date, campaign_id=campaign_id)

    client = MetaGraphClient(access_token)
    insights = client.get_all(
        f"{campaign_id}/insights",
        _insight_params(
            level="adset",
            fields=ADSET_INSIGHT_FIELDS,
            metric_date=metric_date,
            page_limit=page_limit,
            id_field="adset.id",
            ids=adset_ids,
        ),
    )
    return _daily_snapshot(account_id, metric_date, insights, campaign_id=campaign_id)


def ad_traffic_snapshot(
    access_token: str,
    account_id: str,
    adset_id: str,
    metric_date: str,
    *,
    ad_ids: list[str] | None = None,
    page_limit: int = 500,
) -> dict[str, Any]:
    """Fetch ad-level insights for one adset and one calendar day."""
    account_id = ensure_act_prefix(account_id)
    if ad_ids is not None and not ad_ids:
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "metric_date": metric_date,
            "account_id": account_id,
            "adset_id": adset_id,
            "insights": [],
        }

    client = MetaGraphClient(access_token)
    params: dict[str, Any] = {
        "level": "ad",
        "fields": AD_INSIGHT_FIELDS,
        "time_range": {"since": metric_date, "until": metric_date},
        "limit": page_limit,
    }
    if ad_ids:
        params["filtering"] = [{"field": "ad.id", "operator": "IN", "value": ad_ids}]

    insights = client.get_all(f"{adset_id}/insights", params)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metric_date": metric_date,
        "account_id": account_id,
        "adset_id": adset_id,
        "insights": insights,
    }


def ad_daily_snapshot(
    access_token: str,
    account_id: str,
    campaign_id: str,
    ad_ids: list[str],
    metric_date: str,
    *,
    page_limit: int = 500,
) -> dict[str, Any]:
    """Fetch ad-level daily insights for selected ads in one campaign."""
    account_id = ensure_act_prefix(account_id)
    if not ad_ids:
        return _empty_daily_snapshot(account_id, metric_date, campaign_id=campaign_id)

    client = MetaGraphClient(access_token)
    insights = client.get_all(
        f"{campaign_id}/insights",
        _insight_params(
            level="ad",
            fields=AD_INSIGHT_FIELDS,
            metric_date=metric_date,
            page_limit=page_limit,
            id_field="ad.id",
            ids=ad_ids,
        ),
    )
    return _daily_snapshot(account_id, metric_date, insights, campaign_id=campaign_id)


def ad_gender_age_daily_snapshot(
    access_token: str,
    account_id: str,
    campaign_id: str,
    ad_ids: list[str],
    metric_date: str,
    *,
    page_limit: int = 500,
) -> dict[str, Any]:
    """Fetch ad-level daily insights with age and gender breakdowns."""
    account_id = ensure_act_prefix(account_id)
    if not ad_ids:
        return _empty_daily_snapshot(account_id, metric_date, campaign_id=campaign_id)

    client = MetaGraphClient(access_token)
    params = _insight_params(
        level="ad",
        fields=AD_INSIGHT_FIELDS,
        metric_date=metric_date,
        page_limit=page_limit,
        id_field="ad.id",
        ids=ad_ids,
    )
    params["breakdowns"] = "age,gender"
    insights = client.get_all(f"{campaign_id}/insights", params)
    return _daily_snapshot(account_id, metric_date, insights, campaign_id=campaign_id)


def ad_hourly_snapshot(
    access_token: str,
    account_id: str,
    campaign_id: str,
    ad_ids: list[str],
    metric_date: str,
    *,
    page_limit: int = 500,
) -> dict[str, Any]:
    """Fetch ad-level hourly insights for selected ads in one campaign and date."""
    account_id = ensure_act_prefix(account_id)
    if not ad_ids:
        return _empty_daily_snapshot(account_id, metric_date, campaign_id=campaign_id)

    client = MetaGraphClient(access_token)
    params = _insight_params(
        level="ad",
        fields=AD_INSIGHT_FIELDS,
        metric_date=metric_date,
        page_limit=page_limit,
        id_field="ad.id",
        ids=ad_ids,
    )
    params["breakdowns"] = "hourly_stats_aggregated_by_advertiser_time_zone"
    insights = client.get_all(f"{campaign_id}/insights", params)
    return _daily_snapshot(account_id, metric_date, insights, campaign_id=campaign_id)


def insight_metric_values(insight: dict[str, Any]) -> dict[str, Any]:
    """Return typed daily snapshot metric columns from a raw Meta insight row."""
    actions = insight.get("actions", [])
    costs = insight.get("cost_per_action_type", [])
    cost_per_result = insight.get("cost_per_result")
    if isinstance(cost_per_result, list) or cost_per_result in (None, ""):
        cost_per_result = _action_number(costs, ACTION_TYPE_ALIASES["results"])

    return {
        "spend": _number(insight.get("spend")),
        "impressions": _integer(insight.get("impressions")),
        "reach": _integer(insight.get("reach")),
        "frequency": _number(insight.get("frequency")),
        "clicks": _integer(insight.get("clicks")),
        "unique_clicks": _integer(insight.get("unique_clicks")),
        "ctr": _number(insight.get("ctr")),
        "cpc": _number(insight.get("cpc")),
        "cpm": _number(insight.get("cpm")),
        "actions": _json_object(actions),
        "link_clicks": _action_integer(actions, ACTION_TYPE_ALIASES["link_clicks"]),
        "landing_page_views": _action_integer(actions, ACTION_TYPE_ALIASES["landing_page_views"]),
        "page_engagement": _action_integer(actions, ACTION_TYPE_ALIASES["page_engagement"]),
        "post_reactions": _action_integer(actions, ACTION_TYPE_ALIASES["post_reactions"]),
        "post_comments": _action_integer(actions, ACTION_TYPE_ALIASES["post_comments"]),
        "post_saves": _action_integer(actions, ACTION_TYPE_ALIASES["post_saves"]),
        "post_shares": _action_integer(actions, ACTION_TYPE_ALIASES["post_shares"]),
        "facebook_likes": _action_integer(actions, ACTION_TYPE_ALIASES["facebook_likes"]),
        "instagram_follows": _action_integer(actions, ACTION_TYPE_ALIASES["instagram_follows"]),
        "app_installs": _action_integer(actions, ACTION_TYPE_ALIASES["app_installs"]),
        "mobile_app_installs": _action_integer(actions, ACTION_TYPE_ALIASES["mobile_app_installs"]),
        "results": _action_integer(actions, ACTION_TYPE_ALIASES["results"]),
        "cost_per_result": _number(cost_per_result),
        "cost_per_app_install": _action_number(costs, ACTION_TYPE_ALIASES["app_installs"]),
        "cost_per_action_type": _json_object(costs),
        "action_values": _json_object(insight.get("action_values")),
        "conversions": _json_object(insight.get("conversions")),
        "attribution_setting": insight.get("attribution_setting"),
        "video_avg_time_watched_actions": _json_object(insight.get("video_avg_time_watched_actions")),
    }


def traffic_accounts_from_config(
    config_snapshot: dict[str, Any],
    *,
    active_accounts_value: str | None = None,
    lookup_window_days: int | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Select account/campaign/adset work from a campaign config snapshot."""
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

        selected_campaigns: list[dict[str, Any]] = []
        for campaign in account.get("campaigns", []):
            campaign_id = str(campaign.get("id") or "")
            if not campaign_id:
                continue
            selected_adsets: list[dict[str, Any]] = []
            for adset in campaign.get("adsets", []):
                adset_id = str(adset.get("id") or "")
                if adset_id and _object_should_import(adset, cutoff):
                    selected_adsets.append(
                        {
                            "id": adset_id,
                            "status": adset.get("status"),
                            "updated_at": adset.get("updated_at"),
                            "campaign_id": campaign_id,
                            "ads": _ad_nodes_from_config(adset.get("ads", [])),
                        }
                    )

            if selected_adsets or _object_should_import(campaign, cutoff):
                selected_campaigns.append(
                    {
                        "id": campaign_id,
                        "status": campaign.get("status"),
                        "updated_at": campaign.get("updated_at"),
                        "adsets": selected_adsets,
                    }
                )

        if selected_campaigns:
            selected_accounts.append(
                {
                    "id": account_id,
                    "status": account.get("status"),
                    "timezone_name": account.get("timezone_name"),
                    "campaigns": selected_campaigns,
                }
            )

    return selected_accounts


def ad_ids_from_config(adset: dict[str, Any]) -> list[str]:
    """Return ad IDs listed in a campaign config adset node."""
    return [str(ad["id"]) for ad in adset.get("ads", []) if ad.get("id")]


def insight_ad_is_configured(insight: dict[str, Any], adset: dict[str, Any]) -> bool:
    return str(insight.get("ad_id") or "") in set(ad_ids_from_config(adset))


def account_ids_from_text(value: str) -> list[str]:
    return [account_id.strip() for account_id in value.split(",") if account_id.strip()]


def _insight_params(
    *,
    level: str,
    fields: str,
    metric_date: str,
    page_limit: int,
    id_field: str | None = None,
    ids: list[str] | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "level": level,
        "fields": fields,
        "time_range": {"since": metric_date, "until": metric_date},
        "limit": page_limit,
    }
    if id_field and ids:
        params["filtering"] = [{"field": id_field, "operator": "IN", "value": ids}]
    return params


def _daily_snapshot(
    account_id: str,
    metric_date: str,
    insights: list[dict[str, Any]],
    **ids: str,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metric_date": metric_date,
        "account_id": account_id,
        "insights": insights,
        **ids,
    }


def _empty_daily_snapshot(account_id: str, metric_date: str, **ids: str) -> dict[str, Any]:
    return _daily_snapshot(account_id, metric_date, [], **ids)


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, list):
        return {}

    by_action_type: dict[str, Any] = {}
    for item in value:
        if not isinstance(item, dict) or not item.get("action_type"):
            continue
        action_type = str(item["action_type"])
        keys = set(item) - {"action_type"}
        if keys == {"value"}:
            by_action_type[action_type] = _json_scalar(item.get("value"))
        else:
            by_action_type[action_type] = {
                key: _json_scalar(item[key])
                for key in sorted(keys)
            }
    return by_action_type


def _json_scalar(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        number = float(value)
    except ValueError:
        return value
    if number.is_integer():
        return int(number)
    return number


def _integer(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _action_integer(items: Any, action_types: tuple[str, ...]) -> int | None:
    return _integer(_action_value(items, action_types)) or 0


def _action_number(items: Any, action_types: tuple[str, ...]) -> float | None:
    return _number(_action_value(items, action_types))


def _action_value(items: Any, action_types: tuple[str, ...]) -> Any:
    if not isinstance(items, list):
        return None
    for action_type in action_types:
        for item in items:
            if isinstance(item, dict) and item.get("action_type") == action_type:
                return item.get("value")
    return None


def _discover_account_rows(client: MetaGraphClient, page_limit: int) -> list[dict[str, Any]]:
    return client.get_all("me/adaccounts", {"fields": AD_ACCOUNT_FIELDS, "limit": page_limit})


def _explicit_account_rows(client: MetaGraphClient, account_ids: list[str]) -> list[dict[str, Any]]:
    return [
        client.get(ensure_act_prefix(account_id), {"fields": AD_ACCOUNT_FIELDS})
        for account_id in account_ids
    ]


def _ad_nodes_from_config(rows: Any) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        return []

    ads: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict) or not row.get("id"):
            continue
        creative = row.get("creative") if isinstance(row.get("creative"), dict) else {}
        ads.append(
            {
                "id": str(row["id"]),
                "status": row.get("status"),
                "updated_at": row.get("updated_at"),
                "creative_id": str(creative["id"]) if creative.get("id") else None,
            }
        )
    return ads


def _object_should_import(row: dict[str, Any], cutoff: datetime) -> bool:
    status = str(row.get("status") or "").upper()
    if status == "ACTIVE":
        return True

    updated_at = _meta_datetime(row.get("updated_at"))
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
