"""Create planned Meta adsets from audience_analysis queue rows."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any

from merino_meta_jobs.facebook_graph import MetaGraphClient, ensure_act_prefix

POSTGRES_CONN_ID = "merino_analytics"
PLATFORM = "meta"
DEFAULT_TEMPLATE_NAME = "default adset"


@dataclass(frozen=True)
class AdsetCreationRequest:
    queue_id: int
    planned_date: date
    source_account_id: str
    campaign_id: str
    campaign_name: str | None
    adset_name: str
    region_code: str
    demographic_code: str
    demographic_name: str
    interest_group_code: str
    interest_group_name: str
    targeting: dict[str, Any]
    daily_budget: int | None
    lifetime_budget: int | None
    optimization_goal: str | None
    billing_event: str | None
    bid_strategy: str | None
    desired_meta_status: str


def create_planned_adsets(
    conn: Any,
    client: MetaGraphClient,
    *,
    planned_date: date | str,
    dry_run: bool = False,
) -> dict[str, int]:
    requests = load_pending_requests(conn, planned_date)
    result = {"pending": len(requests), "created": 0, "exists": 0, "failed": 0, "dry_run": int(dry_run)}

    for request in requests:
        try:
            existing = existing_adset(client, request)
            if existing:
                if not dry_run:
                    mark_queue_done(conn, request.queue_id, "exists", existing["id"], None)
                    upsert_setup_from_request(conn, request, existing["id"], existing.get("status"))
                result["exists"] += 1
                continue

            payload = adset_create_payload(request)
            if dry_run:
                print(f"dry_run create adset queue_id={request.queue_id} payload={json.dumps(payload, sort_keys=True)}")
                continue

            created = client.post(f"{ensure_act_prefix(request.source_account_id)}/adsets", payload)
            adset_id = str(created.get("id") or "")
            if not adset_id:
                raise RuntimeError(f"Meta did not return an adset id for queue row {request.queue_id}")

            mark_queue_done(conn, request.queue_id, "created", adset_id, None)
            upsert_setup_from_request(conn, request, adset_id, request.desired_meta_status)
            result["created"] += 1
        except Exception as exc:
            if not dry_run:
                mark_queue_done(conn, request.queue_id, "failed", None, str(exc))
            result["failed"] += 1

    return result


def load_pending_requests(conn: Any, planned_date: date | str) -> list[AdsetCreationRequest]:
    sql = """
        WITH default_template AS (
            SELECT id
            FROM audience_analysis.adset_creation_template
            WHERE platform = 'meta'
              AND is_default
              AND is_active
            ORDER BY id
            LIMIT 1
        )
        SELECT
            q.id AS queue_id,
            q.planned_date,
            COALESCE(q.source_account_id, t.source_account_id) AS source_account_id,
            COALESCE(q.campaign_id, t.campaign_id) AS campaign_id,
            COALESCE(q.campaign_name, t.campaign_name) AS campaign_name,
            q.adset_name,
            q.region_code,
            q.demographic_code,
            q.demographic_name,
            q.interest_group_code,
            q.interest_group_name,
            t.targeting AS template_targeting,
            q.targeting_override,
            COALESCE(q.daily_budget_override, t.daily_budget) AS daily_budget,
            COALESCE(q.lifetime_budget_override, t.lifetime_budget) AS lifetime_budget,
            t.optimization_goal,
            t.billing_event,
            t.bid_strategy,
            COALESCE(q.desired_meta_status, t.status, 'PAUSED') AS desired_meta_status
        FROM audience_analysis.adset_creation_queue q
        JOIN default_template dt ON TRUE
        JOIN audience_analysis.adset_creation_template t
          ON t.id = COALESCE(q.template_id, dt.id)
        WHERE q.platform = 'meta'
          AND q.planned_date = %s
          AND q.status IN ('pending', 'failed')
          AND t.is_active
        ORDER BY q.id
        FOR UPDATE OF q SKIP LOCKED
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (planned_date,))
        return [request_from_row(row) for row in dict_rows(cursor)]


def request_from_row(row: dict[str, Any]) -> AdsetCreationRequest:
    targeting = merge_targeting(row.get("template_targeting"), row.get("targeting_override"))
    return AdsetCreationRequest(
        queue_id=int(row["queue_id"]),
        planned_date=row["planned_date"],
        source_account_id=str(row.get("source_account_id") or ""),
        campaign_id=str(row.get("campaign_id") or ""),
        campaign_name=row.get("campaign_name"),
        adset_name=str(row.get("adset_name") or ""),
        region_code=str(row.get("region_code") or ""),
        demographic_code=str(row.get("demographic_code") or ""),
        demographic_name=str(row.get("demographic_name") or ""),
        interest_group_code=str(row.get("interest_group_code") or ""),
        interest_group_name=str(row.get("interest_group_name") or ""),
        targeting=targeting,
        daily_budget=int_or_none(row.get("daily_budget")),
        lifetime_budget=int_or_none(row.get("lifetime_budget")),
        optimization_goal=clean_text(row.get("optimization_goal")),
        billing_event=clean_text(row.get("billing_event")),
        bid_strategy=clean_text(row.get("bid_strategy")),
        desired_meta_status=meta_status(row.get("desired_meta_status")),
    )


def merge_targeting(template_targeting: Any, override: Any) -> dict[str, Any]:
    targeting = template_targeting if isinstance(template_targeting, dict) else {}
    if isinstance(targeting, str):
        targeting = json.loads(targeting)
    merged = dict(targeting)

    if override in (None, ""):
        return merged
    if isinstance(override, str):
        override = json.loads(override)
    if isinstance(override, dict):
        merged.update(override)
    return merged


def existing_adset(client: MetaGraphClient, request: AdsetCreationRequest) -> dict[str, Any] | None:
    rows = client.get_all(
        f"{request.campaign_id}/adsets",
        {"fields": "id,name,campaign_id,status", "limit": 500},
    )
    for row in rows:
        if str(row.get("name") or "") == request.adset_name:
            return row
    return None


def adset_create_payload(request: AdsetCreationRequest) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "name": request.adset_name,
        "campaign_id": request.campaign_id,
        "targeting": request.targeting,
        "status": request.desired_meta_status,
    }
    if request.daily_budget is not None:
        payload["daily_budget"] = request.daily_budget
    if request.lifetime_budget is not None:
        payload["lifetime_budget"] = request.lifetime_budget
    if request.optimization_goal:
        payload["optimization_goal"] = request.optimization_goal
    if request.billing_event:
        payload["billing_event"] = request.billing_event
    if request.bid_strategy:
        payload["bid_strategy"] = request.bid_strategy
    return payload


def mark_queue_done(conn: Any, queue_id: int, status: str, adset_id: str | None, error_message: str | None) -> None:
    sql = """
        UPDATE audience_analysis.adset_creation_queue
        SET status = %s,
            created_adset_id = COALESCE(%s, created_adset_id),
            checked_at = now(),
            error_message = %s,
            updated_at = now(),
            update_count = update_count + 1
        WHERE id = %s
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (status, adset_id, error_message, queue_id))


def upsert_setup_from_request(
    conn: Any,
    request: AdsetCreationRequest,
    adset_id: str,
    meta_status_value: str | None,
) -> None:
    sql = """
        INSERT INTO audience_analysis.adset_region_demograph_setup (
            source_account_id,
            platform,
            campaign_id,
            campaign_name,
            adset_id,
            adset_name,
            region_code,
            demographic_code,
            demographic_name,
            interest_group_code,
            interest_group_name,
            setup_status,
            meta_status,
            daily_budget,
            lifetime_budget,
            targeting,
            observed_date,
            setup_source
        )
        VALUES (
            %s, 'meta', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            CURRENT_DATE, 'adset_creation'
        )
        ON CONFLICT (adset_id) WHERE valid_to IS NULL DO UPDATE SET
            source_account_id = EXCLUDED.source_account_id,
            platform = EXCLUDED.platform,
            campaign_id = EXCLUDED.campaign_id,
            campaign_name = EXCLUDED.campaign_name,
            adset_name = EXCLUDED.adset_name,
            region_code = EXCLUDED.region_code,
            demographic_code = EXCLUDED.demographic_code,
            demographic_name = EXCLUDED.demographic_name,
            interest_group_code = EXCLUDED.interest_group_code,
            interest_group_name = EXCLUDED.interest_group_name,
            setup_status = EXCLUDED.setup_status,
            meta_status = EXCLUDED.meta_status,
            daily_budget = EXCLUDED.daily_budget,
            lifetime_budget = EXCLUDED.lifetime_budget,
            targeting = EXCLUDED.targeting,
            observed_date = CURRENT_DATE,
            setup_source = 'adset_creation',
            updated_at = now(),
            update_count = audience_analysis.adset_region_demograph_setup.update_count + 1
    """
    with conn.cursor() as cursor:
        cursor.execute(
            sql,
            (
                request.source_account_id,
                request.campaign_id,
                request.campaign_name,
                adset_id,
                request.adset_name,
                request.region_code,
                request.demographic_code,
                request.demographic_name,
                request.interest_group_code,
                request.interest_group_name,
                setup_status(meta_status_value),
                meta_status_value,
                request.daily_budget,
                request.lifetime_budget,
                json.dumps(request.targeting, separators=(",", ":"), sort_keys=True),
            ),
        )


def dict_rows(cursor: Any) -> list[dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def clean_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def meta_status(value: Any) -> str:
    status = str(value or "").upper()
    return status if status in {"ACTIVE", "PAUSED"} else "PAUSED"


def setup_status(value: Any) -> str:
    status = str(value or "").upper()
    if status == "ACTIVE":
        return "active"
    if status == "PAUSED":
        return "paused"
    return "created"
