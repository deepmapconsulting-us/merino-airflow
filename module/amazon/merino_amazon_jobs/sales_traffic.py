from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from merino_amazon_jobs.marketplaces import MARKETPLACES

GRANULARITIES = ("PARENT", "CHILD", "SKU")


@dataclass(frozen=True)
class Listing:
    seller_sku: str
    asin: str
    parent_asin: str | None
    fulfillment_channel: str

    @property
    def is_fba(self) -> bool:
        channel = self.fulfillment_channel.upper()
        return channel.startswith("AMAZON") or channel in {"AFN", "FBA"}


@dataclass(frozen=True)
class SalesTrafficRow:
    marketplace: str
    report_date: date
    granularity: str
    dimension_value: str
    seller_sku: str | None
    asin: str | None
    parent_asin: str | None
    sessions: int
    browser_sessions: int
    mobile_app_sessions: int
    page_views: int
    browser_page_views: int
    mobile_app_page_views: int
    units_ordered: int
    total_order_items: int
    ordered_product_sales_amount: Decimal
    currency_code: str
    unit_session_percentage: Decimal
    buy_box_percentage: Decimal
    fba_scope: str
    fba_scope_method: str
    raw: Mapping[str, Any]


def parse_sales_traffic(
    payload: Mapping[str, Any],
    *,
    marketplace: str,
    report_date: date | None,
    granularity: str,
    listings: Iterable[Listing],
) -> list[SalesTrafficRow]:
    granularity = granularity.upper()
    if granularity not in GRANULARITIES:
        raise ValueError(f"unsupported granularity {granularity!r}")
    if report_date is None:
        raise ValueError("report_date is required for ASIN rows")

    listing_rows = tuple(listings)
    result = []
    for item in payload.get("salesAndTrafficByAsin", []):
        scope, scope_method = _fba_scope(item, granularity, listing_rows)
        if scope in {"mixed_excluded", "unknown_excluded"}:
            continue
        result.append(
            _row(
                marketplace=marketplace,
                report_date=report_date,
                granularity=granularity,
                dimension_value=_dimension_value(item, granularity),
                seller_sku=item.get("sku"),
                asin=item.get("childAsin") or item.get("asin"),
                parent_asin=item.get("parentAsin"),
                sales=item.get("salesByAsin") or {},
                traffic=item.get("trafficByAsin") or {},
                fba_scope=scope,
                fba_scope_method=scope_method,
                raw=item,
            )
        )
    return result


def _dimension_value(item: Mapping[str, Any], granularity: str) -> str:
    key = {
        "PARENT": "parentAsin",
        "CHILD": "childAsin",
        "SKU": "sku",
    }[granularity]
    value = item.get(key)
    if not value:
        raise ValueError(f"Sales & Traffic row is missing {key}")
    return str(value)


def _fba_scope(
    item: Mapping[str, Any],
    granularity: str,
    listings: tuple[Listing, ...],
) -> tuple[str, str]:
    if granularity == "SKU":
        matches = [row for row in listings if row.seller_sku == item.get("sku")]
        if not matches:
            return "unknown_excluded", "listing_snapshot_sku_missing"
        scope = "exact" if all(row.is_fba for row in matches) else "mixed_excluded"
        return scope, "listing_snapshot_sku"

    value = item.get("childAsin" if granularity == "CHILD" else "parentAsin")
    matches = [
        row
        for row in listings
        if (row.asin == value if granularity == "CHILD" else row.parent_asin == value)
    ]
    if not matches:
        return "unknown_excluded", f"listing_snapshot_{granularity.lower()}_missing"
    if all(row.is_fba for row in matches):
        return "derived_all_fba", f"listing_snapshot_{granularity.lower()}_all_fba"
    return "mixed_excluded", f"listing_snapshot_{granularity.lower()}_mixed"


def _row(
    *,
    marketplace: str,
    report_date: date,
    granularity: str,
    dimension_value: str,
    seller_sku: str | None,
    asin: str | None,
    parent_asin: str | None,
    sales: Mapping[str, Any],
    traffic: Mapping[str, Any],
    fba_scope: str,
    fba_scope_method: str,
    raw: Mapping[str, Any],
) -> SalesTrafficRow:
    ordered_sales = sales.get("orderedProductSales") or {}
    browser_sessions = _integer(traffic.get("browserSessions"))
    mobile_app_sessions = _integer(traffic.get("mobileAppSessions"))
    browser_page_views = _integer(traffic.get("browserPageViews"))
    mobile_app_page_views = _integer(
        traffic.get("mobilePageViews"),
        fallback=_integer(traffic.get("mobileAppPageViews")),
    )
    return SalesTrafficRow(
        marketplace=marketplace,
        report_date=report_date,
        granularity=granularity,
        dimension_value=dimension_value,
        seller_sku=seller_sku,
        asin=asin,
        parent_asin=parent_asin,
        sessions=_integer(
            traffic.get("sessions"),
            fallback=browser_sessions + mobile_app_sessions,
        ),
        browser_sessions=browser_sessions,
        mobile_app_sessions=mobile_app_sessions,
        page_views=_integer(
            traffic.get("pageViews"),
            fallback=browser_page_views + mobile_app_page_views,
        ),
        browser_page_views=browser_page_views,
        mobile_app_page_views=mobile_app_page_views,
        units_ordered=_integer(sales.get("unitsOrdered")),
        total_order_items=_integer(sales.get("totalOrderItems")),
        ordered_product_sales_amount=_decimal(ordered_sales.get("amount")),
        currency_code=ordered_sales.get("currencyCode")
        or MARKETPLACES[marketplace].currency,
        unit_session_percentage=_decimal(traffic.get("unitSessionPercentage")),
        buy_box_percentage=_decimal(traffic.get("buyBoxPercentage")),
        fba_scope=fba_scope,
        fba_scope_method=fba_scope_method,
        raw=raw,
    )


def _integer(value: Any, *, fallback: int = 0) -> int:
    return int(fallback if value is None else value)


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value or 0))
