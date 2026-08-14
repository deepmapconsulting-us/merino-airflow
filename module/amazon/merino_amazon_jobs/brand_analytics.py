from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

REPORT_TYPE = "GET_BRAND_ANALYTICS_SEARCH_CATALOG_PERFORMANCE_REPORT"
GRANULARITIES = {"WEEK", "MONTH", "QUARTER"}


class BrandAnalyticsUnauthorized(RuntimeError):
    def __init__(self) -> None:
        super().__init__(
            "Brand Analytics report access was denied; the seller must be "
            "Brand Registry enrolled and the SP-API application must be authorized"
        )


@dataclass(frozen=True)
class SearchCatalogPerformance:
    marketplace: str
    period_start: date
    period_end: date
    period_granularity: str
    asin: str
    product_title: str | None
    impressions: int
    clicks: int
    cart_adds: int
    purchases: int
    click_rate: Decimal | None
    cart_add_rate: Decimal | None
    purchase_rate: Decimal | None
    shipped_units: int
    shipped_revenue_amount: Decimal | None
    currency_code: str | None
    raw: Mapping[str, Any]


def parse_search_catalog_performance(
    payload: Mapping[str, Any],
    *,
    marketplace: str,
    period_start: date,
    period_end: date,
    granularity: str,
) -> list[SearchCatalogPerformance]:
    granularity = granularity.upper()
    if granularity not in GRANULARITIES:
        raise ValueError("granularity must be WEEK, MONTH, or QUARTER")
    result = []
    for raw in payload.get("dataByAsin", payload.get("searchCatalogPerformance", [])):
        asin = raw.get("asin")
        if not asin:
            continue
        revenue = (
            raw.get("shippedRevenue")
            or raw.get("shippedSales")
            or raw.get("shippedRevenueAmount")
            or {}
        )
        result.append(
            SearchCatalogPerformance(
                marketplace=marketplace,
                period_start=period_start,
                period_end=period_end,
                period_granularity=granularity,
                asin=str(asin),
                product_title=raw.get("productTitle"),
                impressions=_integer(
                    raw.get("impressions", raw.get("impressionCount"))
                ),
                clicks=_integer(raw.get("clicks", raw.get("clickCount"))),
                cart_adds=_integer(raw.get("cartAdds", raw.get("cartAddCount"))),
                purchases=_integer(raw.get("purchases", raw.get("purchaseCount"))),
                click_rate=_decimal(raw.get("clickRate")),
                cart_add_rate=_decimal(raw.get("cartAddRate")),
                purchase_rate=_decimal(raw.get("purchaseRate")),
                shipped_units=_integer(
                    raw.get("shippedUnits", raw.get("shippedUnitCount"))
                ),
                shipped_revenue_amount=_decimal(revenue.get("amount")),
                currency_code=revenue.get("currencyCode"),
                raw=raw,
            )
        )
    return result


def raise_if_unauthorized(error: Exception) -> None:
    if getattr(error, "status", None) in {401, 403}:
        raise BrandAnalyticsUnauthorized() from error
    raise error


def _integer(value: Any) -> int:
    return int(value or 0)


def _decimal(value: Any) -> Decimal | None:
    return Decimal(str(value)) if value is not None else None
