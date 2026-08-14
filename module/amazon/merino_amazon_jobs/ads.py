from __future__ import annotations

import gzip
import json
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

import requests

from merino_amazon_jobs.marketplaces import MARKETPLACES

AD_PRODUCTS = {
    "SPONSORED_PRODUCTS",
    "SPONSORED_BRANDS",
    "SPONSORED_DISPLAY",
}
ADS_ENDPOINTS = {
    "NA": "https://advertising-api.amazon.com",
    "EU": "https://advertising-api-eu.amazon.com",
    "FE": "https://advertising-api-fe.amazon.com",
}


@dataclass(frozen=True)
class AdPerformance:
    marketplace: str
    report_date: date
    profile_id: str
    ad_product: str
    campaign_id: str
    campaign_name: str | None
    ad_group_id: str
    ad_group_name: str | None
    ad_id: str
    target_id: str
    target_expression: str | None
    advertised_sku: str
    advertised_asin: str
    impressions: int
    clicks: int
    cost_amount: Decimal
    purchases: int
    units_sold: int
    attributed_sales_amount: Decimal
    currency_code: str
    attribution_window_days: int
    raw: Mapping[str, Any]


class AdsReports:
    def __init__(
        self,
        session: requests.Session,
        *,
        access_token: str,
        client_id: str,
        profile_id: str,
        base_url: str,
        sleep: Callable[[float], None] = time.sleep,
        poll_seconds: float = 30,
        max_polls: int = 120,
    ) -> None:
        self.session = session
        self.profile_id = profile_id
        self.base_url = base_url.rstrip("/")
        self.sleep = sleep
        self.poll_seconds = poll_seconds
        self.max_polls = max_polls
        self.auth_headers = {
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": client_id,
            "Amazon-Advertising-API-Scope": profile_id,
        }
        self.create_headers = {
            **self.auth_headers,
            "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
            "Accept": "application/vnd.createasyncreportresponse.v3+json",
        }
        self.status_headers = {
            **self.auth_headers,
            "Accept": "application/vnd.getasyncreportresponse.v3+json",
        }

    def download(self, specification: Mapping[str, Any]) -> list[dict[str, Any]]:
        response = self.session.post(
            f"{self.base_url}/reporting/reports",
            headers=self.create_headers,
            json=specification,
            timeout=(10, 60),
        )
        response.raise_for_status()
        report_id = response.json()["reportId"]
        for _ in range(self.max_polls):
            status = self.session.get(
                f"{self.base_url}/reporting/reports/{report_id}",
                headers=self.status_headers,
                timeout=(10, 60),
            )
            status.raise_for_status()
            body = status.json()
            if body["status"] == "COMPLETED":
                downloaded = self.session.get(body["url"], timeout=(10, 120))
                downloaded.raise_for_status()
                content = downloaded.content
                try:
                    content = gzip.decompress(content)
                except gzip.BadGzipFile:
                    pass
                return json.loads(content.decode("utf-8-sig"))
            if body["status"] in {"FAILURE", "CANCELLED"}:
                raise RuntimeError(
                    f"Amazon Ads report {report_id} ended with {body['status']}"
                )
            self.sleep(self.poll_seconds)
        raise TimeoutError(f"Amazon Ads report {report_id} did not complete")


def ads_access_token(
    session: requests.Session,
    *,
    client_id: str,
    client_secret: str,
    refresh_token: str,
) -> str:
    response = session.post(
        "https://api.amazon.com/auth/o2/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=(10, 60),
    )
    response.raise_for_status()
    return str(response.json()["access_token"])


def parse_ad_report(
    payload: Iterable[Mapping[str, Any]],
    *,
    marketplace: str,
    profile_id: str,
    ad_product: str,
    attribution_window_days: int,
) -> list[AdPerformance]:
    ad_product = ad_product.upper()
    if ad_product not in AD_PRODUCTS:
        raise ValueError(f"unsupported ad product {ad_product!r}")
    if attribution_window_days <= 0:
        raise ValueError("attribution window must be positive")
    suffix = str(attribution_window_days) + "d"
    rows = []
    for raw in payload:
        rows.append(
            AdPerformance(
                marketplace=marketplace,
                report_date=date.fromisoformat(str(raw["date"])),
                profile_id=profile_id,
                ad_product=ad_product,
                campaign_id=str(raw.get("campaignId") or ""),
                campaign_name=raw.get("campaignName"),
                ad_group_id=str(raw.get("adGroupId") or ""),
                ad_group_name=raw.get("adGroupName"),
                ad_id=str(raw.get("adId") or ""),
                target_id=str(raw.get("targetId") or raw.get("keywordId") or ""),
                target_expression=raw.get("targetingExpression")
                or raw.get("keywordText"),
                advertised_sku=str(raw.get("advertisedSku") or raw.get("sku") or ""),
                advertised_asin=str(raw.get("advertisedAsin") or raw.get("asin") or ""),
                impressions=int(raw.get("impressions") or 0),
                clicks=int(raw.get("clicks") or 0),
                cost_amount=Decimal(str(raw.get("cost") or 0)),
                purchases=int(
                    raw.get(f"purchases{suffix}") or raw.get("purchases") or 0
                ),
                units_sold=int(
                    raw.get(f"unitsSold{suffix}")
                    or raw.get(f"unitsSoldClicks{suffix}")
                    or raw.get("unitsSold")
                    or 0
                ),
                attributed_sales_amount=Decimal(
                    str(raw.get(f"sales{suffix}") or raw.get("sales") or 0)
                ),
                currency_code=str(
                    raw.get("currencyCode") or MARKETPLACES[marketplace].currency
                ),
                attribution_window_days=attribution_window_days,
                raw=raw,
            )
        )
    return rows
