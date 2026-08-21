from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from merino_amazon_jobs.marketplaces import MARKETPLACES

REPORT_TYPE = "GET_MERCHANT_LISTINGS_ALL_DATA"


@dataclass(frozen=True)
class ListingSnapshot:
    marketplace: str
    snapshot_date: date
    seller_sku: str
    asin: str | None
    parent_asin: str | None
    fnsku: str | None
    item_name: str | None
    listing_status: str | None
    fulfillment_channel: str
    quantity: int | None
    price_amount: Decimal | None
    currency_code: str | None
    open_date: datetime | None
    raw: dict[str, str]


def parse_listing_tsv(
    payload: str,
    *,
    marketplace: str,
    snapshot_date: date,
) -> list[ListingSnapshot]:
    rows = []
    for raw in csv.DictReader(io.StringIO(payload), delimiter="\t"):
        seller_sku = _value(raw, "seller-sku", "sku")
        if not seller_sku:
            continue
        price = _decimal(_value(raw, "price", "standard-price"))
        rows.append(
            ListingSnapshot(
                marketplace=marketplace,
                snapshot_date=snapshot_date,
                seller_sku=seller_sku,
                asin=_value(raw, "asin1", "asin"),
                parent_asin=_value(raw, "parent-asin"),
                fnsku=_value(raw, "fnsku"),
                item_name=_value(raw, "item-name", "product-name"),
                listing_status=_value(raw, "status", "item-condition"),
                fulfillment_channel=_fulfillment_channel(
                    _value(raw, "fulfillment-channel", "fulfillment-channel-code")
                ),
                quantity=_integer(_value(raw, "quantity")),
                price_amount=price,
                currency_code=(
                    MARKETPLACES[marketplace].currency if price is not None else None
                ),
                open_date=_datetime(_value(raw, "open-date")),
                raw=dict(raw),
            )
        )
    return rows


def _value(row: dict[str, Any], *aliases: str) -> str | None:
    lowered = {key.strip().lower(): value for key, value in row.items() if key}
    for alias in aliases:
        value = lowered.get(alias)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _fulfillment_channel(value: str | None) -> str:
    channel = (value or "").upper()
    if channel in {"AFN", "FBA"}:
        return "AFN"
    if channel.startswith("AMAZON"):
        return "AMAZON"
    if channel == "MFN":
        return "MFN"
    if channel.startswith("MERCHANT"):
        return "MERCHANT"
    return "UNKNOWN"


def _integer(value: str | None) -> int | None:
    return int(value) if value is not None else None


def _decimal(value: str | None) -> Decimal | None:
    return Decimal(value) if value is not None else None


def _datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    timestamp, separator, abbreviation = value.rpartition(" ")
    utc_offsets = {
        "HST": timedelta(hours=-10),
        "AKST": timedelta(hours=-9),
        "AKDT": timedelta(hours=-8),
        "PST": timedelta(hours=-8),
        "PDT": timedelta(hours=-7),
        "MST": timedelta(hours=-7),
        "MDT": timedelta(hours=-6),
        "CST": timedelta(hours=-6),
        "CDT": timedelta(hours=-5),
        "EST": timedelta(hours=-5),
        "EDT": timedelta(hours=-4),
        "AST": timedelta(hours=-4),
        "ADT": timedelta(hours=-3),
        "NST": timedelta(hours=-3, minutes=-30),
        "NDT": timedelta(hours=-2, minutes=-30),
        "BRT": timedelta(hours=-3),
        "BRST": timedelta(hours=-2),
        "AWST": timedelta(hours=8),
        "ACST": timedelta(hours=9, minutes=30),
        "ACDT": timedelta(hours=10, minutes=30),
        "AEST": timedelta(hours=10),
        "AEDT": timedelta(hours=11),
    }
    if separator and abbreviation in utc_offsets:
        date_format = "%d/%m/%Y %H:%M:%S" if "/" in timestamp else "%Y-%m-%d %H:%M:%S"
        return datetime.strptime(timestamp, date_format).replace(
            tzinfo=timezone(utc_offsets[abbreviation])
        )
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
