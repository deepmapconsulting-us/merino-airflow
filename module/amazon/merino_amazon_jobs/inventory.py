from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import date
from typing import Any


@dataclass(frozen=True)
class FbaInventorySnapshot:
    marketplace: str
    snapshot_date: date
    seller_sku: str
    fnsku: str
    asin: str | None
    condition: str | None
    fulfillable_quantity: int
    inbound_working_quantity: int
    inbound_shipped_quantity: int
    inbound_receiving_quantity: int
    reserved_customer_order_quantity: int
    reserved_fc_transfer_quantity: int
    reserved_fc_processing_quantity: int
    unfulfillable_quantity: int
    researching_quantity: int
    total_quantity: int
    raw: dict[str, Any]


@dataclass(frozen=True)
class FbaInventoryAgeSnapshot:
    marketplace: str
    snapshot_date: date
    seller_sku: str
    fnsku: str
    asin: str | None
    age_0_30_days: int
    age_31_60_days: int
    age_61_90_days: int
    age_91_180_days: int
    age_181_330_days: int
    age_331_365_days: int
    age_365_plus_days: int
    snapshot_method: str
    raw: dict[str, str]


class FbaInventorySummaries:
    def __init__(self, api: Any) -> None:
        self.api = api

    def all(self, marketplace_id: str) -> list[dict[str, Any]]:
        summaries: list[dict[str, Any]] = []
        next_token = None
        while True:
            kwargs: dict[str, Any] = {"details": True}
            if next_token:
                kwargs["next_token"] = next_token
            response = self.api.get_inventory_summaries(
                "Marketplace",
                marketplace_id,
                [marketplace_id],
                **kwargs,
            )
            payload = _mapping(_field(response, "payload", response))
            summaries.extend(
                _mapping(item)
                for item in _field(payload, "inventory_summaries", []) or []
            )
            pagination = _mapping(_field(response, "pagination", {}) or {})
            next_token = _field(pagination, "next_token") or _field(
                pagination, "nextToken"
            )
            if not next_token:
                return summaries


def parse_inventory_summaries(
    payloads: list[dict[str, Any]],
    *,
    marketplace: str,
    snapshot_date: date,
) -> list[FbaInventorySnapshot]:
    rows = []
    for raw in payloads:
        details = _mapping(_field(raw, "inventory_details", {}) or {})
        reserved = _mapping(_field(details, "reserved_quantity", {}) or {})
        unfulfillable = _mapping(_field(details, "unfulfillable_quantity", {}) or {})
        researching = _mapping(_field(details, "researching_quantity", {}) or {})
        rows.append(
            FbaInventorySnapshot(
                marketplace=marketplace,
                snapshot_date=snapshot_date,
                seller_sku=str(_field(raw, "seller_sku", "")),
                fnsku=str(_field(raw, "fn_sku", "") or ""),
                asin=_field(raw, "asin"),
                condition=_field(raw, "condition"),
                fulfillable_quantity=_quantity(details, "fulfillable_quantity"),
                inbound_working_quantity=_quantity(details, "inbound_working_quantity"),
                inbound_shipped_quantity=_quantity(details, "inbound_shipped_quantity"),
                inbound_receiving_quantity=_quantity(
                    details, "inbound_receiving_quantity"
                ),
                reserved_customer_order_quantity=_quantity(
                    reserved, "customer_order_quantity"
                ),
                reserved_fc_transfer_quantity=_quantity(
                    reserved, "fc_transfer_quantity"
                ),
                reserved_fc_processing_quantity=_quantity(
                    reserved, "fc_processing_quantity"
                ),
                unfulfillable_quantity=_quantity(
                    unfulfillable, "total_unfulfillable_quantity"
                ),
                researching_quantity=_quantity(
                    researching, "total_researching_quantity"
                ),
                total_quantity=_quantity(raw, "total_quantity"),
                raw=raw,
            )
        )
    return rows


def parse_inventory_age_tsv(
    payload: str,
    *,
    marketplace: str,
    snapshot_date: date,
) -> list[FbaInventoryAgeSnapshot]:
    result = []
    for raw in csv.DictReader(io.StringIO(payload), delimiter="\t"):
        row = {_header(key): value for key, value in raw.items() if key}
        seller_sku = _alias(row, "sku", "seller-sku", "seller-sku-name")
        if not seller_sku:
            continue
        result.append(
            FbaInventoryAgeSnapshot(
                marketplace=marketplace,
                snapshot_date=snapshot_date,
                seller_sku=seller_sku,
                fnsku=_alias(row, "fnsku") or "",
                asin=_alias(row, "asin"),
                age_0_30_days=_age(row, "inv-age-0-to-30-days", "age-0-30-days"),
                age_31_60_days=_age(row, "inv-age-31-to-60-days", "age-31-60-days"),
                age_61_90_days=_age(row, "inv-age-61-to-90-days", "age-61-90-days"),
                age_91_180_days=_age(row, "inv-age-91-to-180-days", "age-91-180-days"),
                age_181_330_days=_age(
                    row, "inv-age-181-to-330-days", "age-181-330-days"
                ),
                age_331_365_days=_age(
                    row, "inv-age-331-to-365-days", "age-331-365-days"
                ),
                age_365_plus_days=_age(
                    row,
                    "inv-age-365-plus-days",
                    "inv-age-365-plus",
                    "age-365-plus-days",
                ),
                snapshot_method="observed",
                raw=dict(raw),
            )
        )
    return result


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if hasattr(value, "__dict__"):
        return vars(value)
    return {}


def _field(value: Any, name: str, default: Any = None) -> Any:
    mapping = _mapping(value)
    camel = name.split("_")[0] + "".join(part.title() for part in name.split("_")[1:])
    return mapping.get(name, mapping.get(camel, default))


def _quantity(value: Any, name: str) -> int:
    return int(_field(value, name, 0) or 0)


def _header(value: str) -> str:
    return value.strip().lower().replace("_", "-").replace(" ", "-")


def _alias(row: dict[str, str], *names: str) -> str | None:
    for name in names:
        value = row.get(name)
        if value is not None and value.strip():
            return value.strip()
    return None


def _age(row: dict[str, str], *names: str) -> int:
    return int(_alias(row, *names) or 0)
