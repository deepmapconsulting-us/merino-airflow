from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class AmazonOrder:
    marketplace_id: str
    amazon_order_id: str
    purchase_date: datetime
    last_update_date: datetime
    order_status: str
    fulfillment_channel: str
    sales_channel: str | None
    ship_service_level: str | None
    number_of_items_shipped: int
    number_of_items_unshipped: int
    order_total_amount: Decimal | None
    currency_code: str | None
    is_business_order: bool
    is_prime: bool
    raw: dict[str, Any]


@dataclass(frozen=True)
class AmazonOrderItem:
    marketplace_id: str
    amazon_order_id: str
    order_item_id: str
    asin: str | None
    seller_sku: str | None
    title: str | None
    quantity_ordered: int
    quantity_shipped: int
    item_price_amount: Decimal | None
    item_tax_amount: Decimal | None
    shipping_price_amount: Decimal | None
    shipping_tax_amount: Decimal | None
    promotion_discount_amount: Decimal | None
    currency_code: str | None
    raw: dict[str, Any]


class Orders:
    def __init__(self, api: Any) -> None:
        self.api = api

    def all(
        self,
        *,
        marketplace_id: str,
        created_after: datetime,
        created_before: datetime,
    ) -> list[dict[str, Any]]:
        result = []
        token = None
        while True:
            kwargs: dict[str, Any] = {
                "marketplace_ids": [marketplace_id],
                "created_after": created_after,
                "created_before": created_before,
                "fulfilled_by": ["AMAZON"],
                "included_data": ["fulfillment", "proceeds"],
            }
            if token:
                kwargs["pagination_token"] = token
            payload = _mapping(self.api.search_orders(**kwargs))
            result.extend(_mapping(row) for row in _field(payload, "orders", []) or [])
            pagination = _mapping(_field(payload, "pagination", {}) or {})
            token = _field(pagination, "next_token")
            if not token:
                return result


def parse_orders(
    payloads: list[dict[str, Any]],
    *,
    marketplace_id: str | None = None,
) -> tuple[list[AmazonOrder], list[AmazonOrderItem]]:
    orders = []
    items = []
    for payload in payloads:
        raw_order = _without_pii(payload)
        sales_channel = _mapping(_field(payload, "sales_channel", {}) or {})
        order_marketplace_id = str(
            _field(payload, "marketplace_id")
            or _field(sales_channel, "marketplace_id")
            or marketplace_id
            or ""
        )
        amazon_order_id = str(
            _field(payload, "amazon_order_id") or _field(payload, "order_id") or ""
        )
        fulfillment = _mapping(_field(payload, "fulfillment", {}) or {})
        proceeds = _mapping(_field(payload, "proceeds", {}) or {})
        total = _money(
            _field(payload, "order_total") or _field(proceeds, "grand_total")
        )
        programs = {
            str(program).upper() for program in _field(payload, "programs", []) or []
        }
        orders.append(
            AmazonOrder(
                marketplace_id=order_marketplace_id,
                amazon_order_id=amazon_order_id,
                purchase_date=_datetime(
                    _field(payload, "purchase_date") or _field(payload, "created_time")
                ),
                last_update_date=_datetime(
                    _field(payload, "last_update_date")
                    or _field(payload, "last_updated_time")
                ),
                order_status=str(
                    _field(payload, "order_status")
                    or _field(fulfillment, "fulfillment_status")
                    or ""
                ),
                fulfillment_channel=_fba_channel(
                    _field(payload, "fulfillment_channel")
                    or _field(fulfillment, "fulfilled_by")
                ),
                sales_channel=_field(sales_channel, "channel_name")
                or (
                    _field(payload, "sales_channel")
                    if isinstance(_field(payload, "sales_channel"), str)
                    else None
                ),
                ship_service_level=_field(payload, "ship_service_level")
                or _field(fulfillment, "fulfillment_service_level"),
                number_of_items_shipped=int(
                    _field(payload, "number_of_items_shipped", 0) or 0
                ),
                number_of_items_unshipped=int(
                    _field(payload, "number_of_items_unshipped", 0) or 0
                ),
                order_total_amount=total[0],
                currency_code=total[1],
                is_business_order=bool(
                    _field(payload, "is_business_order", False)
                    or "AMAZON_BUSINESS" in programs
                    or "BUSINESS" in programs
                ),
                is_prime=bool(
                    _field(payload, "is_prime", False) or "PRIME" in programs
                ),
                raw=raw_order,
            )
        )
        for item in _field(payload, "order_items", []) or []:
            item = _mapping(item)
            product = _mapping(_field(item, "product", {}) or {})
            item_fulfillment = _mapping(_field(item, "fulfillment", {}) or {})
            quantity_ordered = int(_field(item, "quantity_ordered", 0) or 0)
            unit_price = _money(
                _field(_field(product, "price", {}) or {}, "unit_price")
            )
            money = [
                (
                    (
                        (
                            unit_price[0] * quantity_ordered
                            if unit_price[0] is not None
                            else None
                        ),
                        unit_price[1],
                    )
                    if name == "item_price" and unit_price[0] is not None
                    else _money(_field(item, name))
                )
                for name in (
                    "item_price",
                    "item_tax",
                    "shipping_price",
                    "shipping_tax",
                    "promotion_discount",
                )
            ]
            currencies = {currency for _, currency in money if currency}
            if len(currencies) > 1:
                raise ValueError(
                    f"order item {_field(item, 'order_item_id')} has mixed currencies"
                )
            items.append(
                AmazonOrderItem(
                    marketplace_id=order_marketplace_id,
                    amazon_order_id=amazon_order_id,
                    order_item_id=str(_field(item, "order_item_id", "")),
                    asin=_field(item, "asin") or _field(product, "asin"),
                    seller_sku=_field(item, "seller_sku")
                    or _field(product, "seller_sku"),
                    title=_field(item, "title") or _field(product, "title"),
                    quantity_ordered=quantity_ordered,
                    quantity_shipped=int(
                        _field(item, "quantity_shipped")
                        or _field(item_fulfillment, "quantity_fulfilled")
                        or 0
                    ),
                    item_price_amount=money[0][0],
                    item_tax_amount=money[1][0],
                    shipping_price_amount=money[2][0],
                    shipping_tax_amount=money[3][0],
                    promotion_discount_amount=money[4][0],
                    currency_code=next(iter(currencies), None),
                    raw=_without_pii(item),
                )
            )
    return orders, items


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


def _without_pii(value: dict[str, Any]) -> dict[str, Any]:
    pii = {
        "buyerInfo",
        "buyer_info",
        "buyer",
        "recipientInfo",
        "recipient_info",
        "recipient",
        "shippingAddress",
        "shipping_address",
    }
    result = {}
    for key, item in value.items():
        if key in pii:
            continue
        if isinstance(item, dict):
            result[key] = _without_pii(item)
        elif isinstance(item, list):
            result[key] = [
                _without_pii(entry) if isinstance(entry, dict) else entry
                for entry in item
            ]
        else:
            result[key] = item
    return result


def _datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _money(value: Any) -> tuple[Decimal | None, str | None]:
    if not value:
        return None, None
    amount = _field(value, "amount")
    currency = _field(value, "currency_code")
    return (Decimal(str(amount)) if amount is not None else None, currency)


def _fba_channel(value: Any) -> str:
    return "AFN" if str(value or "").upper() == "AFN" else "AMAZON"
