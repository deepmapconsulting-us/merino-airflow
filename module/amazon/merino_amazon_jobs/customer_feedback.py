from __future__ import annotations

import json
import logging
import re
import time
import unicodedata
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

CUSTOMER_FEEDBACK_INTERVAL_SECONDS = 1.05
logger = logging.getLogger(__name__)


class CustomerFeedbackAccessDenied(RuntimeError):
    def __init__(self) -> None:
        super().__init__(
            "Customer Feedback API access was denied; authorize Brand Analytics "
            "or Selling Partner Insights for this SP-API application"
        )


class CustomerFeedback:
    def __init__(
        self,
        api: Any,
        *,
        sleep: Callable[[float], None] = time.sleep,
        interval_seconds: float = CUSTOMER_FEEDBACK_INTERVAL_SECONDS,
    ) -> None:
        self.api = api
        self.sleep = sleep
        self.interval_seconds = interval_seconds
        self._called = False

    def item_review_topics(
        self,
        asin: str,
        marketplace_id: str,
    ) -> Mapping[str, Any] | None:
        """Retrieve one ASIN's topics while respecting the account rate limit."""
        if self._called:
            self.sleep(self.interval_seconds)
        self._called = True
        try:
            raw_method = getattr(
                self.api,
                "get_item_review_topics_with_http_info",
                None,
            )
            if callable(raw_method):
                response, status, _headers = raw_method(
                    asin,
                    marketplace_id,
                    "MENTIONS",
                    _return_http_data_only=False,
                    _preload_content=False,
                )
                if status == 204:
                    return None
                if status in {401, 403}:
                    raise CustomerFeedbackAccessDenied()
                if status >= 400:
                    raise RuntimeError(f"Customer Feedback API returned HTTP {status}")
                body = response.data.decode("utf-8")
                return json.loads(body) if body else None
            response = self.api.get_item_review_topics(asin, marketplace_id, "MENTIONS")
        except Exception as error:
            return self._response_or_raise(error)
        return self._response_or_raise(response)

    @staticmethod
    def _response_or_raise(response: Any) -> Mapping[str, Any] | None:
        status = getattr(response, "status", None)
        if status == 204 or response is None:
            return None
        if status in {401, 403}:
            raise CustomerFeedbackAccessDenied()
        if isinstance(response, Mapping):
            return response
        if hasattr(response, "to_dict"):
            return response.to_dict()
        if status is not None and status >= 400:
            if isinstance(response, Exception):
                raise response
            raise RuntimeError(f"Customer Feedback API returned HTTP {status}")
        if isinstance(response, Exception):
            raise response
        raise TypeError(
            f"unsupported Customer Feedback response {type(response).__name__}"
        )


@dataclass(frozen=True)
class CatalogProduct:
    marketplace: str
    asin: str
    brand_key: str
    brand_name: str
    item_name: str | None


@dataclass(frozen=True)
class ItemReviewTopic:
    marketplace: str
    period_start: date
    period_end: date
    asin: str
    item_name: str | None
    brand_key: str
    brand_name: str
    sentiment: str
    topic_rank: int
    topic: str
    number_of_mentions: int | None
    occurrence_percentage: Decimal | None
    star_rating_impact: Decimal | None
    parent_asin_metrics: Mapping[str, Any] | None
    browse_node_metrics: Mapping[str, Any] | None
    child_asin_metrics: Mapping[str, Any] | None
    review_snippets: Sequence[str]
    subtopics: Sequence[Mapping[str, Any]]
    raw: Mapping[str, Any]


@dataclass(frozen=True)
class BrandFeedbackWeekly:
    marketplace: str
    period_start: date
    period_end: date
    brand_key: str
    brand_name: str
    catalog_asin_count: int
    feedback_asin_count: int
    feedback_coverage_percentage: Decimal
    positive_topic_count: int
    negative_topic_count: int
    positive_mention_count: int
    negative_mention_count: int
    average_positive_occurrence_percentage: Decimal | None
    average_negative_occurrence_percentage: Decimal | None
    average_positive_star_rating_impact: Decimal | None
    average_negative_star_rating_impact: Decimal | None
    raw: Mapping[str, Any]


def brand_key_from_catalog_name(brand_name: str) -> str:
    """Return a stable database key for an Amazon catalog brand name."""
    ascii_name = unicodedata.normalize("NFKD", brand_name).encode("ascii", "ignore")
    compact_punctuation = re.sub(r"[.'`]", "", ascii_name.decode().lower())
    brand_key = re.sub(r"[^a-z0-9]+", "-", compact_punctuation).strip("-")
    if not brand_key:
        raise ValueError(f"catalog brand name has no usable characters: {brand_name!r}")
    return brand_key


def catalog_product(
    payload: Mapping[str, Any],
    *,
    marketplace: str,
    marketplace_id: str,
) -> CatalogProduct:
    """Read product-brand identity from a Catalog Items response."""
    asin = str(_field(payload, "asin") or "")
    if not asin:
        raise ValueError("Catalog Items response is missing asin")
    summary = next(
        (
            item
            for item in _field(payload, "summaries", [])
            if _field(item, "marketplace_id") == marketplace_id
        ),
        None,
    )
    if summary is None:
        raise ValueError(
            f"Catalog Items response for {asin} has no {marketplace_id} summary"
        )
    brand_name = str(
        _field(summary, "brand_name")
        or _field(summary, "brand")
        or _catalog_attribute(payload, "brand", marketplace_id)
        or ""
    ).strip()
    if not brand_name:
        raise ValueError(f"Catalog Items response for {asin} is missing brandName")
    return CatalogProduct(
        marketplace=marketplace,
        asin=asin,
        brand_key=brand_key_from_catalog_name(brand_name),
        brand_name=brand_name,
        item_name=_field(summary, "item_name"),
    )


def parse_item_review_topics(
    payload: Mapping[str, Any],
    *,
    brand_key: str,
    brand_name: str,
) -> list[ItemReviewTopic]:
    """Flatten positive and negative Customer Feedback topics."""
    date_range = _field(payload, "date_range") or {}
    period_start = _date(_field(date_range, "start_date"))
    period_end = _date(_field(date_range, "end_date"))
    marketplace_id = str(_field(payload, "marketplace_id") or "")
    marketplace = "US" if marketplace_id == "ATVPDKIKX0DER" else marketplace_id
    asin = str(_field(payload, "asin") or "")
    if not asin or not marketplace_id:
        raise ValueError("Customer Feedback response is missing asin or marketplaceId")

    rows = []
    topic_groups = (
        (
            "positive",
            _field(_field(payload, "topics") or {}, "positive_topics", []) or [],
        ),
        (
            "negative",
            _field(_field(payload, "topics") or {}, "negative_topics", []) or [],
        ),
    )
    for sentiment, topics in topic_groups:
        for rank, topic in enumerate(topics, start=1):
            topic_name = str(_field(topic, "topic") or "").strip()
            if not topic_name:
                continue
            metrics = _field(topic, "asin_metrics") or {}
            rows.append(
                ItemReviewTopic(
                    marketplace=marketplace,
                    period_start=period_start,
                    period_end=period_end,
                    asin=asin,
                    item_name=_field(payload, "item_name"),
                    brand_key=brand_key,
                    brand_name=brand_name,
                    sentiment=sentiment,
                    topic_rank=rank,
                    topic=topic_name,
                    number_of_mentions=_integer(_field(metrics, "number_of_mentions")),
                    occurrence_percentage=_decimal(
                        _field(metrics, "occurrence_percentage")
                    ),
                    star_rating_impact=_decimal(_field(metrics, "star_rating_impact")),
                    parent_asin_metrics=_mapping(_field(topic, "parent_asin_metrics")),
                    browse_node_metrics=_mapping(_field(topic, "browse_node_metrics")),
                    child_asin_metrics=_mapping(_field(topic, "child_asin_metrics")),
                    review_snippets=_field(topic, "review_snippets", []) or [],
                    subtopics=[
                        _mapping(subtopic)
                        for subtopic in _field(topic, "subtopics", []) or []
                    ],
                    raw=_mapping(topic),
                )
            )
    return rows


def fetch_customer_feedback(
    catalog_api: Any,
    feedback_api: Any,
    asins: Sequence[str],
    *,
    marketplace: str,
    marketplace_id: str,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[list[CatalogProduct], list[ItemReviewTopic]]:
    """Fetch brand identity and current review topics for child ASINs."""
    feedback = CustomerFeedback(feedback_api, sleep=sleep)
    products = []
    topics = []
    for asin in asins:
        try:
            catalog_response = catalog_api.get_catalog_item(
                asin,
                [marketplace_id],
                included_data=["attributes", "summaries"],
            )
        except Exception as error:
            if getattr(error, "status", None) != 404:
                raise
            logger.warning(
                "catalog item unavailable; skipping asin=%s marketplace_id=%s",
                asin,
                marketplace_id,
            )
            continue
        product = catalog_product(
            _mapping(catalog_response) or {},
            marketplace=marketplace,
            marketplace_id=marketplace_id,
        )
        products.append(product)
        payload = feedback.item_review_topics(asin, marketplace_id)
        if payload is not None:
            topics.extend(
                parse_item_review_topics(
                    payload,
                    brand_key=product.brand_key,
                    brand_name=product.brand_name,
                )
            )
    return products, topics


def aggregate_brand_feedback(
    products: Sequence[CatalogProduct],
    topics: Sequence[ItemReviewTopic],
) -> list[BrandFeedbackWeekly]:
    """Aggregate Amazon's returned top review topics at product-brand grain."""
    catalog_counts: dict[tuple[str, str], int] = {}
    brand_names: dict[tuple[str, str], str] = {}
    for product in products:
        key = (product.marketplace, product.brand_key)
        catalog_counts[key] = catalog_counts.get(key, 0) + 1
        brand_names[key] = product.brand_name

    groups: dict[tuple[str, str, date, date], list[ItemReviewTopic]] = {}
    for topic in topics:
        key = (
            topic.marketplace,
            topic.brand_key,
            topic.period_start,
            topic.period_end,
        )
        groups.setdefault(key, []).append(topic)

    periods = sorted(
        {(row.marketplace, row.period_start, row.period_end) for row in topics}
    )
    summaries = []
    aggregate_keys = (
        (marketplace, brand_key, period_start, period_end)
        for marketplace, period_start, period_end in periods
        for product_marketplace, brand_key in catalog_counts
        if product_marketplace == marketplace
    )
    for marketplace, brand_key, period_start, period_end in aggregate_keys:
        rows = groups.get(
            (marketplace, brand_key, period_start, period_end),
            [],
        )
        positive = [row for row in rows if row.sentiment == "positive"]
        negative = [row for row in rows if row.sentiment == "negative"]
        catalog_asin_count = catalog_counts[(marketplace, brand_key)]
        feedback_asin_count = len({row.asin for row in rows})
        summaries.append(
            BrandFeedbackWeekly(
                marketplace=marketplace,
                period_start=period_start,
                period_end=period_end,
                brand_key=brand_key,
                brand_name=brand_names[(marketplace, brand_key)],
                catalog_asin_count=catalog_asin_count,
                feedback_asin_count=feedback_asin_count,
                feedback_coverage_percentage=(
                    Decimal(feedback_asin_count)
                    * Decimal(100)
                    / Decimal(catalog_asin_count)
                ),
                positive_topic_count=len(positive),
                negative_topic_count=len(negative),
                positive_mention_count=sum(
                    row.number_of_mentions or 0 for row in positive
                ),
                negative_mention_count=sum(
                    row.number_of_mentions or 0 for row in negative
                ),
                average_positive_occurrence_percentage=_average(
                    row.occurrence_percentage for row in positive
                ),
                average_negative_occurrence_percentage=_average(
                    row.occurrence_percentage for row in negative
                ),
                average_positive_star_rating_impact=_average(
                    row.star_rating_impact for row in positive
                ),
                average_negative_star_rating_impact=_average(
                    row.star_rating_impact for row in negative
                ),
                raw={
                    "scope": "top_item_review_topics",
                    "catalogAsinCount": catalog_asin_count,
                    "feedbackAsins": sorted({row.asin for row in rows}),
                },
            )
        )
    return summaries


def _date(value: Any) -> date:
    if not value:
        raise ValueError("Customer Feedback response is missing dateRange")
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()


def _integer(value: Any) -> int | None:
    return int(value) if value is not None else None


def _decimal(value: Any) -> Decimal | None:
    return Decimal(str(value)) if value is not None else None


def _average(values: Iterable[Decimal | None]) -> Decimal | None:
    present = [value for value in values if value is not None]
    return sum(present, Decimal(0)) / len(present) if present else None


def _mapping(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if hasattr(value, "__dict__"):
        return vars(value)
    raise TypeError(f"expected mapping, got {type(value).__name__}")


def _field(value: Any, name: str, default: Any = None) -> Any:
    mapping = _mapping(value) or {}
    camel = name.split("_")[0] + "".join(part.title() for part in name.split("_")[1:])
    return mapping.get(name, mapping.get(camel, default))


def _catalog_attribute(
    payload: Mapping[str, Any],
    name: str,
    marketplace_id: str,
) -> Any:
    attributes = _field(payload, "attributes") or {}
    values = _field(attributes, name, []) or []
    matching = next(
        (
            value
            for value in values
            if _field(value, "marketplace_id") == marketplace_id
        ),
        None,
    )
    return _field(matching, "value") if matching is not None else None
