from dataclasses import replace
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from merino_amazon_jobs.customer_feedback import (
    CatalogProduct,
    CustomerFeedback,
    CustomerFeedbackAccessDenied,
    aggregate_brand_feedback,
    brand_key_from_catalog_name,
    catalog_product,
    fetch_customer_feedback,
    parse_item_review_topics,
)
from merino_amazon_jobs.source_postgres import AmazonSourceStore


def test_catalog_product_uses_us_summary_brand() -> None:
    product = catalog_product(
        {
            "asin": "B000000001",
            "summaries": [
                {
                    "marketplaceId": "ATVPDKIKX0DER",
                    "brandName": "GIAGIO",
                    "itemName": "Silk Underwear",
                }
            ],
        },
        marketplace="US",
        marketplace_id="ATVPDKIKX0DER",
    )

    assert product == CatalogProduct(
        marketplace="US",
        asin="B000000001",
        brand_key="giagio",
        brand_name="GIAGIO",
        item_name="Silk Underwear",
    )


def test_catalog_brand_key_is_stable_and_domain_readable() -> None:
    assert brand_key_from_catalog_name("  Brand & Co.  ") == "brand-co"
    assert brand_key_from_catalog_name("GIAG.IO") == "giagio"


def test_catalog_product_accepts_sdk_summary_brand_field() -> None:
    product = catalog_product(
        {
            "asin": "B0DP2DZZDB",
            "summaries": [
                {
                    "marketplace_id": "ATVPDKIKX0DER",
                    "brand": "GIAG.IO",
                    "item_name": "Sports Bra",
                }
            ],
        },
        marketplace="US",
        marketplace_id="ATVPDKIKX0DER",
    )

    assert product.brand_key == "giagio"
    assert product.brand_name == "GIAG.IO"


def test_item_review_topics_preserve_sentiment_rank_and_nested_metrics() -> None:
    rows = parse_item_review_topics(
        {
            "asin": "B000000001",
            "itemName": "Silk Underwear",
            "marketplaceId": "ATVPDKIKX0DER",
            "dateRange": {
                "startDate": "2026-08-02T00:00:00Z",
                "endDate": "2026-08-08T23:59:59Z",
            },
            "topics": {
                "positiveTopics": [
                    {
                        "topic": "Comfort",
                        "asinMetrics": {
                            "numberOfMentions": 12,
                            "occurrencePercentage": 30.5,
                            "starRatingImpact": 0.4,
                        },
                        "reviewSnippets": ["Very comfortable"],
                        "subtopics": [
                            {"subtopic": "Soft", "metrics": {"numberOfMentions": 5}}
                        ],
                    }
                ],
                "negativeTopics": [
                    {
                        "topic": "Sizing",
                        "asinMetrics": {
                            "numberOfMentions": 4,
                            "occurrencePercentage": 10,
                            "starRatingImpact": -0.7,
                        },
                        "parentAsinMetrics": {"numberOfMentions": 9},
                    }
                ],
            },
        },
        brand_key="giagio",
        brand_name="GIAGIO",
    )

    assert len(rows) == 2
    assert rows[0].sentiment == "positive"
    assert rows[0].topic_rank == 1
    assert rows[0].period_start == date(2026, 8, 2)
    assert rows[0].number_of_mentions == 12
    assert rows[0].occurrence_percentage == Decimal("30.5")
    assert rows[0].review_snippets == ["Very comfortable"]
    assert rows[1].sentiment == "negative"
    assert rows[1].star_rating_impact == Decimal("-0.7")
    assert rows[1].parent_asin_metrics == {"numberOfMentions": 9}


def test_brand_aggregate_counts_catalog_coverage_and_topic_metrics() -> None:
    products = [
        CatalogProduct("US", "B000000001", "giagio", "GIAGIO", "One"),
        CatalogProduct("US", "B000000002", "giagio", "GIAGIO", "Two"),
        CatalogProduct("US", "B000000003", "second-brand", "Second Brand", "Three"),
    ]
    topics = parse_item_review_topics(
        {
            "asin": "B000000001",
            "itemName": "One",
            "marketplaceId": "ATVPDKIKX0DER",
            "dateRange": {
                "startDate": "2026-08-02T00:00:00Z",
                "endDate": "2026-08-08T23:59:59Z",
            },
            "topics": {
                "positiveTopics": [
                    {
                        "topic": "Comfort",
                        "asinMetrics": {
                            "numberOfMentions": 12,
                            "occurrencePercentage": 30,
                            "starRatingImpact": 0.5,
                        },
                    }
                ],
                "negativeTopics": [
                    {
                        "topic": "Sizing",
                        "asinMetrics": {
                            "numberOfMentions": 4,
                            "occurrencePercentage": 10,
                            "starRatingImpact": -0.5,
                        },
                    }
                ],
            },
        },
        brand_key="giagio",
        brand_name="GIAGIO",
    )

    summaries = aggregate_brand_feedback(products, topics)

    assert len(summaries) == 2
    summary = next(row for row in summaries if row.brand_key == "giagio")
    assert summary.brand_key == "giagio"
    assert summary.catalog_asin_count == 2
    assert summary.feedback_asin_count == 1
    assert summary.feedback_coverage_percentage == Decimal("50")
    assert summary.positive_topic_count == 1
    assert summary.negative_topic_count == 1
    assert summary.positive_mention_count == 12
    assert summary.negative_mention_count == 4
    assert summary.average_positive_occurrence_percentage == Decimal("30")
    assert summary.average_negative_star_rating_impact == Decimal("-0.5")
    second = next(row for row in summaries if row.brand_key == "second-brand")
    assert second.catalog_asin_count == 1
    assert second.feedback_asin_count == 0
    assert second.feedback_coverage_percentage == Decimal("0")


def test_customer_feedback_client_paces_calls_and_accepts_no_content() -> None:
    api = MagicMock()
    api.get_item_review_topics_with_http_info = None
    api.get_item_review_topics.side_effect = [
        {"asin": "B000000001"},
        SimpleNamespace(status=204),
    ]
    sleeps: list[float] = []
    client = CustomerFeedback(api, sleep=sleeps.append)

    assert client.item_review_topics("B000000001", "ATVPDKIKX0DER") == {
        "asin": "B000000001"
    }
    assert client.item_review_topics("B000000002", "ATVPDKIKX0DER") is None
    assert sleeps == [1.05]
    api.get_item_review_topics.assert_called_with(
        "B000000002",
        "ATVPDKIKX0DER",
        "MENTIONS",
    )


def test_customer_feedback_client_explains_authorization_failure() -> None:
    api = MagicMock()
    api.get_item_review_topics_with_http_info = None
    api.get_item_review_topics.return_value = SimpleNamespace(status=403)

    try:
        CustomerFeedback(api).item_review_topics("B000000001", "ATVPDKIKX0DER")
    except CustomerFeedbackAccessDenied as error:
        assert "Brand Analytics or Selling Partner Insights" in str(error)
    else:
        raise AssertionError("expected CustomerFeedbackAccessDenied")


def test_customer_feedback_client_handles_sdk_204_without_deserialization() -> None:
    api = MagicMock()
    api.get_item_review_topics_with_http_info.return_value = (
        SimpleNamespace(data=b""),
        204,
        {},
    )

    payload = CustomerFeedback(api).item_review_topics(
        "B000000001",
        "ATVPDKIKX0DER",
    )

    assert payload is None
    api.get_item_review_topics.assert_not_called()


def test_fetch_customer_feedback_joins_catalog_brand_and_skips_204() -> None:
    catalog_api = MagicMock()
    catalog_api.get_catalog_item.side_effect = [
        {
            "asin": "B000000001",
            "summaries": [
                {
                    "marketplaceId": "ATVPDKIKX0DER",
                    "brandName": "GIAGIO",
                    "itemName": "One",
                }
            ],
        },
        {
            "asin": "B000000002",
            "summaries": [
                {
                    "marketplaceId": "ATVPDKIKX0DER",
                    "brandName": "Second Brand",
                    "itemName": "Two",
                }
            ],
        },
    ]
    feedback_api = MagicMock()
    feedback_api.get_item_review_topics_with_http_info = None
    feedback_api.get_item_review_topics.side_effect = [
        {
            "asin": "B000000001",
            "itemName": "One",
            "marketplaceId": "ATVPDKIKX0DER",
            "dateRange": {
                "startDate": "2026-08-02T00:00:00Z",
                "endDate": "2026-08-08T23:59:59Z",
            },
            "topics": {
                "positiveTopics": [
                    {"topic": "Comfort", "asinMetrics": {"numberOfMentions": 2}}
                ]
            },
        },
        SimpleNamespace(status=204),
    ]

    products, topics = fetch_customer_feedback(
        catalog_api,
        feedback_api,
        ["B000000001", "B000000002"],
        marketplace="US",
        marketplace_id="ATVPDKIKX0DER",
        sleep=lambda _seconds: None,
    )

    assert [product.brand_key for product in products] == [
        "giagio",
        "second-brand",
    ]
    assert len(topics) == 1
    assert topics[0].brand_key == "giagio"
    catalog_api.get_catalog_item.assert_any_call(
        "B000000001",
        ["ATVPDKIKX0DER"],
        included_data=["attributes", "summaries"],
    )


class FakeCursor:
    def __init__(self) -> None:
        self.executions: list[tuple[str, tuple[object, ...]]] = []
        self.brand_ids = {"giagio": 1, "second-brand": 2}
        self.result: tuple[int] | None = None
        self.rows: list[tuple[str]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.executions.append((sql, params))
        self.result = (
            (self.brand_ids[str(params[0])],) if "RETURNING brand_id" in sql else None
        )

    def fetchone(self) -> tuple[int] | None:
        return self.result

    def fetchall(self) -> list[tuple[str]]:
        return self.rows


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> FakeCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def test_feedback_store_resolves_each_product_brand_id() -> None:
    first_topics = parse_item_review_topics(
        {
            "asin": "B000000001",
            "itemName": "One",
            "marketplaceId": "ATVPDKIKX0DER",
            "dateRange": {
                "startDate": "2026-08-02T00:00:00Z",
                "endDate": "2026-08-08T23:59:59Z",
            },
            "topics": {
                "positiveTopics": [
                    {
                        "topic": "Comfort",
                        "asinMetrics": {"numberOfMentions": 2},
                    }
                ]
            },
        },
        brand_key="giagio",
        brand_name="GIAGIO",
    )
    second_topic = replace(
        first_topics[0],
        asin="B000000002",
        brand_key="second-brand",
        brand_name="Second Brand",
    )
    products = [
        CatalogProduct("US", "B000000001", "giagio", "GIAGIO", "One"),
        CatalogProduct("US", "B000000002", "second-brand", "Second Brand", "Two"),
    ]
    topics = [*first_topics, second_topic]
    summaries = aggregate_brand_feedback(products, topics)
    connection = FakeConnection()

    loaded = AmazonSourceStore(
        connection,
        account_key="giagio",
    ).write_customer_feedback(42, topics, summaries)

    assert loaded == 4
    topic_brand_ids = [
        params[1]
        for sql, params in connection.cursor_instance.executions
        if "customer_feedback_item_topic_weekly" in sql
    ]
    summary_brand_ids = [
        params[1]
        for sql, params in connection.cursor_instance.executions
        if "customer_feedback_brand_weekly" in sql
    ]
    assert topic_brand_ids == [1, 2]
    assert summary_brand_ids == [1, 2]
    assert connection.commits == 1


def test_feedback_store_reads_distinct_asins_from_latest_listing_snapshot() -> None:
    connection = FakeConnection()
    connection.cursor_instance.rows = [("B000000001",), ("B000000002",)]
    store = AmazonSourceStore(connection, account_key="giagio")

    asins = store.current_listing_asins("ATVPDKIKX0DER")

    assert asins == ["B000000001", "B000000002"]
    sql, params = connection.cursor_instance.executions[-1]
    assert "MAX(latest.snapshot_date)" in sql
    assert params == ("giagio", "ATVPDKIKX0DER", None, None)
