from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import astuple
from typing import Any

from merino_amazon_jobs.ads import AdPerformance
from merino_amazon_jobs.brand_analytics import SearchCatalogPerformance
from merino_amazon_jobs.customer_feedback import (
    BrandFeedbackWeekly,
    ItemReviewTopic,
)
from merino_amazon_jobs.inventory import (
    FbaInventoryAgeSnapshot,
    FbaInventorySnapshot,
)
from merino_amazon_jobs.listings import ListingSnapshot
from merino_amazon_jobs.marketplaces import MARKETPLACES
from merino_amazon_jobs.orders import AmazonOrder, AmazonOrderItem

START_SOURCE_RUN_SQL = """
INSERT INTO amazon.ingestion_run (
    seller_account_id, marketplace_id, source_system, report_type,
    period_start, period_end, granularity, status
)
SELECT seller_account_id, %s, %s, %s, %s, %s, %s, 'started'
FROM amazon.seller_account WHERE account_key = %s
RETURNING ingestion_run_id
"""

FINISH_SOURCE_RUN_SQL = """
UPDATE amazon.ingestion_run
SET status = 'completed', report_id = COALESCE(%s, report_id),
    report_document_id = COALESCE(%s, report_document_id),
    source_row_count = %s, loaded_row_count = %s, completed_at = now()
WHERE ingestion_run_id = %s
"""

FAIL_SOURCE_RUN_SQL = """
UPDATE amazon.ingestion_run
SET status = 'failed', error_message = %s, completed_at = now()
WHERE ingestion_run_id = %s
"""

CURRENT_LISTING_ASINS_SQL = """
SELECT DISTINCT listing.asin
FROM amazon.listing_snapshot listing
JOIN amazon.seller_account account
  ON account.seller_account_id = listing.seller_account_id
WHERE account.account_key = %s
  AND listing.marketplace_id = %s
  AND listing.snapshot_date = (
      SELECT MAX(latest.snapshot_date)
      FROM amazon.listing_snapshot latest
      WHERE latest.seller_account_id = listing.seller_account_id
        AND latest.marketplace_id = listing.marketplace_id
  )
  AND listing.asin IS NOT NULL
  AND (%s IS NULL OR listing.asin = %s)
ORDER BY listing.asin
"""

LISTING_SQL = """
INSERT INTO amazon.listing_snapshot (
    ingestion_run_id, seller_account_id, marketplace_id, snapshot_date,
    seller_sku, asin, parent_asin, fnsku, item_name, listing_status,
    fulfillment_channel, quantity, price_amount, currency_code, open_date,
    raw_payload
)
SELECT %s, seller_account_id, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
       %s, %s, %s, %s::jsonb
FROM amazon.seller_account WHERE account_key = %s
ON CONFLICT (seller_account_id, marketplace_id, snapshot_date, seller_sku)
DO UPDATE SET
    ingestion_run_id = EXCLUDED.ingestion_run_id, asin = EXCLUDED.asin,
    parent_asin = EXCLUDED.parent_asin, fnsku = EXCLUDED.fnsku,
    item_name = EXCLUDED.item_name, listing_status = EXCLUDED.listing_status,
    fulfillment_channel = EXCLUDED.fulfillment_channel,
    quantity = EXCLUDED.quantity, price_amount = EXCLUDED.price_amount,
    currency_code = EXCLUDED.currency_code, open_date = EXCLUDED.open_date,
    raw_payload = EXCLUDED.raw_payload, updated_at = now()
"""

FBA_INVENTORY_SQL = """
INSERT INTO amazon.fba_inventory_snapshot (
    ingestion_run_id, seller_account_id, marketplace_id, snapshot_date,
    seller_sku, fnsku, asin, condition, fulfillment_channel,
    fulfillable_quantity, inbound_working_quantity, inbound_shipped_quantity,
    inbound_receiving_quantity, reserved_customer_order_quantity,
    reserved_fc_transfer_quantity, reserved_fc_processing_quantity,
    unfulfillable_quantity, researching_quantity, total_quantity,
    snapshot_method, raw_payload
)
SELECT %s, seller_account_id, %s, %s, %s, %s, %s, %s, 'AMAZON', %s, %s,
       %s, %s, %s, %s, %s, %s, %s, %s, 'observed', %s::jsonb
FROM amazon.seller_account WHERE account_key = %s
ON CONFLICT (
    seller_account_id, marketplace_id, snapshot_date, seller_sku, fnsku
) DO UPDATE SET
    ingestion_run_id = EXCLUDED.ingestion_run_id, asin = EXCLUDED.asin,
    condition = EXCLUDED.condition,
    fulfillable_quantity = EXCLUDED.fulfillable_quantity,
    inbound_working_quantity = EXCLUDED.inbound_working_quantity,
    inbound_shipped_quantity = EXCLUDED.inbound_shipped_quantity,
    inbound_receiving_quantity = EXCLUDED.inbound_receiving_quantity,
    reserved_customer_order_quantity = EXCLUDED.reserved_customer_order_quantity,
    reserved_fc_transfer_quantity = EXCLUDED.reserved_fc_transfer_quantity,
    reserved_fc_processing_quantity = EXCLUDED.reserved_fc_processing_quantity,
    unfulfillable_quantity = EXCLUDED.unfulfillable_quantity,
    researching_quantity = EXCLUDED.researching_quantity,
    total_quantity = EXCLUDED.total_quantity, raw_payload = EXCLUDED.raw_payload,
    updated_at = now()
"""

FBA_INVENTORY_AGE_SQL = """
INSERT INTO amazon.fba_inventory_age_snapshot (
    ingestion_run_id, seller_account_id, marketplace_id, snapshot_date,
    seller_sku, fnsku, asin, fulfillment_channel, age_0_30_days,
    age_31_60_days, age_61_90_days, age_91_180_days, age_181_330_days,
    age_331_365_days, age_365_plus_days, snapshot_method, raw_payload
)
SELECT %s, seller_account_id, %s, %s, %s, %s, %s, 'AMAZON', %s, %s, %s,
       %s, %s, %s, %s, %s, %s::jsonb
FROM amazon.seller_account WHERE account_key = %s
ON CONFLICT (
    seller_account_id, marketplace_id, snapshot_date, seller_sku, fnsku
) DO UPDATE SET
    ingestion_run_id = EXCLUDED.ingestion_run_id, asin = EXCLUDED.asin,
    age_0_30_days = EXCLUDED.age_0_30_days,
    age_31_60_days = EXCLUDED.age_31_60_days,
    age_61_90_days = EXCLUDED.age_61_90_days,
    age_91_180_days = EXCLUDED.age_91_180_days,
    age_181_330_days = EXCLUDED.age_181_330_days,
    age_331_365_days = EXCLUDED.age_331_365_days,
    age_365_plus_days = EXCLUDED.age_365_plus_days,
    snapshot_method = EXCLUDED.snapshot_method,
    raw_payload = EXCLUDED.raw_payload, updated_at = now()
"""

ORDER_SQL = """
INSERT INTO amazon.orders (
    ingestion_run_id, seller_account_id, marketplace_id, amazon_order_id,
    purchase_date, last_update_date, order_status, fulfillment_channel,
    fulfilled_by, sales_channel, ship_service_level, number_of_items_shipped,
    number_of_items_unshipped, order_total_amount, currency_code,
    is_business_order, is_prime, raw_payload
)
SELECT %s, seller_account_id, %s, %s, %s, %s, %s, %s, 'AMAZON', %s, %s,
       %s, %s, %s, %s, %s, %s, %s::jsonb
FROM amazon.seller_account WHERE account_key = %s
ON CONFLICT (seller_account_id, marketplace_id, amazon_order_id)
DO UPDATE SET
    ingestion_run_id = EXCLUDED.ingestion_run_id,
    purchase_date = EXCLUDED.purchase_date,
    last_update_date = EXCLUDED.last_update_date,
    order_status = EXCLUDED.order_status,
    fulfillment_channel = EXCLUDED.fulfillment_channel,
    sales_channel = EXCLUDED.sales_channel,
    ship_service_level = EXCLUDED.ship_service_level,
    number_of_items_shipped = EXCLUDED.number_of_items_shipped,
    number_of_items_unshipped = EXCLUDED.number_of_items_unshipped,
    order_total_amount = EXCLUDED.order_total_amount,
    currency_code = EXCLUDED.currency_code,
    is_business_order = EXCLUDED.is_business_order,
    is_prime = EXCLUDED.is_prime, raw_payload = EXCLUDED.raw_payload,
    updated_at = now()
RETURNING order_id
"""

ORDER_ITEM_SQL = """
INSERT INTO amazon.order_items (
    order_id, ingestion_run_id, seller_account_id, marketplace_id,
    amazon_order_id, order_item_id, asin, seller_sku, title,
    quantity_ordered, quantity_shipped, item_price_amount, item_tax_amount,
    shipping_price_amount, shipping_tax_amount, promotion_discount_amount,
    currency_code, raw_payload
)
SELECT %s, %s, seller_account_id, %s, %s, %s, %s, %s, %s, %s, %s, %s,
       %s, %s, %s, %s, %s, %s::jsonb
FROM amazon.seller_account WHERE account_key = %s
ON CONFLICT (
    seller_account_id, marketplace_id, amazon_order_id, order_item_id
) DO UPDATE SET
    order_id = EXCLUDED.order_id, ingestion_run_id = EXCLUDED.ingestion_run_id,
    asin = EXCLUDED.asin, seller_sku = EXCLUDED.seller_sku,
    title = EXCLUDED.title, quantity_ordered = EXCLUDED.quantity_ordered,
    quantity_shipped = EXCLUDED.quantity_shipped,
    item_price_amount = EXCLUDED.item_price_amount,
    item_tax_amount = EXCLUDED.item_tax_amount,
    shipping_price_amount = EXCLUDED.shipping_price_amount,
    shipping_tax_amount = EXCLUDED.shipping_tax_amount,
    promotion_discount_amount = EXCLUDED.promotion_discount_amount,
    currency_code = EXCLUDED.currency_code, raw_payload = EXCLUDED.raw_payload,
    updated_at = now()
"""

BRAND_ANALYTICS_SQL = """
INSERT INTO amazon.search_catalog_performance_period (
    ingestion_run_id, seller_account_id, marketplace_id, period_start,
    period_end, period_granularity, asin, product_title, impressions, clicks,
    cart_adds, purchases, click_rate, cart_add_rate, purchase_rate,
    shipped_units, shipped_revenue_amount, currency_code, raw_payload
)
SELECT %s, seller_account_id, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
       %s, %s, %s, %s, %s, %s, %s::jsonb
FROM amazon.seller_account WHERE account_key = %s
ON CONFLICT (
    seller_account_id, marketplace_id, period_start, period_end, asin
) DO UPDATE SET
    ingestion_run_id = EXCLUDED.ingestion_run_id,
    period_granularity = EXCLUDED.period_granularity,
    product_title = EXCLUDED.product_title, impressions = EXCLUDED.impressions,
    clicks = EXCLUDED.clicks, cart_adds = EXCLUDED.cart_adds,
    purchases = EXCLUDED.purchases, click_rate = EXCLUDED.click_rate,
    cart_add_rate = EXCLUDED.cart_add_rate,
    purchase_rate = EXCLUDED.purchase_rate,
    shipped_units = EXCLUDED.shipped_units,
    shipped_revenue_amount = EXCLUDED.shipped_revenue_amount,
    currency_code = EXCLUDED.currency_code, raw_payload = EXCLUDED.raw_payload,
    updated_at = now()
"""

AD_PERFORMANCE_SQL = """
INSERT INTO amazon.ad_performance_daily (
    ingestion_run_id, seller_account_id, marketplace_id, report_date,
    profile_id, ad_product, campaign_id, campaign_name, ad_group_id,
    ad_group_name, ad_id, target_id, target_expression, advertised_sku,
    advertised_asin, impressions, clicks, cost_amount, purchases, units_sold,
    attributed_sales_amount, currency_code, attribution_window_days, raw_payload
)
SELECT %s, seller_account_id, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
       %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
FROM amazon.seller_account WHERE account_key = %s
ON CONFLICT (
    seller_account_id, marketplace_id, report_date, profile_id, ad_product,
    campaign_id, ad_group_id, ad_id, target_id, advertised_sku,
    advertised_asin, attribution_window_days
) DO UPDATE SET
    ingestion_run_id = EXCLUDED.ingestion_run_id,
    campaign_name = EXCLUDED.campaign_name,
    ad_group_name = EXCLUDED.ad_group_name,
    target_expression = EXCLUDED.target_expression,
    impressions = EXCLUDED.impressions, clicks = EXCLUDED.clicks,
    cost_amount = EXCLUDED.cost_amount, purchases = EXCLUDED.purchases,
    units_sold = EXCLUDED.units_sold,
    attributed_sales_amount = EXCLUDED.attributed_sales_amount,
    currency_code = EXCLUDED.currency_code,
    raw_payload = EXCLUDED.raw_payload, updated_at = now()
"""

UPSERT_PRODUCT_BRAND_SQL = """
INSERT INTO amazon.brand (brand_key, display_name)
VALUES (%s, %s)
ON CONFLICT (brand_key) DO UPDATE SET
    active = true,
    updated_at = now()
RETURNING brand_id
"""

CUSTOMER_FEEDBACK_ITEM_TOPIC_SQL = """
INSERT INTO amazon.customer_feedback_item_topic_weekly (
    ingestion_run_id, seller_account_id, brand_id, marketplace_id,
    period_start, period_end, asin, item_name, sentiment, topic_rank, topic,
    number_of_mentions, occurrence_percentage, star_rating_impact,
    parent_asin_metrics, browse_node_metrics, child_asin_metrics,
    review_snippets, subtopics, raw_payload
)
SELECT %s, seller_account_id, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
       %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb
FROM amazon.seller_account WHERE account_key = %s
ON CONFLICT (
    seller_account_id, marketplace_id, period_start, period_end, asin,
    brand_id, sentiment, topic
) DO UPDATE SET
    ingestion_run_id = EXCLUDED.ingestion_run_id,
    item_name = EXCLUDED.item_name,
    topic_rank = EXCLUDED.topic_rank,
    number_of_mentions = EXCLUDED.number_of_mentions,
    occurrence_percentage = EXCLUDED.occurrence_percentage,
    star_rating_impact = EXCLUDED.star_rating_impact,
    parent_asin_metrics = EXCLUDED.parent_asin_metrics,
    browse_node_metrics = EXCLUDED.browse_node_metrics,
    child_asin_metrics = EXCLUDED.child_asin_metrics,
    review_snippets = EXCLUDED.review_snippets,
    subtopics = EXCLUDED.subtopics,
    raw_payload = EXCLUDED.raw_payload,
    updated_at = now()
"""

CUSTOMER_FEEDBACK_BRAND_SQL = """
INSERT INTO amazon.customer_feedback_brand_weekly (
    ingestion_run_id, seller_account_id, brand_id, marketplace_id,
    period_start, period_end, catalog_asin_count, feedback_asin_count,
    feedback_coverage_percentage, positive_topic_count, negative_topic_count,
    positive_mention_count, negative_mention_count,
    average_positive_occurrence_percentage,
    average_negative_occurrence_percentage,
    average_positive_star_rating_impact,
    average_negative_star_rating_impact, raw_payload
)
SELECT %s, seller_account_id, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
       %s, %s, %s, %s, %s::jsonb
FROM amazon.seller_account WHERE account_key = %s
ON CONFLICT (
    seller_account_id, marketplace_id, brand_id, period_start, period_end
) DO UPDATE SET
    ingestion_run_id = EXCLUDED.ingestion_run_id,
    catalog_asin_count = EXCLUDED.catalog_asin_count,
    feedback_asin_count = EXCLUDED.feedback_asin_count,
    feedback_coverage_percentage = EXCLUDED.feedback_coverage_percentage,
    positive_topic_count = EXCLUDED.positive_topic_count,
    negative_topic_count = EXCLUDED.negative_topic_count,
    positive_mention_count = EXCLUDED.positive_mention_count,
    negative_mention_count = EXCLUDED.negative_mention_count,
    average_positive_occurrence_percentage =
        EXCLUDED.average_positive_occurrence_percentage,
    average_negative_occurrence_percentage =
        EXCLUDED.average_negative_occurrence_percentage,
    average_positive_star_rating_impact =
        EXCLUDED.average_positive_star_rating_impact,
    average_negative_star_rating_impact =
        EXCLUDED.average_negative_star_rating_impact,
    raw_payload = EXCLUDED.raw_payload,
    updated_at = now()
"""


class AmazonSourceStore:
    def __init__(self, connection: Any, *, account_key: str) -> None:
        self.connection = connection
        self.account_key = account_key

    def start_run(
        self,
        *,
        marketplace_id: str,
        source_system: str,
        report_type: str,
        period_start: Any,
        period_end: Any,
        granularity: str | None = None,
    ) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                START_SOURCE_RUN_SQL,
                (
                    marketplace_id,
                    source_system,
                    report_type,
                    period_start,
                    period_end,
                    granularity,
                    self.account_key,
                ),
            )
            result = cursor.fetchone()
        if result is None:
            raise RuntimeError(f"seller account {self.account_key!r} is not configured")
        self.connection.commit()
        return int(result[0])

    def finish_run(
        self,
        run_id: int,
        *,
        source_rows: int,
        loaded_rows: int,
        report_id: str | None = None,
        report_document_id: str | None = None,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                FINISH_SOURCE_RUN_SQL,
                (
                    report_id,
                    report_document_id,
                    source_rows,
                    loaded_rows,
                    run_id,
                ),
            )
        self.connection.commit()

    def fail_run(self, run_id: int, error: Exception) -> None:
        self.connection.rollback()
        with self.connection.cursor() as cursor:
            cursor.execute(FAIL_SOURCE_RUN_SQL, (str(error)[:4000], run_id))
        self.connection.commit()

    def current_listing_asins(
        self,
        marketplace_id: str,
        *,
        asin: str | None = None,
    ) -> list[str]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                CURRENT_LISTING_ASINS_SQL,
                (self.account_key, marketplace_id, asin, asin),
            )
            return [str(row[0]) for row in cursor.fetchall()]

    def write_listings(self, run_id: int, rows: Iterable[ListingSnapshot]) -> int:
        return self._write(LISTING_SQL, run_id, rows)

    def write_inventory(self, run_id: int, rows: Iterable[FbaInventorySnapshot]) -> int:
        return self._write(FBA_INVENTORY_SQL, run_id, rows)

    def write_inventory_age(
        self, run_id: int, rows: Iterable[FbaInventoryAgeSnapshot]
    ) -> int:
        return self._write(FBA_INVENTORY_AGE_SQL, run_id, rows)

    def write_brand_analytics(
        self, run_id: int, rows: Iterable[SearchCatalogPerformance]
    ) -> int:
        return self._write(BRAND_ANALYTICS_SQL, run_id, rows)

    def write_ads(self, run_id: int, rows: Iterable[AdPerformance]) -> int:
        return self._write(AD_PERFORMANCE_SQL, run_id, rows)

    def write_customer_feedback(
        self,
        run_id: int,
        topics: Iterable[ItemReviewTopic],
        summaries: Iterable[BrandFeedbackWeekly],
    ) -> int:
        topic_rows = list(topics)
        summary_rows = list(summaries)
        brand_names = {
            row.brand_key: row.brand_name for row in [*topic_rows, *summary_rows]
        }
        count = 0
        try:
            with self.connection.cursor() as cursor:
                brand_ids = {}
                for brand_key, brand_name in brand_names.items():
                    cursor.execute(
                        UPSERT_PRODUCT_BRAND_SQL,
                        (brand_key, brand_name),
                    )
                    result = cursor.fetchone()
                    if result is None:
                        raise RuntimeError(
                            f"brand {brand_key!r} could not be configured"
                        )
                    brand_ids[brand_key] = int(result[0])

                for row in topic_rows:
                    cursor.execute(
                        CUSTOMER_FEEDBACK_ITEM_TOPIC_SQL,
                        (
                            run_id,
                            brand_ids[row.brand_key],
                            self._marketplace_id(row.marketplace),
                            row.period_start,
                            row.period_end,
                            row.asin,
                            row.item_name,
                            row.sentiment,
                            row.topic_rank,
                            row.topic,
                            row.number_of_mentions,
                            row.occurrence_percentage,
                            row.star_rating_impact,
                            self._json(row.parent_asin_metrics),
                            self._json(row.browse_node_metrics),
                            self._json(row.child_asin_metrics),
                            self._json(row.review_snippets),
                            self._json(row.subtopics),
                            self._json(row.raw),
                            self.account_key,
                        ),
                    )
                    count += 1

                for row in summary_rows:
                    cursor.execute(
                        CUSTOMER_FEEDBACK_BRAND_SQL,
                        (
                            run_id,
                            brand_ids[row.brand_key],
                            self._marketplace_id(row.marketplace),
                            row.period_start,
                            row.period_end,
                            row.catalog_asin_count,
                            row.feedback_asin_count,
                            row.feedback_coverage_percentage,
                            row.positive_topic_count,
                            row.negative_topic_count,
                            row.positive_mention_count,
                            row.negative_mention_count,
                            row.average_positive_occurrence_percentage,
                            row.average_negative_occurrence_percentage,
                            row.average_positive_star_rating_impact,
                            row.average_negative_star_rating_impact,
                            self._json(row.raw),
                            self.account_key,
                        ),
                    )
                    count += 1
        except Exception:
            self.connection.rollback()
            raise
        self.connection.commit()
        return count

    def write_orders(
        self,
        run_id: int,
        orders: Iterable[AmazonOrder],
        items: Iterable[AmazonOrderItem],
    ) -> int:
        item_by_order: dict[tuple[str, str], list[AmazonOrderItem]] = {}
        for item in items:
            item_by_order.setdefault(
                (item.marketplace_id, item.amazon_order_id), []
            ).append(item)
        count = 0
        try:
            with self.connection.cursor() as cursor:
                for order in orders:
                    cursor.execute(
                        ORDER_SQL,
                        (run_id, *self._values(order), self.account_key),
                    )
                    result = cursor.fetchone()
                    if result is None:
                        raise RuntimeError(
                            f"order upsert returned no id for {order.amazon_order_id}"
                        )
                    order_id = result[0]
                    count += 1
                    for item in item_by_order.get(
                        (order.marketplace_id, order.amazon_order_id), []
                    ):
                        cursor.execute(
                            ORDER_ITEM_SQL,
                            (
                                order_id,
                                run_id,
                                *self._values(item),
                                self.account_key,
                            ),
                        )
                        count += 1
        except Exception:
            self.connection.rollback()
            raise
        self.connection.commit()
        return count

    def _write(self, sql: str, run_id: int, rows: Iterable[Any]) -> int:
        count = 0
        with self.connection.cursor() as cursor:
            for row in rows:
                cursor.execute(
                    sql,
                    (run_id, *self._values(row), self.account_key),
                )
                count += 1
        self.connection.commit()
        return count

    @staticmethod
    def _values(row: Any) -> tuple[Any, ...]:
        values = list(astuple(row))
        if values[0] in MARKETPLACES:
            values[0] = MARKETPLACES[values[0]].marketplace_id
        values[-1] = json.dumps(values[-1], separators=(",", ":"), default=str)
        return tuple(values)

    @staticmethod
    def _marketplace_id(marketplace: str) -> str:
        return (
            MARKETPLACES[marketplace].marketplace_id
            if marketplace in MARKETPLACES
            else marketplace
        )

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, separators=(",", ":"), default=str)
