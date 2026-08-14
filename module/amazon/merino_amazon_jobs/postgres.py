from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import date
from typing import Any

from merino_amazon_jobs.marketplaces import MARKETPLACES, Marketplace
from merino_amazon_jobs.sales_traffic import Listing, SalesTrafficRow

BOOTSTRAP_BRAND_SQL = """
    INSERT INTO amazon.brand (
        brand_key,
        display_name,
        active
    )
    VALUES (%s, %s, true)
    ON CONFLICT (brand_key) DO UPDATE SET
        display_name = EXCLUDED.display_name,
        active = true,
        updated_at = now()
    RETURNING brand_id
"""

BOOTSTRAP_ACCOUNT_SQL = """
    INSERT INTO amazon.seller_account (
        brand_id,
        account_key,
        seller_id,
        display_name,
        active
    )
    VALUES (%s, %s, %s, %s, true)
    ON CONFLICT (account_key) DO UPDATE SET
        brand_id = EXCLUDED.brand_id,
        seller_id = EXCLUDED.seller_id,
        display_name = EXCLUDED.display_name,
        active = true,
        updated_at = now()
    RETURNING seller_account_id
"""

LINK_MARKETPLACE_SQL = """
    INSERT INTO amazon.seller_account_marketplace (
        seller_account_id,
        marketplace_id,
        active
    )
    VALUES (%s, %s, true)
    ON CONFLICT (seller_account_id, marketplace_id) DO UPDATE SET
        active = true,
        updated_at = now()
"""

START_RUN_SQL = """
    INSERT INTO amazon.ingestion_run (
        seller_account_id,
        marketplace_id,
        source_system,
        report_type,
        period_start,
        period_end,
        granularity,
        status
    )
    SELECT seller_account_id, %s, 'sp_api',
           'GET_SALES_AND_TRAFFIC_REPORT', %s, %s, %s, 'started'
    FROM amazon.seller_account
    WHERE account_key = %s
    RETURNING ingestion_run_id
"""

LISTINGS_SQL = """
    SELECT DISTINCT ON (seller_sku)
           seller_sku, asin, parent_asin, fulfillment_channel
    FROM amazon.listing_snapshot
    WHERE seller_account_id = (
        SELECT seller_account_id
        FROM amazon.seller_account
        WHERE account_key = %s
    )
      AND marketplace_id = %s
      AND snapshot_date <= %s
    ORDER BY seller_sku, snapshot_date DESC
"""

UPDATE_RUN_SQL = """
    UPDATE amazon.ingestion_run
    SET status = %s,
        request_id = COALESCE(%s, request_id),
        report_id = COALESCE(%s, report_id),
        report_document_id = COALESCE(%s, report_document_id)
    WHERE ingestion_run_id = %s
"""

FINISH_RUN_SQL = """
    UPDATE amazon.ingestion_run
    SET status = 'completed',
        report_id = COALESCE(%s, report_id),
        report_document_id = COALESCE(%s, report_document_id),
        source_row_count = %s,
        loaded_row_count = %s,
        completed_at = now()
    WHERE ingestion_run_id = %s
"""

FAIL_RUN_SQL = """
    UPDATE amazon.ingestion_run
    SET status = %s,
        error_message = %s,
        completed_at = now()
    WHERE ingestion_run_id = %s
"""


def sales_traffic_insert_sql(*, overwrite: bool) -> str:
    conflict_action = (
        """
        DO UPDATE SET
            ingestion_run_id = EXCLUDED.ingestion_run_id,
            seller_sku = EXCLUDED.seller_sku,
            asin = EXCLUDED.asin,
            parent_asin = EXCLUDED.parent_asin,
            sessions = EXCLUDED.sessions,
            browser_sessions = EXCLUDED.browser_sessions,
            mobile_app_sessions = EXCLUDED.mobile_app_sessions,
            page_views = EXCLUDED.page_views,
            browser_page_views = EXCLUDED.browser_page_views,
            mobile_app_page_views = EXCLUDED.mobile_app_page_views,
            units_ordered = EXCLUDED.units_ordered,
            total_order_items = EXCLUDED.total_order_items,
            ordered_product_sales_amount =
                EXCLUDED.ordered_product_sales_amount,
            currency_code = EXCLUDED.currency_code,
            unit_session_percentage = EXCLUDED.unit_session_percentage,
            buy_box_percentage = EXCLUDED.buy_box_percentage,
            fba_scope = EXCLUDED.fba_scope,
            fba_scope_method = EXCLUDED.fba_scope_method,
            raw_payload = EXCLUDED.raw_payload,
            updated_at = now()
        """
        if overwrite
        else "DO NOTHING"
    )
    return f"""
        INSERT INTO amazon.sales_traffic_daily (
            ingestion_run_id,
            seller_account_id,
            marketplace_id,
            report_date,
            granularity,
            dimension_value,
            seller_sku,
            asin,
            parent_asin,
            sessions,
            browser_sessions,
            mobile_app_sessions,
            page_views,
            browser_page_views,
            mobile_app_page_views,
            units_ordered,
            total_order_items,
            ordered_product_sales_amount,
            currency_code,
            unit_session_percentage,
            buy_box_percentage,
            fba_scope,
            fba_scope_method,
            raw_payload
        )
        SELECT %s, seller_account_id, %s, %s, %s, %s, %s, %s, %s,
               %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
               %s, %s, %s::jsonb
        FROM amazon.seller_account
        WHERE account_key = %s
        ON CONFLICT (
            seller_account_id,
            marketplace_id,
            report_date,
            granularity,
            dimension_value
        ) {conflict_action}
    """


class AmazonSalesTrafficStore:
    def __init__(self, connection: Any, *, account_key: str) -> None:
        self.connection = connection
        self.account_key = account_key

    def bootstrap_account(
        self,
        *,
        brand_key: str,
        brand_name: str,
        seller_id: str,
        display_name: str,
        marketplaces: Iterable[Marketplace],
    ) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                BOOTSTRAP_BRAND_SQL,
                (brand_key, brand_name),
            )
            brand = cursor.fetchone()
            if brand is None:
                raise RuntimeError(f"brand {brand_key!r} could not be configured")
            brand_id = int(brand[0])
            cursor.execute(
                BOOTSTRAP_ACCOUNT_SQL,
                (brand_id, self.account_key, seller_id, display_name),
            )
            account = cursor.fetchone()
            if account is None:
                raise RuntimeError(
                    f"seller account {self.account_key!r} could not be configured"
                )
            account_id = int(account[0])
            for marketplace in marketplaces:
                cursor.execute(
                    LINK_MARKETPLACE_SQL,
                    (account_id, marketplace.marketplace_id),
                )
        self.connection.commit()
        return account_id

    def start_run(
        self,
        marketplace: Marketplace,
        start_date: date,
        end_date: date,
        granularity: str,
    ) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                START_RUN_SQL,
                (
                    marketplace.marketplace_id,
                    start_date,
                    end_date,
                    granularity,
                    self.account_key,
                ),
            )
            result = cursor.fetchone()
            if result is None:
                raise RuntimeError(
                    f"seller account {self.account_key!r} is not configured"
                )
            run_id = int(result[0])
        self.connection.commit()
        return run_id

    def listings(
        self,
        marketplace: Marketplace,
        report_date: date,
    ) -> list[Listing]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                LISTINGS_SQL,
                (self.account_key, marketplace.marketplace_id, report_date),
            )
            return [Listing(*row) for row in cursor.fetchall()]

    def update_run(
        self,
        run_id: int,
        *,
        status: str,
        request_id: str | None = None,
        report_id: str | None = None,
        report_document_id: str | None = None,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                UPDATE_RUN_SQL,
                (
                    status,
                    request_id,
                    report_id,
                    report_document_id,
                    run_id,
                ),
            )
        self.connection.commit()

    def write_rows(
        self,
        run_id: int,
        rows: Iterable[SalesTrafficRow],
        *,
        overwrite: bool,
    ) -> int:
        rows = list(rows)
        if not rows:
            return 0
        statement = sales_traffic_insert_sql(overwrite=overwrite)
        loaded_row_count = 0
        with self.connection.cursor() as cursor:
            for row in rows:
                cursor.execute(
                    statement,
                    (
                        run_id,
                        MARKETPLACES[row.marketplace].marketplace_id,
                        row.report_date,
                        row.granularity,
                        row.dimension_value,
                        row.seller_sku,
                        row.asin,
                        row.parent_asin,
                        row.sessions,
                        row.browser_sessions,
                        row.mobile_app_sessions,
                        row.page_views,
                        row.browser_page_views,
                        row.mobile_app_page_views,
                        row.units_ordered,
                        row.total_order_items,
                        row.ordered_product_sales_amount,
                        row.currency_code,
                        row.unit_session_percentage,
                        row.buy_box_percentage,
                        row.fba_scope,
                        row.fba_scope_method,
                        json.dumps(row.raw, separators=(",", ":")),
                        self.account_key,
                    ),
                )
                loaded_row_count += (
                    cursor.rowcount if isinstance(cursor.rowcount, int) else 1
                )
        self.connection.commit()
        return loaded_row_count

    def finish_run(
        self,
        run_id: int,
        *,
        report_id: str,
        report_document_id: str,
        source_row_count: int,
        loaded_row_count: int,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                FINISH_RUN_SQL,
                (
                    report_id,
                    report_document_id,
                    source_row_count,
                    loaded_row_count,
                    run_id,
                ),
            )
        self.connection.commit()

    def fail_run(self, run_id: int, error: Exception) -> None:
        self.connection.rollback()
        status = (
            "cancelled" if getattr(error, "status", None) == "CANCELLED" else "failed"
        )
        with self.connection.cursor() as cursor:
            cursor.execute(
                FAIL_RUN_SQL,
                (status, str(error)[:4000], run_id),
            )
        self.connection.commit()
