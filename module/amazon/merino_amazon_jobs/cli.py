from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Sequence
from datetime import date, datetime, timedelta, timezone

from merino_amazon_jobs.client import reports_api
from merino_amazon_jobs.marketplaces import MARKETPLACES, date_windows
from merino_amazon_jobs.postgres import AmazonSalesTrafficStore
from merino_amazon_jobs.quota import sp_api_job_lock
from merino_amazon_jobs.reports import SalesTrafficReports
from merino_amazon_jobs.runtime import load_sales_traffic

logger = logging.getLogger(__name__)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Backfill strict-FBA Amazon Sales & Traffic facts.",
    )
    marketplace_group = result.add_mutually_exclusive_group()
    marketplace_group.add_argument(
        "--marketplace",
        action="append",
        choices=tuple(MARKETPLACES),
    )
    marketplace_group.add_argument("--all-marketplaces", action="store_true")
    result.add_argument("--start-date", type=date.fromisoformat)
    result.add_argument("--end-date", type=date.fromisoformat)
    result.add_argument(
        "--granularity",
        action="append",
        choices=("PARENT", "CHILD", "SKU"),
    )
    result.add_argument("--90-day", dest="ninety_day", action="store_true")
    result.add_argument("--dry-run", action="store_true")
    result.add_argument("--overwrite", action="store_true")
    result.add_argument(
        "--database-url",
        default=os.getenv("AMAZON_DATABASE_URL") or os.getenv("DATABASE_URL"),
    )
    result.add_argument(
        "--account-key",
        default=os.getenv("AMAZON_ACCOUNT_KEY") or os.getenv("amazon_account_key"),
    )
    result.add_argument(
        "--brand-key",
        default=os.getenv("AMAZON_BRAND_KEY") or os.getenv("amazon_brand_key"),
    )
    result.add_argument(
        "--brand-name",
        default=os.getenv("AMAZON_BRAND_NAME") or os.getenv("amazon_brand_name"),
    )
    result.add_argument(
        "--seller-id",
        default=(
            os.getenv("AMAZON_SELLER_ID")
            or os.getenv("SP_API_SELLER_ID")
            or os.getenv("sp_api_seller_id")
        ),
    )
    result.add_argument(
        "--seller-display-name",
        default=(
            os.getenv("AMAZON_SELLER_DISPLAY_NAME")
            or os.getenv("amazon_seller_display_name")
        ),
    )
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    start_date, end_date = _date_range(args)
    marketplaces = (
        list(MARKETPLACES.values())
        if args.all_marketplaces
        else [MARKETPLACES[code] for code in (args.marketplace or ["US"])]
    )
    granularities = args.granularity or ["SKU"]

    if args.dry_run:
        for marketplace in marketplaces:
            for granularity in granularities:
                for window_start, window_end in date_windows(start_date, end_date):
                    print(
                        f"{marketplace.code} {granularity} "
                        f"{window_start.isoformat()}..{window_end.isoformat()}"
                    )
        return 0

    if not args.database_url:
        raise RuntimeError("AMAZON_DATABASE_URL or DATABASE_URL is required")
    if not args.seller_id:
        raise RuntimeError(
            "AMAZON_SELLER_ID is required "
            "(GSM airflow-variables-amazon_seller_id, --seller-id, or env). "
            "Sellers.getAccount does not return sellerId — use Seller Central "
            "Merchant Token / Selling Partner ID."
        )
    if not args.seller_display_name:
        raise RuntimeError(
            "--seller-display-name or AMAZON_SELLER_DISPLAY_NAME is required"
        )
    if not args.brand_key:
        raise RuntimeError("--brand-key or AMAZON_BRAND_KEY is required")
    if not args.brand_name:
        raise RuntimeError("--brand-name or AMAZON_BRAND_NAME is required")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    seller_id = args.seller_id
    account_key = args.account_key or seller_id

    import psycopg

    total = 0
    with sp_api_job_lock(owner="sales_traffic"):
        with psycopg.connect(args.database_url) as connection:
            store = AmazonSalesTrafficStore(connection, account_key=account_key)
            store.bootstrap_account(
                brand_key=args.brand_key,
                brand_name=args.brand_name,
                seller_id=seller_id,
                display_name=args.seller_display_name,
                marketplaces=marketplaces,
            )
            reports_by_region: dict[str, SalesTrafficReports] = {}
            for marketplace in marketplaces:
                reports = reports_by_region.get(marketplace.region)
                if reports is None:
                    reports = SalesTrafficReports(reports_api(marketplace.region))
                    reports_by_region[marketplace.region] = reports
                for granularity in granularities:
                    total += load_sales_traffic(
                        reports,
                        store,
                        marketplace,
                        start_date,
                        end_date,
                        granularity,
                        overwrite=args.overwrite,
                    )
    logger.info("Amazon Sales & Traffic backfill completed rows=%s", total)
    return 0


def _date_range(args: argparse.Namespace) -> tuple[date, date]:
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    if args.ninety_day:
        if args.start_date:
            raise ValueError("--90-day cannot be combined with --start-date")
        end_date = args.end_date or yesterday
        return end_date - timedelta(days=89), end_date
    start_date = args.start_date or yesterday
    end_date = args.end_date or start_date
    if start_date > end_date:
        raise ValueError("start date must be on or before end date")
    return start_date, end_date
