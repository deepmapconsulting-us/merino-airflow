from __future__ import annotations

import argparse
import json
import os
from collections.abc import Callable, Sequence
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import requests
from spapi.rest import ApiException

from merino_amazon_jobs.ads import (
    ADS_ENDPOINTS,
    AdsReports,
    ads_access_token,
    parse_ad_report,
)
from merino_amazon_jobs.brand_analytics import (
    REPORT_TYPE as BRAND_REPORT_TYPE,
)
from merino_amazon_jobs.brand_analytics import (
    parse_search_catalog_performance,
    raise_if_unauthorized,
)
from merino_amazon_jobs.client import (
    fba_inventory_api,
    reports_api,
    search_orders_api,
)
from merino_amazon_jobs.inventory import (
    FbaInventorySummaries,
    parse_inventory_age_tsv,
    parse_inventory_summaries,
)
from merino_amazon_jobs.listings import REPORT_TYPE as LISTING_REPORT_TYPE
from merino_amazon_jobs.listings import parse_listing_tsv
from merino_amazon_jobs.marketplaces import MARKETPLACES
from merino_amazon_jobs.orders import Orders, parse_orders
from merino_amazon_jobs.postgres import AmazonSalesTrafficStore
from merino_amazon_jobs.quota import sp_api_job_lock
from merino_amazon_jobs.reports import SpApiReports
from merino_amazon_jobs.source_postgres import AmazonSourceStore

INVENTORY_AGE_REPORT_TYPE = "GET_FBA_INVENTORY_PLANNING_DATA"


def listings_main(argv: Sequence[str] | None = None) -> int:
    with sp_api_job_lock(owner="listings"):
        return _sp_report_main(
            argv,
            report_type=LISTING_REPORT_TYPE,
            parser=parse_listing_tsv,
            writer="write_listings",
            description="Load daily Amazon listing snapshots.",
        )


def inventory_age_main(argv: Sequence[str] | None = None) -> int:
    with sp_api_job_lock(owner="inventory_age"):
        return _sp_report_main(
            argv,
            report_type=INVENTORY_AGE_REPORT_TYPE,
            parser=parse_inventory_age_tsv,
            writer="write_inventory_age",
            description="Load observed Amazon FBA inventory age snapshots.",
        )


def inventory_main(argv: Sequence[str] | None = None) -> int:
    with sp_api_job_lock(owner="fba_inventory"):
        return _inventory_main(argv)


def _inventory_main(argv: Sequence[str] | None = None) -> int:
    args = _common_parser("Load exact Amazon FBA inventory summaries.").parse_args(argv)
    marketplace, store = _store(args)
    snapshot_date = args.snapshot_date or _yesterday()
    run_id = store.start_run(
        marketplace_id=marketplace.marketplace_id,
        source_system="sp_api",
        report_type="FbaInventoryApi.get_inventory_summaries",
        period_start=snapshot_date,
        period_end=snapshot_date,
    )
    try:
        payload = FbaInventorySummaries(
            fba_inventory_api(marketplace.region, marketplace.credential_group)
        ).all(marketplace.marketplace_id)
        rows = parse_inventory_summaries(
            payload,
            marketplace=marketplace.code,
            snapshot_date=snapshot_date,
        )
        loaded = store.write_inventory(run_id, rows)
        store.finish_run(run_id, source_rows=len(payload), loaded_rows=loaded)
    except Exception as error:
        store.fail_run(run_id, error)
        raise
    return 0


def orders_main(argv: Sequence[str] | None = None) -> int:
    with sp_api_job_lock(owner="orders"):
        return _orders_main(argv)


def _orders_main(argv: Sequence[str] | None = None) -> int:
    parser = _common_parser("Load FBA orders through Orders API 2026-01-01.")
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    args = parser.parse_args(argv)
    if args.start_date > args.end_date:
        raise ValueError("start date must be on or before end date")
    marketplace, store = _store(args)
    zone = ZoneInfo(marketplace.timezone)
    run_id = store.start_run(
        marketplace_id=marketplace.marketplace_id,
        source_system="sp_api",
        report_type="SearchOrdersApi.search_orders",
        period_start=args.start_date,
        period_end=args.end_date,
    )
    try:
        payload = Orders(
            search_orders_api(marketplace.region, marketplace.credential_group)
        ).all(
            marketplace_id=marketplace.marketplace_id,
            created_after=datetime.combine(args.start_date, time.min, zone),
            created_before=datetime.combine(args.end_date, time.max, zone),
        )
        orders, items = parse_orders(
            payload,
            marketplace_id=marketplace.marketplace_id,
        )
        loaded = store.write_orders(run_id, orders, items)
        store.finish_run(run_id, source_rows=len(payload), loaded_rows=loaded)
    except Exception as error:
        store.fail_run(run_id, error)
        raise
    return 0


def brand_analytics_main(argv: Sequence[str] | None = None) -> int:
    with sp_api_job_lock(owner="brand_analytics"):
        return _brand_analytics_main(argv)


def _brand_analytics_main(argv: Sequence[str] | None = None) -> int:
    parser = _common_parser("Load optional Brand Analytics catalog performance.")
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--period", choices=("WEEK", "MONTH", "QUARTER"), required=True)
    args = parser.parse_args(argv)
    marketplace, store = _store(args)
    run_id = store.start_run(
        marketplace_id=marketplace.marketplace_id,
        source_system="sp_api",
        report_type=BRAND_REPORT_TYPE,
        period_start=args.start_date,
        period_end=args.end_date,
        granularity=args.period,
    )
    try:
        try:
            document = SpApiReports(
                reports_api(marketplace.region, marketplace.credential_group)
            ).download(
                report_type=BRAND_REPORT_TYPE,
                marketplace_id=marketplace.marketplace_id,
                start_time=datetime.combine(
                    args.start_date, time.min, ZoneInfo(marketplace.timezone)
                ),
                end_time=datetime.combine(
                    args.end_date, time.max, ZoneInfo(marketplace.timezone)
                ),
                report_options={"reportPeriod": args.period},
            )
        except (ApiException, requests.HTTPError) as error:
            raise_if_unauthorized(error)
        payload = json.loads(document.content.decode("utf-8-sig"))
        rows = parse_search_catalog_performance(
            payload,
            marketplace=marketplace.code,
            period_start=args.start_date,
            period_end=args.end_date,
            granularity=args.period,
        )
        loaded = store.write_brand_analytics(run_id, rows)
        store.finish_run(
            run_id,
            source_rows=len(rows),
            loaded_rows=loaded,
            report_id=document.report_id,
            report_document_id=document.report_document_id,
        )
    except Exception as error:
        store.fail_run(run_id, error)
        raise
    return 0


def ads_main(argv: Sequence[str] | None = None) -> int:
    parser = _common_parser("Load Amazon Ads API v3 daily performance.")
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--profile-id", default=os.getenv("AMAZON_ADS_PROFILE_ID"))
    parser.add_argument(
        "--ad-product",
        action="append",
        choices=("SPONSORED_PRODUCTS", "SPONSORED_BRANDS", "SPONSORED_DISPLAY"),
    )
    parser.add_argument("--attribution-window-days", type=int, default=14)
    args = parser.parse_args(argv)
    marketplace, store = _store(args)
    profile_id = _required(args.profile_id, "AMAZON_ADS_PROFILE_ID")
    client_id = _required(os.getenv("AMAZON_ADS_CLIENT_ID"), "AMAZON_ADS_CLIENT_ID")
    secret = _required(
        os.getenv("AMAZON_ADS_CLIENT_SECRET"), "AMAZON_ADS_CLIENT_SECRET"
    )
    refresh = _required(
        os.getenv("AMAZON_ADS_REFRESH_TOKEN"), "AMAZON_ADS_REFRESH_TOKEN"
    )
    session = requests.Session()
    token = ads_access_token(
        session,
        client_id=client_id,
        client_secret=secret,
        refresh_token=refresh,
    )
    reports = AdsReports(
        session,
        access_token=token,
        client_id=client_id,
        profile_id=profile_id,
        base_url=ADS_ENDPOINTS[marketplace.region],
    )
    products = args.ad_product or [
        "SPONSORED_PRODUCTS",
        "SPONSORED_BRANDS",
        "SPONSORED_DISPLAY",
    ]
    for product in products:
        run_id = store.start_run(
            marketplace_id=marketplace.marketplace_id,
            source_system="amazon_ads_api",
            report_type=f"{product}_daily_v3",
            period_start=args.start_date,
            period_end=args.end_date,
            granularity="DAY",
        )
        try:
            payload = reports.download(
                _ads_specification(product, args.start_date, args.end_date)
            )
            rows = parse_ad_report(
                payload,
                marketplace=marketplace.code,
                profile_id=profile_id,
                ad_product=product,
                attribution_window_days=args.attribution_window_days,
            )
            loaded = store.write_ads(run_id, rows)
            store.finish_run(run_id, source_rows=len(payload), loaded_rows=loaded)
        except Exception as error:
            store.fail_run(run_id, error)
            raise
    return 0


def _sp_report_main(
    argv: Sequence[str] | None,
    *,
    report_type: str,
    parser: Callable[..., list[Any]],
    writer: str,
    description: str,
) -> int:
    args = _common_parser(description).parse_args(argv)
    marketplace, store = _store(args)
    snapshot_date = args.snapshot_date or _yesterday()
    run_id = store.start_run(
        marketplace_id=marketplace.marketplace_id,
        source_system="sp_api",
        report_type=report_type,
        period_start=snapshot_date,
        period_end=snapshot_date,
    )
    try:
        document = SpApiReports(
            reports_api(marketplace.region, marketplace.credential_group)
        ).download(
            report_type=report_type,
            marketplace_id=marketplace.marketplace_id,
        )
        payload = document.content.decode("utf-8-sig")
        rows = parser(
            payload,
            marketplace=marketplace.code,
            snapshot_date=snapshot_date,
        )
        loaded = getattr(store, writer)(run_id, rows)
        store.finish_run(
            run_id,
            source_rows=len(rows),
            loaded_rows=loaded,
            report_id=document.report_id,
            report_document_id=document.report_document_id,
        )
    except Exception as error:
        store.fail_run(run_id, error)
        raise
    return 0


def _common_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--marketplace", choices=tuple(MARKETPLACES), default="US")
    parser.add_argument("--snapshot-date", type=date.fromisoformat)
    parser.add_argument(
        "--database-url",
        default=os.getenv("AMAZON_DATABASE_URL") or os.getenv("DATABASE_URL"),
    )
    parser.add_argument(
        "--account-key",
        default=os.getenv("AMAZON_ACCOUNT_KEY") or os.getenv("amazon_account_key"),
    )
    parser.add_argument(
        "--brand-key",
        default=os.getenv("AMAZON_BRAND_KEY") or os.getenv("amazon_brand_key"),
    )
    parser.add_argument(
        "--brand-name",
        default=os.getenv("AMAZON_BRAND_NAME") or os.getenv("amazon_brand_name"),
    )
    parser.add_argument(
        "--seller-id",
        default=os.getenv("AMAZON_SELLER_ID") or os.getenv("SP_API_SELLER_ID"),
    )
    parser.add_argument(
        "--seller-display-name",
        default=os.getenv("AMAZON_SELLER_DISPLAY_NAME"),
    )
    return parser


def _store(args: argparse.Namespace) -> tuple[Any, AmazonSourceStore]:
    database_url = _required(args.database_url, "AMAZON_DATABASE_URL or DATABASE_URL")
    seller_id = _required(args.seller_id, "AMAZON_SELLER_ID")
    display_name = _required(args.seller_display_name, "AMAZON_SELLER_DISPLAY_NAME")
    brand_key = _required(args.brand_key, "AMAZON_BRAND_KEY")
    brand_name = _required(args.brand_name, "AMAZON_BRAND_NAME")
    marketplace = MARKETPLACES[args.marketplace]
    import psycopg

    connection = psycopg.connect(database_url)
    account_key = args.account_key or seller_id
    AmazonSalesTrafficStore(connection, account_key=account_key).bootstrap_account(
        brand_key=brand_key,
        brand_name=brand_name,
        seller_id=seller_id,
        display_name=display_name,
        marketplaces=[marketplace],
    )
    return marketplace, AmazonSourceStore(connection, account_key=account_key)


def _ads_specification(product: str, start: date, end: date) -> dict[str, Any]:
    configurations = {
        "SPONSORED_PRODUCTS": (
            "spAdvertisedProduct",
            ["advertisedProduct"],
            ["adGroupId", "adGroupName", "adId", "advertisedSku", "advertisedAsin"],
            "unitsSoldClicks14d",
        ),
        "SPONSORED_BRANDS": (
            "sbCampaigns",
            ["campaign"],
            [],
            "unitsSold14d",
        ),
        "SPONSORED_DISPLAY": (
            "sdAdvertisedProduct",
            ["advertisedProduct"],
            ["adGroupId", "adGroupName", "adId", "advertisedSku", "advertisedAsin"],
            "unitsSold14d",
        ),
    }
    report_type, group_by, dimensions, units_column = configurations[product]
    return {
        "name": f"{product.lower()}-{start}-{end}",
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "configuration": {
            "adProduct": product,
            "groupBy": group_by,
            "columns": [
                "date",
                "campaignId",
                "campaignName",
                *dimensions,
                "impressions",
                "clicks",
                "cost",
                "purchases14d",
                units_column,
                "sales14d",
            ],
            "reportTypeId": report_type,
            "timeUnit": "DAILY",
            "format": "GZIP_JSON",
        },
    }


def _required(value: str | None, name: str) -> str:
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _yesterday() -> date:
    return datetime.now(timezone.utc).date() - timedelta(days=1)
