from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from merino_amazon_jobs.marketplaces import Marketplace
from merino_amazon_jobs.postgres import AmazonSalesTrafficStore
from merino_amazon_jobs.reports import SalesTrafficReports
from merino_amazon_jobs.sales_traffic import parse_sales_traffic

logger = logging.getLogger(__name__)


def load_sales_traffic(
    reports: SalesTrafficReports,
    store: AmazonSalesTrafficStore,
    marketplace: Marketplace,
    start_date: date,
    end_date: date,
    granularity: str,
    *,
    overwrite: bool,
) -> int:
    total_rows = 0
    report_date = start_date
    report_timezone = ZoneInfo(marketplace.timezone)
    while report_date <= end_date:
        run_id = store.start_run(
            marketplace,
            report_date,
            report_date,
            granularity,
        )
        try:
            downloaded = reports.download(
                marketplace,
                datetime.combine(report_date, time.min, report_timezone),
                datetime.combine(report_date, time.max, report_timezone),
                granularity,
                on_status=lambda report_id, document_id, status, run_id=run_id: (
                    store.update_run(
                        run_id,
                        status=status,
                        report_id=report_id,
                        report_document_id=document_id,
                    )
                ),
            )
            listings = store.listings(marketplace, report_date)
            rows = parse_sales_traffic(
                downloaded.payload,
                marketplace=marketplace.code,
                report_date=report_date,
                granularity=granularity,
                listings=listings,
            )
            count = store.write_rows(run_id, rows, overwrite=overwrite)
            store.finish_run(
                run_id,
                report_id=downloaded.report_id,
                report_document_id=downloaded.report_document_id,
                source_row_count=len(
                    downloaded.payload.get("salesAndTrafficByAsin", [])
                ),
                loaded_row_count=count,
            )
            total_rows += count
            logger.info(
                "loaded marketplace=%s date=%s granularity=%s rows=%s",
                marketplace.code,
                report_date,
                granularity,
                count,
            )
        except Exception as exc:
            store.fail_run(run_id, exc)
            raise
        report_date += timedelta(days=1)
    return total_rows
