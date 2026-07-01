"""Import LingXing Amazon sales-performance rows into `erp_logistics`."""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

import psycopg  # type: ignore[import-not-found]
from psycopg import Connection  # type: ignore[import-not-found]
from psycopg.rows import dict_row  # type: ignore[import-not-found]
from psycopg.types.json import Jsonb  # type: ignore[import-not-found]

from merino_erp_jobs.logistics_import import (
    ERP_LOGISTICS_SCHEMA,
    SOURCE_SYSTEM,
    create_import_run,
    finish_import_run,
    upsert_store,
)


def import_sales_report_rows(
    *,
    database_url: str,
    rows: Iterable[dict[str, Any]],
    source: str | None,
    period_start: date,
    period_end: date,
    dimension: str = "asin",
) -> dict[str, int]:
    rows = list(rows)
    if not rows:
        return {"sales_report_rows": 0}

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        with conn.transaction():
            use_erp_logistics_schema(conn)
            count = import_period_rows(
                conn,
                rows,
                source,
                period_start=period_start,
                period_end=period_end,
                dimension=dimension,
            )

    return {"sales_report_rows": count}


def use_erp_logistics_schema(conn: Connection[dict[str, Any]]) -> None:
    conn.execute(f"set local search_path to {ERP_LOGISTICS_SCHEMA}, public")


def import_period_rows(
    conn: Connection[dict[str, Any]],
    rows: Iterable[dict[str, Any]],
    source_file: str | None,
    *,
    period_start: date,
    period_end: date,
    dimension: str,
) -> int:
    rows = list(rows)
    if not rows:
        return 0

    import_run_id = create_import_run(
        conn,
        source_object="amazon_sales_performance",
        source_file=source_file,
        row_count=len(rows),
    )
    imported = 0
    total = len(rows)
    for index, row in enumerate(rows, start=1):
        raw_id = insert_raw_sales_report(conn, import_run_id, row, period_start, period_end)
        store_id = upsert_store(conn, row)
        if store_id is None:
            continue
        upsert_sales_performance_period(
            conn,
            store_id=store_id,
            source_raw_id=raw_id,
            row=row,
            period_start=period_start,
            period_end=period_end,
            dimension=dimension,
        )
        imported = index
        if index == 1 or index % 1000 == 0 or index == total:
            print(
                f"  Postgres import {period_start:%Y-%m}: {index}/{total} rows processed",
                flush=True,
            )

    finish_import_run(conn, import_run_id, imported)
    return imported


def insert_raw_sales_report(
    conn: Connection[dict[str, Any]],
    import_run_id: int,
    row: dict[str, Any],
    period_start: date,
    period_end: date,
) -> int:
    result = conn.execute(
        """
        insert into raw_lingxing_sales_report_asin_list (
            import_run_id, store_id, period_start, period_end, asin, parent_asin,
            seller_sku, local_sku, sku, spu, product_name, currency_code,
            volume, amount, sales_amount, order_items, raw_row
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        returning raw_sales_report_id
        """,
        (
            import_run_id,
            integer(row.get("sid")),
            period_start,
            period_end,
            text(row.get("asin")),
            text(row.get("parent_asin")),
            text(row.get("seller_sku")) or text(row.get("msku")),
            text(row.get("local_sku")),
            text(row.get("sku")),
            text(row.get("spu")),
            product_name(row),
            text(row.get("currency_code")),
            number(row.get("volume")),
            number(row.get("amount")),
            number(row.get("sales_amount")) or number(row.get("amount")),
            number(row.get("order_items")) or number(row.get("order_num")),
            Jsonb(row),
        ),
    ).fetchone()
    return int(result["raw_sales_report_id"])


def upsert_sales_performance_period(
    conn: Connection[dict[str, Any]],
    *,
    store_id: int,
    source_raw_id: int,
    row: dict[str, Any],
    period_start: date,
    period_end: date,
    dimension: str,
) -> None:
    dimension_value = sales_dimension_value(row, dimension)
    if not dimension_value:
        return

    conn.execute(
        """
        insert into amazon_sales_performance_period (
            source_system, store_id, period_start, period_end, dimension,
            dimension_value, asin, parent_asin, seller_sku, local_sku, sku, spu,
            product_name, currency_code, units_sold, sales_amount, order_count,
            source_raw_id, raw_row, updated_at
        )
        values (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, now()
        )
        on conflict (source_system, store_id, period_start, period_end, dimension, dimension_value)
        do update set
            asin = excluded.asin,
            parent_asin = excluded.parent_asin,
            seller_sku = excluded.seller_sku,
            local_sku = excluded.local_sku,
            sku = excluded.sku,
            spu = excluded.spu,
            product_name = excluded.product_name,
            currency_code = excluded.currency_code,
            units_sold = excluded.units_sold,
            sales_amount = excluded.sales_amount,
            order_count = excluded.order_count,
            source_raw_id = excluded.source_raw_id,
            raw_row = excluded.raw_row,
            updated_at = now()
        """,
        (
            SOURCE_SYSTEM,
            store_id,
            period_start,
            period_end,
            dimension,
            dimension_value,
            text(row.get("asin")),
            text(row.get("parent_asin")),
            text(row.get("seller_sku")) or text(row.get("msku")),
            text(row.get("local_sku")),
            text(row.get("sku")),
            text(row.get("spu")),
            product_name(row),
            text(row.get("currency_code")),
            number(row.get("volume")) or Decimal(0),
            number(row.get("sales_amount")) or number(row.get("amount")) or Decimal(0),
            number(row.get("order_items")) or number(row.get("order_num")) or Decimal(0),
            source_raw_id,
            Jsonb(row),
        ),
    )


def sales_dimension_value(row: dict[str, Any], dimension: str) -> str | None:
    if dimension == "sku":
        return text(row.get("sku")) or text(row.get("local_sku")) or text(row.get("seller_sku"))
    if dimension == "msku":
        return text(row.get("seller_sku")) or text(row.get("msku"))
    if dimension == "spu":
        return text(row.get("spu"))
    return text(row.get("asin"))


def product_name(row: dict[str, Any]) -> str | None:
    return text(row.get("product_name")) or text(row.get("item_name")) or text(row.get("local_name"))


def text(value: Any) -> str | None:
    if value is None:
        return None
    stripped = str(value).strip()
    return stripped or None


def integer(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def number(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return None
