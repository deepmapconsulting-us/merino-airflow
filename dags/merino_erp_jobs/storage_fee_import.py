"""Import LingXing FBA storage fee rows into the `erp_logistics` schema."""

from __future__ import annotations

from datetime import date, datetime
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

LONG_TERM_FEE_KEY = "12_mo_long_terms_storage_fee"
SHORT_TERM_FEE_KEY = "6_mo_long_terms_storage_fee"
LONG_TERM_QTY_KEY = "qty_charged_12_mo_long_term_storage_fee"
SHORT_TERM_QTY_KEY = "qty_charged_6_mo_long_term_storage_fee"


MONTHLY_IMPORT_BATCH_SIZE = 1000


def import_storage_fee_rows(
    *,
    database_url: str,
    long_term_rows: Iterable[dict[str, Any]],
    monthly_rows: Iterable[dict[str, Any]],
    long_term_source: str | None,
    monthly_source: str | None,
) -> dict[str, int]:
    long_term_list = list(long_term_rows)
    monthly_list = list(monthly_rows)

    long_term_count = 0
    if long_term_list:
        with psycopg.connect(database_url, row_factory=dict_row) as conn:
            with conn.transaction():
                use_erp_logistics_schema(conn)
                long_term_count = import_long_term_rows(conn, long_term_list, long_term_source)

    monthly_count = 0
    for offset in range(0, len(monthly_list), MONTHLY_IMPORT_BATCH_SIZE):
        chunk = monthly_list[offset : offset + MONTHLY_IMPORT_BATCH_SIZE]
        with psycopg.connect(database_url, row_factory=dict_row) as conn:
            with conn.transaction():
                use_erp_logistics_schema(conn)
                monthly_count += import_monthly_rows(conn, chunk, monthly_source)

    return {
        "long_term_rows": long_term_count,
        "monthly_rows": monthly_count,
    }


def use_erp_logistics_schema(conn: Connection[dict[str, Any]]) -> None:
    conn.execute(f"set local search_path to {ERP_LOGISTICS_SCHEMA}, public")


def import_long_term_rows(
    conn: Connection[dict[str, Any]],
    rows: Iterable[dict[str, Any]],
    source_file: str | None,
) -> int:
    rows = list(rows)
    if not rows:
        return 0

    import_run_id = create_import_run(
        conn,
        source_object="fba_long_term_storage_fee",
        source_file=source_file,
        row_count=len(rows),
    )
    imported = 0
    for index, row in enumerate(rows, start=1):
        raw_id = insert_raw_long_term_fee(conn, import_run_id, row)
        store_id = upsert_store(conn, row)
        if store_id is None:
            continue
        upsert_long_term_fee_line(conn, store_id=store_id, source_raw_id=raw_id, row=row)
        imported = index
    finish_import_run(conn, import_run_id, imported)
    return imported


def import_monthly_rows(
    conn: Connection[dict[str, Any]],
    rows: Iterable[dict[str, Any]],
    source_file: str | None,
) -> int:
    rows = list(rows)
    if not rows:
        return 0

    import_run_id = create_import_run(
        conn,
        source_object="fba_monthly_storage_fee",
        source_file=source_file,
        row_count=len(rows),
    )
    imported = 0
    for index, row in enumerate(rows, start=1):
        raw_id = insert_raw_monthly_fee(conn, import_run_id, row)
        store_id = upsert_store(conn, row)
        if store_id is None:
            continue
        upsert_monthly_fee_line(conn, store_id=store_id, source_raw_id=raw_id, row=row)
        imported = index
    finish_import_run(conn, import_run_id, imported)
    return imported


def insert_raw_long_term_fee(
    conn: Connection[dict[str, Any]],
    import_run_id: int,
    row: dict[str, Any],
) -> int:
    snapshot_at = parse_timestamp(row.get("snapshot_date"))
    result = conn.execute(
        """
        insert into raw_lingxing_fba_long_term_storage_fee (
            import_run_id, store_id, snapshot_date, sku, fnsku, asin, product_name,
            condition, country, currency, surcharge_age_tier, qty_short_term,
            fee_short_term, qty_long_term, fee_long_term, per_unit_volume,
            volume_unit, raw_row
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        returning raw_long_term_fee_id
        """,
        (
            import_run_id,
            integer(row.get("sid")),
            snapshot_at,
            text(row.get("sku")),
            text(row.get("fnsku")),
            text(row.get("asin")),
            text(row.get("product_name")),
            text(row.get("condition")),
            text(row.get("country")),
            text(row.get("currency")),
            text(row.get("surcharge_age_tier")),
            number(row.get(SHORT_TERM_QTY_KEY)),
            number(row.get(SHORT_TERM_FEE_KEY)),
            number(row.get(LONG_TERM_QTY_KEY)),
            number(row.get(LONG_TERM_FEE_KEY)),
            number(row.get("per_unit_volume")),
            text(row.get("volume_unit")),
            Jsonb(row),
        ),
    ).fetchone()
    return int(result["raw_long_term_fee_id"])


def insert_raw_monthly_fee(
    conn: Connection[dict[str, Any]],
    import_run_id: int,
    row: dict[str, Any],
) -> int:
    result = conn.execute(
        """
        insert into raw_lingxing_fba_monthly_storage_fee (
            import_run_id, store_id, month_of_charge, asin, fnsku, product_name,
            fulfillment_center, country_code, currency, estimated_monthly_storage_fee,
            raw_row
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        returning raw_monthly_fee_id
        """,
        (
            import_run_id,
            integer(row.get("sid")),
            text(row.get("month_of_charge")),
            text(row.get("asin")),
            text(row.get("fnsku")),
            text(row.get("product_name")),
            text(row.get("fulfillment_center")),
            text(row.get("country_code")),
            text(row.get("currency")),
            number(row.get("estimated_monthly_storage_fee")),
            Jsonb(row),
        ),
    ).fetchone()
    return int(result["raw_monthly_fee_id"])


def upsert_long_term_fee_line(
    conn: Connection[dict[str, Any]],
    *,
    store_id: int,
    source_raw_id: int,
    row: dict[str, Any],
) -> None:
    snapshot_date = parse_date(row.get("snapshot_date"))
    if snapshot_date is None:
        return

    conn.execute(
        """
        insert into fba_long_term_storage_fee_line (
            store_id, snapshot_date, sku, fnsku, asin, product_name, condition,
            country, currency, surcharge_age_tier, qty_short_term, fee_short_term,
            qty_long_term, fee_long_term, per_unit_volume, volume_unit,
            source_system, source_raw_id, raw_row, updated_at
        )
        values (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
        )
        on conflict (store_id, snapshot_date, asin, fnsku, sku, surcharge_age_tier)
        do update set
            product_name = excluded.product_name,
            condition = excluded.condition,
            country = excluded.country,
            currency = excluded.currency,
            qty_short_term = excluded.qty_short_term,
            fee_short_term = excluded.fee_short_term,
            qty_long_term = excluded.qty_long_term,
            fee_long_term = excluded.fee_long_term,
            per_unit_volume = excluded.per_unit_volume,
            volume_unit = excluded.volume_unit,
            source_raw_id = excluded.source_raw_id,
            raw_row = excluded.raw_row,
            updated_at = now()
        """,
        (
            store_id,
            snapshot_date,
            text(row.get("sku")),
            text(row.get("fnsku")),
            text(row.get("asin")),
            text(row.get("product_name")),
            text(row.get("condition")),
            text(row.get("country")),
            text(row.get("currency")) or "",
            text(row.get("surcharge_age_tier")) or "",
            number(row.get(SHORT_TERM_QTY_KEY)) or Decimal(0),
            number(row.get(SHORT_TERM_FEE_KEY)) or Decimal(0),
            number(row.get(LONG_TERM_QTY_KEY)) or Decimal(0),
            number(row.get(LONG_TERM_FEE_KEY)) or Decimal(0),
            number(row.get("per_unit_volume")),
            text(row.get("volume_unit")),
            SOURCE_SYSTEM,
            source_raw_id,
            Jsonb(row),
        ),
    )


def upsert_monthly_fee_line(
    conn: Connection[dict[str, Any]],
    *,
    store_id: int,
    source_raw_id: int,
    row: dict[str, Any],
) -> None:
    month_of_charge = parse_month(row.get("month_of_charge"))
    if month_of_charge is None:
        return

    conn.execute(
        """
        insert into fba_monthly_storage_fee_line (
            store_id, month_of_charge, asin, fnsku, product_name, fulfillment_center,
            country_code, currency, storage_rate, estimated_monthly_storage_fee,
            average_quantity_on_hand, estimated_total_item_volume,
            source_system, source_raw_id, raw_row, updated_at
        )
        values (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
        )
        on conflict (store_id, month_of_charge, asin, fnsku, fulfillment_center)
        do update set
            product_name = excluded.product_name,
            country_code = excluded.country_code,
            currency = excluded.currency,
            storage_rate = excluded.storage_rate,
            estimated_monthly_storage_fee = excluded.estimated_monthly_storage_fee,
            average_quantity_on_hand = excluded.average_quantity_on_hand,
            estimated_total_item_volume = excluded.estimated_total_item_volume,
            source_raw_id = excluded.source_raw_id,
            raw_row = excluded.raw_row,
            updated_at = now()
        """,
        (
            store_id,
            month_of_charge,
            text(row.get("asin")),
            text(row.get("fnsku")),
            text(row.get("product_name")),
            text(row.get("fulfillment_center")) or "",
            text(row.get("country_code")),
            text(row.get("currency")),
            number(row.get("storage_rate")),
            number(row.get("estimated_monthly_storage_fee")) or Decimal(0),
            number(row.get("average_quantity_on_hand")),
            number(row.get("estimated_total_item_volume")),
            SOURCE_SYSTEM,
            source_raw_id,
            Jsonb(row),
        ),
    )


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


def parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def parse_date(value: Any) -> date | None:
    timestamp = parse_timestamp(value)
    if timestamp is not None:
        return timestamp.date()
    raw = text(value)
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def parse_month(value: Any) -> date | None:
    raw = text(value)
    if not raw:
        return None
    try:
        year, month = raw.split("-", 1)
        return date(int(year), int(month), 1)
    except (TypeError, ValueError):
        return None
