"""Import LingXing multi-platform order profit rows into Postgres and Shopify."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from merino_erp_jobs.lingxing import LingXingOpenApi

import psycopg  # type: ignore[import-not-found]
from psycopg import Connection  # type: ignore[import-not-found]
from psycopg.rows import dict_row  # type: ignore[import-not-found]
from psycopg.types.json import Jsonb  # type: ignore[import-not-found]

from merino_erp_jobs.logistics_import import (
    ERP_LOGISTICS_SCHEMA,
    SOURCE_SYSTEM,
    create_import_run,
    finish_import_run,
    mark_current_import_batch,
)

MP_ORDER_LIST_ENDPOINT = "/pb/mp/order/list"
MIN_PAGE_LENGTH = 20
DEFAULT_PAGE_LENGTH = 50
ERP_PROFIT_SOURCE = "lingxing"
MONEY_RE = re.compile(r"[^0-9.\-]")


@dataclass(frozen=True)
class OrderProfitRecord:
    store_id: int | None
    store_name: str | None
    global_order_no: str | None
    platform_order_no: str | None
    platform_order_name: str
    purchase_cost: Decimal
    platform_fee: Decimal
    currency_code: str | None
    order_purchase_at: datetime | None
    raw_row: dict[str, Any]


def import_order_profit_rows(
    *,
    database_url: str,
    rows: Iterable[dict[str, Any]],
    source: str | None,
    period_start: date,
    period_end: date,
) -> dict[str, int]:
    records = [record for row in rows for record in parse_mp_order_profit_records(row)]
    if not records:
        return {"order_profit_rows": 0, "shopify_orders_updated": 0}

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        with conn.transaction():
            ensure_shopify_order_profit_columns(conn)
            imported, matched = import_period_records(
                conn,
                records,
                source,
                period_start=period_start,
                period_end=period_end,
            )

    return {"order_profit_rows": imported, "shopify_orders_updated": matched}


def ensure_shopify_order_profit_columns(conn: Connection[dict[str, Any]]) -> None:
    conn.execute(f"set local search_path to {ERP_LOGISTICS_SCHEMA}, shopify, public")
    conn.execute(
        """
        create table if not exists raw_lingxing_order_profit (
            raw_order_profit_id bigserial primary key,
            import_run_id bigint references import_run (import_run_id),
            store_id bigint,
            store_name text,
            global_order_no text,
            platform_order_no text,
            platform_order_name text not null,
            purchase_cost numeric(18, 2),
            platform_fee numeric(18, 2),
            currency_code text,
            order_purchase_at timestamptz,
            raw_row jsonb not null default '{}'::jsonb,
            ingested_at timestamptz not null default now(),
            unique (import_run_id, platform_order_name)
        )
        """
    )
    conn.execute(
        """
        create index if not exists idx_raw_lingxing_order_profit_platform_order_name
            on raw_lingxing_order_profit (platform_order_name)
        """
    )
    conn.execute(
        """
        alter table shopify.orders
            add column if not exists erp_purchase_cost numeric(18, 2),
            add column if not exists erp_platform_fee numeric(18, 2),
            add column if not exists erp_cost_currency text,
            add column if not exists erp_profit_source text default 'lingxing',
            add column if not exists erp_profit_synced_at timestamptz
        """
    )
    conn.execute(
        """
        create index if not exists shopify_orders_erp_profit_synced_at_idx
            on shopify.orders (erp_profit_synced_at)
        """
    )


def import_period_records(
    conn: Connection[dict[str, Any]],
    records: Iterable[OrderProfitRecord],
    source_file: str | None,
    *,
    period_start: date,
    period_end: date,
) -> tuple[int, int]:
    records = list(records)
    if not records:
        return 0, 0

    conn.execute(f"set local search_path to {ERP_LOGISTICS_SCHEMA}, shopify, public")
    import_run_id = create_import_run(
        conn,
        source_object="order_profit",
        source_file=source_file,
        row_count=len(records),
    )
    snapshot_at = conn.execute("select now() as snapshot_at").fetchone()["snapshot_at"]

    for index, record in enumerate(records, start=1):
        insert_raw_order_profit(conn, import_run_id, record, period_start, period_end)

    matched = upsert_shopify_order_costs(conn, records)
    finish_import_run(conn, import_run_id, len(records))
    mark_current_import_batch(
        conn,
        source_object="order_profit",
        import_run_id=import_run_id,
        snapshot_at=snapshot_at,
        row_count=len(records),
    )
    print(
        f"  Order profit import {period_start:%Y-%m-%d}->{period_end:%Y-%m-%d}: "
        f"{len(records)} ERP rows, {matched} Shopify orders updated",
        flush=True,
    )
    return len(records), matched


def insert_raw_order_profit(
    conn: Connection[dict[str, Any]],
    import_run_id: int,
    record: OrderProfitRecord,
    period_start: date,
    period_end: date,
) -> None:
    conn.execute(
        """
        insert into raw_lingxing_order_profit (
            import_run_id, store_id, store_name, global_order_no,
            platform_order_no, platform_order_name, purchase_cost, platform_fee,
            currency_code, order_purchase_at, raw_row
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (import_run_id, platform_order_name)
        do update set
            store_id = excluded.store_id,
            store_name = excluded.store_name,
            global_order_no = excluded.global_order_no,
            platform_order_no = excluded.platform_order_no,
            purchase_cost = excluded.purchase_cost,
            platform_fee = excluded.platform_fee,
            currency_code = excluded.currency_code,
            order_purchase_at = excluded.order_purchase_at,
            raw_row = excluded.raw_row
        """,
        (
            import_run_id,
            record.store_id,
            record.store_name,
            record.global_order_no,
            record.platform_order_no,
            record.platform_order_name,
            record.purchase_cost,
            record.platform_fee,
            record.currency_code,
            record.order_purchase_at,
            Jsonb(
                {
                    **record.raw_row,
                    "period_start": period_start.isoformat(),
                    "period_end": period_end.isoformat(),
                }
            ),
        ),
    )


def upsert_shopify_order_costs(
    conn: Connection[dict[str, Any]],
    records: Iterable[OrderProfitRecord],
) -> int:
    matched = 0
    for record in records:
        if not shopify_order_name(record.platform_order_name):
            continue
        row = conn.execute(
            """
            update shopify.orders as o
            set
                erp_purchase_cost = %s,
                erp_platform_fee = %s,
                erp_cost_currency = %s,
                erp_profit_source = %s,
                erp_profit_synced_at = now(),
                updated_at = now()
            where lower(trim(both from o.order_name)) = lower(trim(both from %s))
            returning o.order_id
            """,
            (
                record.purchase_cost,
                record.platform_fee,
                record.currency_code,
                ERP_PROFIT_SOURCE,
                record.platform_order_name,
            ),
        ).fetchone()
        if row:
            matched += 1
    return matched


def parse_mp_order_profit_records(row: dict[str, Any]) -> list[OrderProfitRecord]:
    platform_rows = row.get("platform_info") or []
    if not platform_rows:
        return []

    items = row.get("item_info") or []
    transactions = row.get("transaction_info") or []
    currency_code = text(row.get("amount_currency"))
    store_id = integer(row.get("store_id")) or integer(row.get("sid"))
    store_name = text(row.get("store_name"))
    global_order_no = text(row.get("global_order_no"))
    purchase_at = unix_timestamp(row.get("global_purchase_time"))

    records: list[OrderProfitRecord] = []
    for platform in platform_rows:
        platform_order_name = text(platform.get("platform_order_name"))
        if not platform_order_name:
            continue
        platform_order_no = text(platform.get("platform_order_no"))
        matching_items = [
            item
            for item in items
            if text(item.get("platform_order_no")) == platform_order_no
        ] or list(items)

        purchase_cost = sum_amount(matching_items, "cg_price_amount")
        platform_fee = sum_amount(matching_items, "transaction_fee_amount")

        if purchase_cost == 0 and transactions:
            purchase_cost = sum_amount(transactions, "cg_price_amount")
        if platform_fee == 0 and transactions:
            platform_fee = sum_amount(transactions, "transaction_fee_amount")

        records.append(
            OrderProfitRecord(
                store_id=store_id,
                store_name=store_name,
                global_order_no=global_order_no,
                platform_order_no=platform_order_no,
                platform_order_name=platform_order_name,
                purchase_cost=purchase_cost,
                platform_fee=platform_fee,
                currency_code=currency_code,
                order_purchase_at=purchase_at or unix_timestamp(platform.get("purchase_time")),
                raw_row=row,
            )
        )
    return records


def shopify_order_name(value: str | None) -> bool:
    return bool(value and value.startswith("#"))


def period_chunks(period_start: date, period_end: date) -> list[tuple[date, date]]:
    """Split [start, end) into calendar-month chunks (LingXing max range is 1 month)."""
    if period_end <= period_start:
        return []

    chunks: list[tuple[date, date]] = []
    current = period_start
    while current < period_end:
        if current.month == 12:
            month_end = date(current.year + 1, 1, 1)
        else:
            month_end = date(current.year, current.month + 1, 1)
        chunk_end = min(month_end, period_end)
        chunks.append((current, chunk_end))
        current = chunk_end
    return chunks


def extract_mp_order_list(response: dict[str, Any]) -> list[dict[str, Any]]:
    data = response.get("data") if isinstance(response, dict) else {}
    if isinstance(data, dict) and isinstance(data.get("data"), dict):
        data = data["data"]
    if isinstance(data, dict):
        page = data.get("list")
        return page if isinstance(page, list) else []
    return data if isinstance(data, list) else []


def fetch_mp_order_rows(
    client: LingXingOpenApi,
    *,
    store_ids: list[int],
    period_start: date,
    period_end: date,
    page_size: int,
    page_delay_seconds: float = 1.05,
) -> list[dict[str, Any]]:
    page_size = max(page_size, MIN_PAGE_LENGTH)
    rows: list[dict[str, Any]] = []
    for chunk_start, chunk_end in period_chunks(period_start, period_end):
        start_time = int(datetime.combine(chunk_start, datetime.min.time(), tzinfo=timezone.utc).timestamp())
        end_time = int(datetime.combine(chunk_end, datetime.min.time(), tzinfo=timezone.utc).timestamp())
        for sid in store_ids:
            offset = 1
            while True:
                response = client.post(
                    MP_ORDER_LIST_ENDPOINT,
                    {
                        "sid": sid,
                        "offset": offset,
                        "length": page_size,
                        "start_time": start_time,
                        "end_time": end_time,
                        "date_type": "global_purchase_time",
                    },
                )
                page = extract_mp_order_list(response)
                if not page:
                    break
                for row in page:
                    enriched = dict(row)
                    enriched.setdefault("sid", sid)
                    rows.append(enriched)
                if len(page) < page_size:
                    break
                offset += page_size
                if page_delay_seconds > 0:
                    time.sleep(page_delay_seconds)
    return rows


def sum_amount(rows: Iterable[dict[str, Any]], field: str) -> Decimal:
    total = Decimal(0)
    for row in rows:
        total += abs(money_amount(row.get(field)))
    return total


def money_amount(value: Any) -> Decimal:
    if value is None:
        return Decimal(0)
    cleaned = MONEY_RE.sub("", str(value).strip())
    if not cleaned or cleaned in {"-", "."}:
        return Decimal(0)
    try:
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return Decimal(0)


def unix_timestamp(value: Any) -> datetime | None:
    parsed = integer(value)
    if parsed is None or parsed <= 0:
        return None
    return datetime.fromtimestamp(parsed, tz=timezone.utc)


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
