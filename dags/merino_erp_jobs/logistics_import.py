"""Import LingXing logistics rows into the `erp_logistics` schema."""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

import psycopg  # type: ignore[import-not-found]
from psycopg import Connection  # type: ignore[import-not-found]
from psycopg.rows import dict_row  # type: ignore[import-not-found]
from psycopg.types.json import Jsonb  # type: ignore[import-not-found]

SOURCE_SYSTEM = "lingxing"


def import_lingxing_rows(
    *,
    database_url: str,
    stock_rows: Iterable[dict[str, Any]],
    listing_rows: Iterable[dict[str, Any]],
    stock_source: str | None,
    listing_source: str | None,
    warehouse_rows: Iterable[dict[str, Any]] | None = None,
    warehouse_source: str | None = None,
) -> dict[str, int]:
    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        with conn.transaction():
            warehouse_count = import_warehouse_rows(conn, warehouse_rows or [], warehouse_source)
            stock_count = import_stock_rows(conn, stock_rows, stock_source)
            listing_count = import_listing_rows(conn, listing_rows, listing_source)
    return {
        "warehouse_rows": warehouse_count,
        "stock_rows": stock_count,
        "listing_rows": listing_count,
    }


def import_warehouse_rows(
    conn: Connection[dict[str, Any]],
    rows: Iterable[dict[str, Any]],
    source_file: str | None,
) -> int:
    rows = list(rows)
    if not rows:
        return 0

    import_run_id = create_import_run(
        conn,
        source_object="warehouse",
        source_file=source_file,
        row_count=len(rows),
    )
    imported = 0
    for index, row in enumerate(rows, start=1):
        upsert_warehouse(conn, row)
        imported = index
    finish_import_run(conn, import_run_id, imported)
    return imported


def import_stock_rows(
    conn: Connection[dict[str, Any]],
    rows: Iterable[dict[str, Any]],
    source_file: str | None,
) -> int:
    rows = list(rows)
    if not rows:
        return 0

    import_run_id = create_import_run(
        conn,
        source_object="fbm_stock",
        source_file=source_file,
        row_count=len(rows),
    )
    snapshot_at = conn.execute("select now() as snapshot_at").fetchone()["snapshot_at"]
    imported = 0

    for index, row in enumerate(rows, start=1):
        raw_stock_id = insert_raw_stock(conn, import_run_id, snapshot_at, row)
        store_id = upsert_store(conn, row)
        warehouse_id = upsert_warehouse(conn, row)
        product_id = upsert_product_from_stock(conn, row)
        if product_id is None:
            continue

        conn.execute(
            """
            insert into warehouse_product (
                warehouse_id, product_id, store_id, seller_sku, local_sku,
                asin, parent_asin, fnsku, fulfillment_channel, updated_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            on conflict (warehouse_id, product_id, store_id, seller_sku)
            do update set
                local_sku = excluded.local_sku,
                asin = excluded.asin,
                parent_asin = excluded.parent_asin,
                fnsku = excluded.fnsku,
                fulfillment_channel = excluded.fulfillment_channel,
                active = true,
                updated_at = now()
            """,
            (
                warehouse_id,
                product_id,
                store_id,
                warehouse_product_sku(row),
                text(row.get("sku")) or text(row.get("local_sku")),
                text(row.get("asin")),
                text(row.get("parent_asin_real")) or text(row.get("parent_asin")),
                text(row.get("fnsku")),
                text(row.get("fulfillment_channel_name")) or text(row.get("fulfillment_channel")),
            ),
        )

        conn.execute(
            """
            insert into inventory_snapshot (
                snapshot_at, snapshot_date, warehouse_id, product_id, store_id,
                seller_sku, sku, available_qty, reserved_qty, inbound_qty,
                outbound_qty, damaged_qty, defective_qty, total_qty, total_cost,
                source_system, source_raw_id, raw_snapshot
            )
            values (
                %s, (%s)::date, %s, %s, %s, %s, %s, %s, %s, %s,
                0, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                snapshot_at,
                snapshot_at,
                warehouse_id,
                product_id,
                store_id,
                warehouse_product_sku(row),
                product_sku(row),
                available_quantity(row),
                reserved_quantity(row),
                inbound_quantity(row),
                damaged_quantity(row),
                defective_quantity(row),
                total_quantity(row),
                total_cost(row),
                SOURCE_SYSTEM,
                raw_stock_id,
                Jsonb(row),
            ),
        )
        imported = index

    finish_import_run(conn, import_run_id, imported)
    return imported


def import_listing_rows(
    conn: Connection[dict[str, Any]],
    rows: Iterable[dict[str, Any]],
    source_file: str | None,
) -> int:
    rows = list(rows)
    if not rows:
        return 0

    import_run_id = create_import_run(
        conn,
        source_object="fbm_listing",
        source_file=source_file,
        row_count=len(rows),
    )
    imported = 0

    for index, row in enumerate(rows, start=1):
        insert_raw_listing(conn, import_run_id, row)
        upsert_store(conn, row)
        upsert_product_from_listing(conn, row)
        imported = index

    finish_import_run(conn, import_run_id, imported)
    return imported


def create_import_run(
    conn: Connection[dict[str, Any]],
    *,
    source_object: str,
    source_file: str | None,
    row_count: int,
) -> int:
    row = conn.execute(
        """
        insert into import_run (
            source_system, source_object, source_file_name, source_row_count
        )
        values (%s, %s, %s, %s)
        returning import_run_id
        """,
        (SOURCE_SYSTEM, source_object, source_file, row_count),
    ).fetchone()
    return int(row["import_run_id"])


def finish_import_run(
    conn: Connection[dict[str, Any]],
    import_run_id: int,
    imported_count: int,
) -> None:
    conn.execute(
        """
        update import_run
        set status = 'completed',
            completed_at = now(),
            notes = %s
        where import_run_id = %s
        """,
        (f"imported_rows={imported_count}", import_run_id),
    )


def insert_raw_stock(
    conn: Connection[dict[str, Any]],
    import_run_id: int,
    snapshot_at: Any,
    row: dict[str, Any],
) -> int:
    result = conn.execute(
        """
        insert into raw_lingxing_fbm_stock (
            import_run_id, snapshot_at, sid, seller_name, seller_id,
            seller_account_name, warehouse_name, storage_type, storage_type_name,
            fulfillment_channel, fulfillment_channel_name, seller_sku, local_sku,
            sku, spu, spu_name, asin, parent_asin, fnsku, product_name,
            product_brand, category_text, quantity, available_total,
            total_onhand_quantity, total_fulfillable_quantity,
            reserved_customerorders, reserved_fc_transfers,
            reserved_fc_processing, inbound_working_quantity,
            inbound_shipped_quantity, inbound_receiving_quantity,
            unsellable_quantity, damaged_quantity, defective_quantity,
            total_price, total_cost, raw_row
        )
        values (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
        )
        returning raw_stock_id
        """,
        (
            import_run_id,
            snapshot_at,
            integer(row.get("sid")) or integer(row.get("wid")),
            text(row.get("seller_name")),
            text(row.get("seller_id")),
            text(row.get("seller_account_name")),
            text(row.get("name")),
            text(row.get("storage_type")),
            text(row.get("storage_type_name")),
            text(row.get("fulfillment_channel")),
            text(row.get("fulfillment_channel_name")),
            text(row.get("seller_sku")),
            text(row.get("local_sku")),
            text(row.get("sku")),
            text(row.get("spu")),
            text(row.get("spu_name")),
            text(row.get("asin")),
            text(row.get("parent_asin_real")) or text(row.get("parent_asin")),
            text(row.get("fnsku")),
            text(row.get("product_name")),
            text(row.get("product_brand_text")),
            text(row.get("category_text")),
            number(row.get("quantity")) or number(row.get("product_total")),
            number(row.get("available_total")) or number(row.get("product_valid_num")),
            number(row.get("total_onhand_quantity")) or number(row.get("product_total")),
            number(row.get("total_fulfillable_quantity")) or number(row.get("product_valid_num")),
            number(row.get("reserved_customerorders")) or number(row.get("product_lock_num")),
            number(row.get("reserved_fc_transfers")),
            number(row.get("reserved_fc_processing")),
            number(row.get("afn_inbound_working_quantity")),
            number(row.get("afn_inbound_shipped_quantity")),
            number(row.get("afn_inbound_receiving_quantity")),
            number(row.get("afn_unsellable_quantity")),
            damaged_quantity(row),
            number(row.get("defective_quantity")),
            number(row.get("total_price")),
            total_cost(row),
            Jsonb(row),
        ),
    ).fetchone()
    return int(result["raw_stock_id"])


def insert_raw_listing(
    conn: Connection[dict[str, Any]],
    import_run_id: int,
    row: dict[str, Any],
) -> None:
    conn.execute(
        """
        insert into raw_lingxing_listing (
            import_run_id, sid, shop, seller_name, marketplace,
            fulfillment_channel_type, seller_sku, local_sku, local_name,
            item_name, asin, parent_asin, fnsku, product_mws_id,
            product_relation_id, status, status_text, product_brand,
            category_text, quantity, price, listing_price, regular_price,
            currency_code, open_date_time, raw_row
        )
        values (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            import_run_id,
            integer(row.get("sid")),
            text(row.get("shop")),
            text(row.get("seller_name")),
            text(row.get("marketplace")),
            text(row.get("fulfillment_channel_type")),
            text(row.get("seller_sku")),
            text(row.get("local_sku")),
            text(row.get("local_name")),
            text(row.get("item_name")),
            text(row.get("asin1")) or text(row.get("asin")),
            text(row.get("parent_asin")),
            text(row.get("fnsku")),
            text(row.get("product_mws_id")),
            text(row.get("product_relation_id")),
            integer(row.get("status")),
            text(row.get("status_text")),
            text(row.get("product_brand_text")),
            text(row.get("category_text")),
            number(row.get("quantity")),
            number(row.get("price")),
            number(row.get("listing_price")),
            number(row.get("regular_price")),
            text(row.get("currency_code")),
            text(row.get("open_date_time")),
            Jsonb(row),
        ),
    )


def upsert_store(conn: Connection[dict[str, Any]], row: dict[str, Any]) -> int | None:
    sid = integer(row.get("sid")) or integer(row.get("wid"))
    store_name = text(row.get("shop")) or text(row.get("seller_name")) or text(row.get("name"))
    if sid is None or not store_name:
        return None

    result = conn.execute(
        """
        insert into store (
            store_id, source_system, store_name, seller_id, seller_account_name,
            marketplace, platform, updated_at
        )
        values (%s, %s, %s, %s, %s, %s, %s, now())
        on conflict (store_id)
        do update set
            source_system = excluded.source_system,
            store_name = excluded.store_name,
            seller_id = coalesce(excluded.seller_id, store.seller_id),
            seller_account_name = coalesce(excluded.seller_account_name, store.seller_account_name),
            marketplace = coalesce(excluded.marketplace, store.marketplace),
            platform = excluded.platform,
            active = true,
            updated_at = now()
        returning store_id
        """,
        (
            sid,
            SOURCE_SYSTEM,
            store_name,
            text(row.get("seller_id")),
            text(row.get("seller_account_name")),
            text(row.get("marketplace")),
            "amazon",
        ),
    ).fetchone()
    return int(result["store_id"])


def upsert_warehouse(conn: Connection[dict[str, Any]], row: dict[str, Any]) -> int:
    warehouse_name = warehouse_name_from_row(row)
    api_warehouse_id = warehouse_api_id(row)
    if api_warehouse_id:
        code = warehouse_code(warehouse_name, api_warehouse_id=api_warehouse_id)
        result = conn.execute(
            """
            insert into warehouse (
                warehouse_id, warehouse_code, warehouse_name, warehouse_type,
                platform_scope, source_system, source_warehouse_name, updated_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, now())
            on conflict (warehouse_id)
            do update set
                warehouse_code = excluded.warehouse_code,
                warehouse_name = excluded.warehouse_name,
                source_warehouse_name = excluded.source_warehouse_name,
                active = true,
                updated_at = now()
            returning warehouse_id
            """,
            (
                api_warehouse_id,
                code,
                warehouse_name,
                "non_fba",
                "FBM",
                SOURCE_SYSTEM,
                warehouse_name,
            ),
        ).fetchone()
        return int(result["warehouse_id"])

    existing = conn.execute(
        """
        select warehouse_id
        from warehouse
        where source_system = %s
          and warehouse_name = %s
        order by warehouse_id
        limit 1
        """,
        (SOURCE_SYSTEM, warehouse_name),
    ).fetchone()
    if existing:
        return int(existing["warehouse_id"])
    raise ValueError(f"LingXing warehouse row missing wid for new warehouse: {warehouse_name}")


def upsert_product_from_stock(
    conn: Connection[dict[str, Any]],
    row: dict[str, Any],
) -> int | None:
    sku = product_sku(row)
    if not sku:
        return None
    return upsert_product(
        conn,
        sku=sku,
        seller_sku=text(row.get("seller_sku")),
        spu=text(row.get("spu")),
        spu_name=text(row.get("spu_name")),
        product_name=text(row.get("product_name")),
        brand=text(row.get("product_brand_text")),
        category=text(row.get("category_text")),
        api_product_id=integer(row.get("product_id")),
    )


def upsert_product_from_listing(
    conn: Connection[dict[str, Any]],
    row: dict[str, Any],
) -> int | None:
    sku = text(row.get("local_sku")) or text(row.get("seller_sku"))
    if not sku:
        return None
    return upsert_product(
        conn,
        sku=sku,
        seller_sku=text(row.get("seller_sku")),
        spu=None,
        spu_name=None,
        product_name=text(row.get("local_name")) or text(row.get("item_name")),
        brand=text(row.get("product_brand_text")),
        category=text(row.get("category_text")),
        api_product_id=integer(row.get("product_relation_id")) or integer(row.get("pid")),
    )


def upsert_product(
    conn: Connection[dict[str, Any]],
    *,
    sku: str,
    seller_sku: str | None,
    spu: str | None,
    spu_name: str | None,
    product_name: str | None,
    brand: str | None,
    category: str | None,
    api_product_id: int | None,
) -> int:
    if not api_product_id:
        existing = conn.execute(
            "select product_id from product where sku = %s",
            (sku,),
        ).fetchone()
        if existing:
            return int(existing["product_id"])
        raise ValueError(f"LingXing product row missing usable product_id for new SKU: {sku}")

    existing_for_sku = conn.execute(
        "select product_id from product where sku = %s",
        (sku,),
    ).fetchone()
    if existing_for_sku and int(existing_for_sku["product_id"]) != api_product_id:
        existing_for_id = conn.execute(
            "select product_id from product where product_id = %s",
            (api_product_id,),
        ).fetchone()
        if existing_for_id:
            return int(existing_for_id["product_id"])
        conn.execute(
            "update product set product_id = %s, updated_at = now() where product_id = %s",
            (api_product_id, int(existing_for_sku["product_id"])),
        )

    result = conn.execute(
        """
        insert into product (
            product_id, sku, seller_sku, spu, spu_name, product_name,
            brand, category, source_system, updated_at
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        on conflict (product_id)
        do update set
            sku = excluded.sku,
            seller_sku = coalesce(excluded.seller_sku, product.seller_sku),
            spu = coalesce(excluded.spu, product.spu),
            spu_name = coalesce(excluded.spu_name, product.spu_name),
            product_name = coalesce(excluded.product_name, product.product_name),
            brand = coalesce(excluded.brand, product.brand),
            category = coalesce(excluded.category, product.category),
            source_system = excluded.source_system,
            active = true,
            updated_at = now()
        returning product_id
        """,
        (
            api_product_id,
            sku,
            seller_sku,
            spu,
            spu_name,
            product_name,
            brand,
            category,
            SOURCE_SYSTEM,
        ),
    ).fetchone()
    return int(result["product_id"])


def product_sku(row: dict[str, Any]) -> str | None:
    return text(row.get("local_sku")) or text(row.get("sku")) or text(row.get("seller_sku"))


def warehouse_product_sku(row: dict[str, Any]) -> str | None:
    return text(row.get("seller_sku")) or product_sku(row)


def warehouse_name_from_row(row: dict[str, Any]) -> str:
    return text(row.get("name")) or text(row.get("warehouse_name")) or "LingXing FBM"


def warehouse_api_id(row: dict[str, Any]) -> int | None:
    return integer(row.get("wid")) or integer(row.get("warehouse_id")) or integer(row.get("warehouseId"))


def warehouse_code(name: str, *, api_warehouse_id: int | None = None) -> str:
    if "梦迪" in name:
        return "mengdi"
    if "独立站" in name:
        return "independent_site_fbm"
    if api_warehouse_id:
        return f"lingxing_wid_{api_warehouse_id}"[:80]
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    if slug:
        return f"lingxing_{slug}"[:80]
    return f"lingxing_{abs(hash(name))}"


def inbound_quantity(row: dict[str, Any]) -> Decimal:
    return sum(
        value or Decimal("0")
        for value in [
            number(row.get("afn_inbound_working_quantity")),
            number(row.get("afn_inbound_shipped_quantity")),
            number(row.get("afn_inbound_receiving_quantity")),
            number(row.get("product_onway")),
        ]
    )


def damaged_quantity(row: dict[str, Any]) -> Decimal:
    return sum(
        value or Decimal("0")
        for value in [
            number(row.get("warehouse_damaged_quantity")),
            number(row.get("carrier_damaged_quantity")),
            number(row.get("distributor_damaged_quantity")),
            number(row.get("customer_damaged_quantity")),
            number(row.get("product_bad_num")),
        ]
    )


def available_quantity(row: dict[str, Any]) -> Decimal:
    return (
        number(row.get("available_total"))
        or number(row.get("quantity"))
        or number(row.get("product_valid_num"))
        or Decimal("0")
    )


def reserved_quantity(row: dict[str, Any]) -> Decimal:
    return number(row.get("reserved_customerorders")) or number(row.get("product_lock_num")) or Decimal("0")


def defective_quantity(row: dict[str, Any]) -> Decimal:
    return number(row.get("defective_quantity")) or number(row.get("product_bad_num")) or Decimal("0")


def total_quantity(row: dict[str, Any]) -> Decimal:
    return (
        number(row.get("total_onhand_quantity"))
        or number(row.get("quantity"))
        or number(row.get("available_total"))
        or number(row.get("product_total"))
        or Decimal("0")
    )


def total_cost(row: dict[str, Any]) -> Decimal | None:
    return number(row.get("total_cost")) or number(row.get("stock_cost_total"))


def text(value: Any) -> str | None:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def integer(value: Any) -> int | None:
    value = text(value)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def number(value: Any) -> Decimal | None:
    value = text(value)
    if value is None:
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None
