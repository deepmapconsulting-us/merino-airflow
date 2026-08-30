"""Import LingXing ERP data into Cloud SQL every 4 hours.

Runs two imports from the same LingXing OAuth session:

1. **Logistics** (every run): FBM/FBA inventory, warehouses, listings → `erp_logistics`
2. **Order profit** (once daily on the 12:30 UTC run): multi-platform orders
   `/pb/mp/order/list` → `shopify.orders` (`erp_purchase_cost`, `erp_platform_fee`)

Stable LingXing credentials are read from GSM-backed Airflow Variables:

- `erp_app_id`
- `erp_app_secret`
- `erp_access_token` / `erp_refresh_token` (optional seed values)

The rotating OAuth token pair is cached in Airflow Variable
`erp_lingxing_oauth_cache`; refreshed tokens are not written back to GKE
Secrets.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import Variable, dag, task  # type: ignore[import-not-found]

MODULE_PATH = Path(__file__).resolve().parent
if str(MODULE_PATH) not in sys.path:
    sys.path.insert(0, str(MODULE_PATH))

from merino_erp_jobs.lingxing import (  # noqa: E402
    DEFAULT_PAGE_DELAY_SECONDS,
    DEFAULT_FBA_STORE_IDS,
    DEFAULT_INVENTORY_WAREHOUSE_NAMES,
    LINGXING_HOST,
    LingXingCredentials,
    LingXingOpenApi,
    LingXingTokenManager,
    fba_store_ids,
)
from merino_erp_jobs.logistics_import import (  # noqa: E402
    import_lingxing_rows,
    products_missing_model,
)
from merino_erp_jobs.order_profit_import import (  # noqa: E402
    DEFAULT_PAGE_LENGTH,
    MIN_PAGE_LENGTH,
    MP_ORDER_LIST_ENDPOINT,
    fetch_mp_order_rows,
    import_order_profit_rows,
)

DAG_ID = "lingxing_erp_logistics_import"
REPORT_TIMEZONE = "UTC"
POSTGRES_CONN_ID = "merino_analytics"
ERP_LOGISTICS_DB = "merino-analytics"

TOKEN_CACHE_VARIABLE = "erp_lingxing_oauth_cache"
STOCK_ENDPOINT_VARIABLE = "erp_lingxing_stock_endpoint"
STOCK_LIST_ENDPOINT_VARIABLE = "erp_lingxing_stock_list_endpoint"
PRODUCT_ENDPOINT_VARIABLE = "erp_lingxing_product_endpoint"
STORE_IDS_VARIABLE = "erp_lingxing_stock_list_store_ids"
LISTING_ENDPOINT_VARIABLE = "erp_lingxing_listing_endpoint"
PAGE_SIZE_VARIABLE = "erp_lingxing_page_size"
WAREHOUSE_ENDPOINT_VARIABLE = "erp_lingxing_warehouse_endpoint"
WAREHOUSE_NAMES_VARIABLE = "erp_lingxing_warehouse_names"
ORDER_PROFIT_PAGE_SIZE_VARIABLE = "erp_lingxing_order_profit_page_size"
ORDER_PROFIT_WINDOW_DAYS_VARIABLE = "erp_lingxing_order_profit_window_days"
ORDER_PROFIT_STORE_IDS_VARIABLE = "erp_lingxing_order_profit_store_ids"

DEFAULT_STOCK_ENDPOINT = "/erp/sc/routing/data/local_inventory/inventoryDetails"
DEFAULT_STOCK_LIST_ENDPOINT = "/erp/sc/routing/fba/fbaStock/fbaList"
DEFAULT_PRODUCT_ENDPOINT = "/erp/sc/routing/data/local_inventory/productList"
PRODUCT_DETAIL_ENDPOINT = "/erp/sc/routing/data/local_inventory/productInfo"
SELLER_LIST_ENDPOINT = "/erp/sc/data/seller/lists"
DEFAULT_WAREHOUSE_ENDPOINT = "/erp/sc/data/local_inventory/warehouse"
DEFAULT_WAREHOUSE_NAMES = DEFAULT_INVENTORY_WAREHOUSE_NAMES
DEFAULT_PAGE_SIZE = 500
MAX_PAGE_SIZE = 800
DEFAULT_ORDER_PROFIT_WINDOW_DAYS = 30
MAX_ORDER_PROFIT_PAGE_SIZE = 200
ORDER_PROFIT_DAILY_RUN_HOUR = 12
WAREHOUSE_LIST_TYPES = (1, 3, 4, 6)
STOCK_LIST_PAGE_SIZES = (20, 50, 100, 200, 500)


class AirflowVariableStore:
    def get(self, key: str, default: str = "") -> str:
        return variable_get(key, default)

    def set(self, key: str, value: str) -> None:
        Variable.set(key, value)


def variable_get(key: str, default: str = "") -> str:
    try:
        return str(Variable.get(key, default_var=default))
    except TypeError:
        try:
            return str(Variable.get(key))
        except Exception:
            return default
    except Exception:
        return default


def config_value(conf: dict[str, Any], key: str, variable_key: str, default: str = "") -> str:
    value = conf.get(key)
    if value is not None and str(value).strip():
        return str(value).strip()
    return variable_get(variable_key, default).strip()


def page_size(conf: dict[str, Any]) -> int:
    raw = config_value(conf, "page_size", PAGE_SIZE_VARIABLE, str(DEFAULT_PAGE_SIZE))
    try:
        return min(max(int(raw), 1), MAX_PAGE_SIZE)
    except ValueError:
        return DEFAULT_PAGE_SIZE


def order_profit_page_size(conf: dict[str, Any]) -> int:
    raw = config_value(conf, "order_profit_page_size", ORDER_PROFIT_PAGE_SIZE_VARIABLE, str(DEFAULT_PAGE_LENGTH))
    try:
        return min(max(int(raw), MIN_PAGE_LENGTH), MAX_ORDER_PROFIT_PAGE_SIZE)
    except ValueError:
        return DEFAULT_PAGE_LENGTH


def order_profit_window_days(conf: dict[str, Any]) -> int:
    raw = config_value(conf, "order_profit_window_days", ORDER_PROFIT_WINDOW_DAYS_VARIABLE, str(DEFAULT_ORDER_PROFIT_WINDOW_DAYS))
    try:
        return max(int(raw), 1)
    except ValueError:
        return DEFAULT_ORDER_PROFIT_WINDOW_DAYS


def postgres_database_url(postgres_conn_id: str, database: str) -> str:
    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-not-found]

    conn = PostgresHook(postgres_conn_id=postgres_conn_id).get_connection(postgres_conn_id)
    user = quote(conn.login or "", safe="")
    password = quote(conn.password or "", safe="")
    host = conn.host or "localhost"
    port = conn.port or 5432
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def current_dag_conf() -> dict[str, Any]:
    try:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        dag_run = get_current_context().get("dag_run")
        conf = getattr(dag_run, "conf", None) or {}
        return dict(conf) if isinstance(conf, dict) else {}
    except Exception:
        return {}


def lingxing_credentials() -> LingXingCredentials:
    app_id = variable_get("erp_app_id").strip()
    app_secret = variable_get("erp_app_secret").strip()
    app_key = variable_get("erp_lingxing_app_key", app_id).strip() or app_id
    if not app_id or not app_secret:
        raise RuntimeError("Set Airflow Variables `erp_app_id` and `erp_app_secret`.")
    return LingXingCredentials(app_id=app_id, app_secret=app_secret, app_key=app_key)


def lingxing_client(conf: dict[str, Any]) -> LingXingOpenApi:
    credentials = lingxing_credentials()
    host = config_value(conf, "host", "erp_lingxing_host", LINGXING_HOST)
    access_token = LingXingTokenManager(
        host=host,
        credentials=credentials,
        variable_store=AirflowVariableStore(),
        cache_key=TOKEN_CACHE_VARIABLE,
    ).access_token()
    return LingXingOpenApi(host=host, app_key=credentials.app_key, access_token=access_token)


def should_run_order_profit(conf: dict[str, Any]) -> bool:
    forced = str(conf.get("include_order_profit", "")).lower()
    if forced in {"1", "true", "yes"}:
        return True
    if str(conf.get("skip_order_profit", "")).lower() in {"1", "true", "yes"}:
        return False
    try:
        from airflow.sdk import get_current_context  # type: ignore[import-not-found]

        logical = get_current_context()["logical_date"]
        return int(logical.hour) == ORDER_PROFIT_DAILY_RUN_HOUR
    except Exception:
        return False


def order_profit_store_ids(conf: dict[str, Any], store_names: dict[int, str]) -> list[int]:
    raw = config_value(conf, "order_profit_store_ids", ORDER_PROFIT_STORE_IDS_VARIABLE, "")
    if raw:
        return [int(part.strip()) for part in raw.split(",") if part.strip()]
    return sorted(store_names)


def rolling_order_profit_period(days: int) -> tuple[date, date]:
    today = date.today()
    return today - timedelta(days=days), today + timedelta(days=1)


def wanted_warehouse_names(conf: dict[str, Any]) -> set[str]:
    raw = config_value(conf, "warehouse_names", WAREHOUSE_NAMES_VARIABLE, DEFAULT_WAREHOUSE_NAMES)
    return {part.strip() for part in raw.split(",") if part.strip()}


def local_warehouses(client: LingXingOpenApi, conf: dict[str, Any], *, page_size: int) -> list[dict[str, Any]]:
    endpoint = config_value(conf, "warehouse_endpoint", WAREHOUSE_ENDPOINT_VARIABLE, DEFAULT_WAREHOUSE_ENDPOINT)
    return client.fetch_all(endpoint, {"type": 1, "is_delete": 0}, page_size=page_size)


def all_warehouses(client: LingXingOpenApi, conf: dict[str, Any], *, page_size: int) -> list[dict[str, Any]]:
    endpoint = config_value(conf, "warehouse_endpoint", WAREHOUSE_ENDPOINT_VARIABLE, DEFAULT_WAREHOUSE_ENDPOINT)
    rows = []
    seen: set[str] = set()
    for warehouse_type in WAREHOUSE_LIST_TYPES:
        params = {"type": warehouse_type, "is_delete": 0}
        for warehouse in client.fetch_all(endpoint, params, page_size=page_size):
            wid = str(warehouse.get("wid") or "").strip()
            if wid and wid not in seen:
                seen.add(wid)
                rows.append(warehouse)
    return rows


def stock_warehouses(client: LingXingOpenApi, conf: dict[str, Any], *, page_size: int) -> list[dict[str, Any]]:
    names = wanted_warehouse_names(conf)
    warehouses = [
        warehouse
        for warehouse in all_warehouses(client, conf, page_size=page_size)
        if str(warehouse.get("name") or "").strip() in names
    ]
    if not warehouses:
        raise RuntimeError(f"No LingXing warehouses matched: {sorted(names)}")
    return warehouses


def inventory_rows_for_warehouses(
    client: LingXingOpenApi,
    conf: dict[str, Any],
    warehouses: list[dict[str, Any]],
    *,
    page_size: int,
) -> list[dict[str, Any]]:
    endpoint = config_value(conf, "stock_endpoint", STOCK_ENDPOINT_VARIABLE, DEFAULT_STOCK_ENDPOINT)
    rows: list[dict[str, Any]] = []
    for warehouse in warehouses:
        wid = str(warehouse.get("wid") or "").strip()
        if not wid:
            continue
        for row in client.fetch_all(endpoint, {"wid": wid}, page_size=page_size):
            enriched = dict(row)
            enriched.setdefault("wid", warehouse.get("wid"))
            enriched.setdefault("name", warehouse.get("name"))
            enriched.setdefault("warehouse_type", warehouse.get("type"))
            enriched.setdefault("warehouse_sub_type", warehouse.get("sub_type"))
            enriched.setdefault("country_code", warehouse.get("country_code"))
            rows.append(enriched)
    return rows


def seller_names(client: LingXingOpenApi, page_size: int) -> dict[int, str]:
    rows = client.fetch_all(SELLER_LIST_ENDPOINT, {}, page_size=page_size)
    names: dict[int, str] = {}
    for row in rows:
        sid = row.get("sid")
        name = str(row.get("name") or row.get("seller_name") or "").strip()
        if sid is not None and name:
            names[int(sid)] = name
    return names


def selected_store_ids(conf: dict[str, Any], store_names: dict[int, str]) -> list[int]:
    raw = config_value(conf, "store_ids", STORE_IDS_VARIABLE, DEFAULT_FBA_STORE_IDS)
    store_ids = fba_store_ids(raw)
    if store_ids:
        return store_ids
    return sorted(store_names)


def stock_spu_rows(
    client: LingXingOpenApi,
    conf: dict[str, Any],
    store_ids: list[int],
    *,
    page_size: int,
) -> list[dict[str, Any]]:
    endpoint = config_value(conf, "stock_list_endpoint", STOCK_LIST_ENDPOINT_VARIABLE, DEFAULT_STOCK_LIST_ENDPOINT)
    if not endpoint or not store_ids:
        return []
    return client.fetch_all_sids(
        endpoint,
        {
            "sort_field": "sku",
            "sort_type": "asc",
            "is_cost_page": "0",
            "is_hide_zero_stock": 0,
        },
        [str(sid) for sid in store_ids],
        page_size=stock_list_page_size(page_size),
    )


def product_rows(
    client: LingXingOpenApi,
    conf: dict[str, Any],
    *,
    page_size: int,
) -> tuple[list[dict[str, Any]], str]:
    endpoint = config_value(conf, "product_endpoint", PRODUCT_ENDPOINT_VARIABLE, DEFAULT_PRODUCT_ENDPOINT)
    return client.fetch_all(endpoint, {}, page_size=page_size), f"lingxing-api:{endpoint}"


def product_detail_rows(
    client: LingXingOpenApi,
    products: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for product in products:
        product_id = product.get("id")
        if product_id is None:
            continue
        client.sleep(DEFAULT_PAGE_DELAY_SECONDS)
        payload = client.post(PRODUCT_DETAIL_ENDPOINT, {"id": product_id})
        detail = payload.get("data")
        if isinstance(detail, dict) and detail:
            details.append(detail)
    return details


def stock_list_page_size(page_size: int) -> int:
    for allowed_size in reversed(STOCK_LIST_PAGE_SIZES):
        if page_size >= allowed_size:
            return allowed_size
    return STOCK_LIST_PAGE_SIZES[0]


def stock_rows_with_spu(
    stock_rows: list[dict[str, Any]],
    stock_list_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    spu_by_product_id: dict[str, dict[str, Any]] = {}
    spu_by_sku: dict[str, dict[str, Any]] = {}
    for row in stock_list_rows:
        if not lingxing_text(row.get("spu")):
            continue
        product_id = lingxing_text(row.get("product_id")) or lingxing_text(row.get("id"))
        sku = lingxing_text(row.get("sku"))
        if product_id:
            spu_by_product_id.setdefault(product_id, row)
        if sku:
            spu_by_sku.setdefault(sku, row)

    enriched_rows: list[dict[str, Any]] = []
    for row in stock_rows:
        enriched = dict(row)
        product_id = lingxing_text(row.get("product_id")) or lingxing_text(row.get("id"))
        sku = lingxing_text(row.get("sku"))
        spu_row = (spu_by_product_id.get(product_id) if product_id else None) or (
            spu_by_sku.get(sku) if sku else None
        )
        if spu_row:
            add_missing_product_fields(enriched, spu_row)
        enriched_rows.append(enriched)
    return enriched_rows


def add_missing_product_fields(stock_row: dict[str, Any], spu_row: dict[str, Any]) -> None:
    for field in (
        "seller_sku",
        "spu",
        "spu_name",
        "product_name",
        "product_brand_text",
        "category_text",
    ):
        if not lingxing_text(stock_row.get(field)) and lingxing_text(spu_row.get(field)):
            stock_row[field] = spu_row[field]


def lingxing_text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def fetch_lingxing_rows(
    conf: dict[str, Any],
    database_url: str,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    str | None,
    str | None,
    str | None,
    str | None,
    str | None,
]:
    client = lingxing_client(conf)
    size = page_size(conf)
    products, product_source = product_rows(client, conf, page_size=size)
    products = product_detail_rows(client, products_missing_model(database_url, products))
    warehouse_endpoint = config_value(conf, "warehouse_endpoint", WAREHOUSE_ENDPOINT_VARIABLE, DEFAULT_WAREHOUSE_ENDPOINT)
    warehouse_rows = all_warehouses(client, conf, page_size=size)
    stock_endpoint = config_value(conf, "stock_endpoint", STOCK_ENDPOINT_VARIABLE, DEFAULT_STOCK_ENDPOINT)
    warehouses = stock_warehouses(client, conf, page_size=size)
    stock_rows = inventory_rows_for_warehouses(client, conf, warehouses, page_size=size)
    store_names = seller_names(client, size)
    store_ids = selected_store_ids(conf, store_names)
    stock_list_endpoint = config_value(
        conf, "stock_list_endpoint", STOCK_LIST_ENDPOINT_VARIABLE, DEFAULT_STOCK_LIST_ENDPOINT
    )
    fba_stock_rows = stock_spu_rows(client, conf, store_ids, page_size=size)
    stock_rows = stock_rows_with_spu(stock_rows, fba_stock_rows)

    listing_endpoint = config_value(conf, "listing_endpoint", LISTING_ENDPOINT_VARIABLE, "")
    listing_rows: list[dict[str, Any]] = []
    if listing_endpoint:
        listing_rows = client.fetch_all(
            listing_endpoint,
            {
                "pvi_ids": "",
                "fulfillment_channel_type": "FBM",
                "exact_search": "0",
            },
            page_size=size,
        )

    return (
        products,
        warehouse_rows,
        stock_rows,
        fba_stock_rows,
        listing_rows,
        product_source,
        f"lingxing-api:{warehouse_endpoint}",
        f"lingxing-api:{stock_endpoint}",
        f"lingxing-api:{stock_list_endpoint}" if stock_list_endpoint else None,
        f"lingxing-api:{listing_endpoint}" if listing_endpoint else None,
    )


@dag(
    dag_id=DAG_ID,
    schedule="30 */4 * * *",
    start_date=pendulum.datetime(2026, 6, 23, tz=REPORT_TIMEZONE),
    catchup=False,
    max_active_runs=1,
    tags=["lingxing", "erp", "postgres", "logistics", "shopify", "order-profit"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
    },
    doc_md=__doc__,
)
def lingxing_erp_logistics_import():
    @task
    def import_lingxing_logistics() -> dict[str, int]:
        conf = current_dag_conf()
        database_url = postgres_database_url(POSTGRES_CONN_ID, ERP_LOGISTICS_DB)
        if str(conf.get("products_only", "")).lower() in {"1", "true", "yes"}:
            client = lingxing_client(conf)
            products, product_source = product_rows(client, conf, page_size=page_size(conf))
            products = product_detail_rows(client, products_missing_model(database_url, products))
            return import_lingxing_rows(
                database_url=database_url,
                product_rows=products,
                product_source=product_source,
                warehouse_rows=[],
                stock_rows=[],
                fba_stock_rows=[],
                listing_rows=[],
                warehouse_source=None,
                stock_source=None,
                fba_stock_source=None,
                listing_source=None,
            )

        (
            products,
            warehouse_rows,
            stock_rows,
            fba_stock_rows,
            listing_rows,
            product_source,
            warehouse_source,
            stock_source,
            fba_stock_source,
            listing_source,
        ) = fetch_lingxing_rows(conf, database_url)
        return import_lingxing_rows(
            database_url=database_url,
            product_rows=products,
            product_source=product_source,
            warehouse_rows=warehouse_rows,
            stock_rows=stock_rows,
            fba_stock_rows=fba_stock_rows,
            listing_rows=listing_rows,
            warehouse_source=warehouse_source,
            stock_source=stock_source,
            fba_stock_source=fba_stock_source,
            listing_source=listing_source,
        )

    @task
    def import_lingxing_order_profit() -> dict[str, int]:
        conf = current_dag_conf()
        if not should_run_order_profit(conf):
            return {"order_profit_rows": 0, "shopify_orders_updated": 0, "skipped": 1}

        client = lingxing_client(conf)
        size = order_profit_page_size(conf)
        store_names = seller_names(client, size)
        store_ids = order_profit_store_ids(conf, store_names)
        period_start, period_end = rolling_order_profit_period(order_profit_window_days(conf))
        rows = fetch_mp_order_rows(
            client,
            store_ids=store_ids,
            period_start=period_start,
            period_end=period_end,
            page_size=size,
        )
        return import_order_profit_rows(
            database_url=postgres_database_url(POSTGRES_CONN_ID, ERP_LOGISTICS_DB),
            rows=rows,
            source=f"lingxing-api:{MP_ORDER_LIST_ENDPOINT}",
            period_start=period_start,
            period_end=period_end,
        )

    import_lingxing_logistics()
    import_lingxing_order_profit()


lingxing_erp_logistics_import()
