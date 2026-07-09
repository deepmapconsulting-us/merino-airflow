"""Import LingXing multi-platform order profit into Shopify orders daily.

The DAG reads `/pb/mp/order/list` for Shopify platform orders and upserts
`erp_purchase_cost` and `erp_platform_fee` onto `shopify.orders` when
`platform_order_name` matches `order_name` (for example `#9732`).
"""

from __future__ import annotations

import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import Variable, dag, task  # type: ignore[import-not-found]

MODULE_PATH = Path(__file__).resolve().parent
if str(MODULE_PATH) not in sys.path:
    sys.path.insert(0, str(MODULE_PATH))

from merino_erp_jobs.lingxing import (  # noqa: E402
    LINGXING_HOST,
    LingXingCredentials,
    LingXingOpenApi,
    LingXingTokenManager,
)
from merino_erp_jobs.order_profit_import import (  # noqa: E402
    DEFAULT_PAGE_LENGTH,
    MIN_PAGE_LENGTH,
    MP_ORDER_LIST_ENDPOINT,
    import_order_profit_rows,
)

DAG_ID = "lingxing_erp_order_profit_import"
REPORT_TIMEZONE = "UTC"
POSTGRES_CONN_ID = "merino_analytics"
ERP_LOGISTICS_DB = "merino-analytics"

TOKEN_CACHE_VARIABLE = "erp_lingxing_oauth_cache"
PAGE_SIZE_VARIABLE = "erp_lingxing_order_profit_page_size"
WINDOW_DAYS_VARIABLE = "erp_lingxing_order_profit_window_days"
STORE_IDS_VARIABLE = "erp_lingxing_order_profit_store_ids"

DEFAULT_WINDOW_DAYS = 30
MAX_PAGE_SIZE = 200

SELLER_LIST_ENDPOINT = "/erp/sc/data/seller/lists"


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
    raw = config_value(conf, "page_size", PAGE_SIZE_VARIABLE, str(DEFAULT_PAGE_LENGTH))
    try:
        return min(max(int(raw), MIN_PAGE_LENGTH), MAX_PAGE_SIZE)
    except ValueError:
        return DEFAULT_PAGE_LENGTH


def window_days(conf: dict[str, Any]) -> int:
    raw = config_value(conf, "window_days", WINDOW_DAYS_VARIABLE, str(DEFAULT_WINDOW_DAYS))
    try:
        return max(int(raw), 1)
    except ValueError:
        return DEFAULT_WINDOW_DAYS


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


def fetch_order_profit_period(conf: dict[str, Any]) -> tuple[date, date, list[dict[str, Any]]]:
    credentials = lingxing_credentials()
    host = config_value(conf, "host", "erp_lingxing_host", LINGXING_HOST)
    access_token = LingXingTokenManager(
        host=host,
        credentials=credentials,
        variable_store=AirflowVariableStore(),
        cache_key=TOKEN_CACHE_VARIABLE,
    ).access_token()
    client = LingXingOpenApi(host=host, app_key=credentials.app_key, access_token=access_token)
    size = page_size(conf)

    store_names = seller_names(client, size)
    store_ids = selected_store_ids(conf, store_names)
    period_start, period_end = rolling_period(window_days(conf))
    rows = fetch_mp_order_rows(
        client,
        store_ids=store_ids,
        period_start=period_start,
        period_end=period_end,
        page_size=size,
    )
    return period_start, period_end, rows


def seller_names(client: LingXingOpenApi, size: int) -> dict[int, str]:
    rows = client.fetch_all(SELLER_LIST_ENDPOINT, {}, page_size=size)
    names: dict[int, str] = {}
    for row in rows:
        sid = row.get("sid")
        name = str(row.get("name") or row.get("seller_name") or "").strip()
        if sid is not None and name:
            names[int(sid)] = name
    return names


def selected_store_ids(conf: dict[str, Any], store_names: dict[int, str]) -> list[int]:
    raw = config_value(conf, "store_ids", STORE_IDS_VARIABLE, "")
    if raw:
        return [int(part.strip()) for part in raw.split(",") if part.strip()]
    return sorted(store_names)


def rolling_period(days: int) -> tuple[date, date]:
    today = date.today()
    return today - timedelta(days=days), today + timedelta(days=1)


def fetch_mp_order_rows(
    client: LingXingOpenApi,
    *,
    store_ids: list[int],
    period_start: date,
    period_end: date,
    page_size: int,
) -> list[dict[str, Any]]:
    start_time = int(datetime.combine(period_start, datetime.min.time(), tzinfo=timezone.utc).timestamp())
    end_time = int(datetime.combine(period_end, datetime.min.time(), tzinfo=timezone.utc).timestamp())
    rows: list[dict[str, Any]] = []
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
            page = extract_list(response)
            if not page:
                break
            for row in page:
                enriched = dict(row)
                enriched.setdefault("sid", sid)
                rows.append(enriched)
            if len(page) < page_size:
                break
            offset += page_size
            time.sleep(1.05)
    return rows


def extract_list(response: dict[str, Any]) -> list[dict[str, Any]]:
    data = response.get("data") if isinstance(response, dict) else {}
    if isinstance(data, dict) and isinstance(data.get("data"), dict):
        data = data["data"]
    if isinstance(data, dict):
        page = data.get("list")
        return page if isinstance(page, list) else []
    return data if isinstance(data, list) else []


@dag(
    dag_id=DAG_ID,
    schedule="0 10 * * *",
    start_date=pendulum.datetime(2026, 6, 26, tz=REPORT_TIMEZONE),
    catchup=False,
    max_active_runs=1,
    tags=["lingxing", "erp", "postgres", "shopify", "order-profit"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
    },
    doc_md=__doc__,
)
def lingxing_erp_order_profit_import():
    @task
    def import_lingxing_order_profit() -> dict[str, int]:
        conf = current_dag_conf()
        database_url = postgres_database_url(POSTGRES_CONN_ID, ERP_LOGISTICS_DB)
        period_start, period_end, rows = fetch_order_profit_period(conf)
        return import_order_profit_rows(
            database_url=database_url,
            rows=rows,
            source=f"lingxing-api:{MP_ORDER_LIST_ENDPOINT}",
            period_start=period_start,
            period_end=period_end,
        )

    import_lingxing_order_profit()


lingxing_erp_order_profit_import()
