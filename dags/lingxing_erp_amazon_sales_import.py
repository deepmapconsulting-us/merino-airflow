"""Import LingXing Amazon sales-performance data into Cloud SQL daily.

The DAG reads `/erp/sc/data/sales_report/asinList` for calendar months touched
by a recent rolling window. Full-month periods are refreshed so reporting views
do not double-count overlapping rolling windows.
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
    LINGXING_HOST,
    LingXingCredentials,
    LingXingOpenApi,
    LingXingTokenManager,
)
from merino_erp_jobs.sales_report_import import import_sales_report_rows  # noqa: E402

DAG_ID = "lingxing_erp_amazon_sales_import"
REPORT_TIMEZONE = "UTC"
POSTGRES_CONN_ID = "merino_analytics"
ERP_LOGISTICS_DB = "merino-shopify"

TOKEN_CACHE_VARIABLE = "erp_lingxing_oauth_cache"
PAGE_SIZE_VARIABLE = "erp_lingxing_sales_report_page_size"
WINDOW_DAYS_VARIABLE = "erp_lingxing_sales_report_window_days"
STORE_IDS_VARIABLE = "erp_lingxing_sales_report_store_ids"

DEFAULT_PAGE_SIZE = 500
MAX_PAGE_SIZE = 800
DEFAULT_WINDOW_DAYS = 14

SELLER_LIST_ENDPOINT = "/erp/sc/data/seller/lists"
SALES_REPORT_ENDPOINT = "/erp/sc/data/sales_report/asinList"


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


def fetch_sales_report_periods(conf: dict[str, Any]) -> list[tuple[date, date, list[dict[str, Any]]]]:
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
    periods = recent_month_periods(window_days(conf))

    results: list[tuple[date, date, list[dict[str, Any]]]] = []
    for period_start, period_end in periods:
        rows: list[dict[str, Any]] = []
        for sid in store_ids:
            for row in client.fetch_all(
                SALES_REPORT_ENDPOINT,
                {
                    "sid": sid,
                    "start_date": period_start.isoformat(),
                    "end_date": period_end.isoformat(),
                    "sort_field": "volume",
                    "sort_type": "desc",
                },
                page_size=size,
            ):
                enriched = dict(row)
                enriched.setdefault("sid", sid)
                enriched.setdefault("shop", store_names.get(sid, f"sid-{sid}"))
                rows.append(enriched)
        results.append((period_start, period_end, rows))
    return results


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


def recent_month_periods(days: int) -> list[tuple[date, date]]:
    today = date.today()
    start = today - timedelta(days=days)
    periods: list[tuple[date, date]] = []
    year, month = start.year, start.month
    while (year, month) <= (today.year, today.month):
        period_start = date(year, month, 1)
        period_end = next_month(period_start)
        periods.append((period_start, period_end))
        year, month = period_end.year, period_end.month
    return periods


def next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2026, 6, 26, tz=REPORT_TIMEZONE),
    catchup=False,
    tags=["lingxing", "erp", "postgres", "amazon-sales"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
    },
    doc_md=__doc__,
)
def lingxing_erp_amazon_sales_import():
    @task
    def import_lingxing_amazon_sales() -> dict[str, int]:
        conf = current_dag_conf()
        database_url = postgres_database_url(POSTGRES_CONN_ID, ERP_LOGISTICS_DB)
        imported = 0
        for period_start, period_end, rows in fetch_sales_report_periods(conf):
            result = import_sales_report_rows(
                database_url=database_url,
                rows=rows,
                source=f"lingxing-api:{SALES_REPORT_ENDPOINT}",
                period_start=period_start,
                period_end=period_end,
            )
            imported += result["sales_report_rows"]
        return {"sales_report_rows": imported}

    import_lingxing_amazon_sales()


lingxing_erp_amazon_sales_import()
