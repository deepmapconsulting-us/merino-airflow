"""Import LingXing FBA storage fees into Cloud SQL monthly.

Long-term (aged) storage fees and monthly FBA storage fees are fetched per
Amazon store (`sid`) and upserted into `erp_logistics`.
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
from merino_erp_jobs.storage_fee_import import import_storage_fee_rows  # noqa: E402

DAG_ID = "lingxing_erp_storage_fee_import"
REPORT_TIMEZONE = "UTC"
POSTGRES_CONN_ID = "merino_analytics"
ERP_LOGISTICS_DB = "merino-analytics"

TOKEN_CACHE_VARIABLE = "erp_lingxing_oauth_cache"
PAGE_SIZE_VARIABLE = "erp_lingxing_page_size"
DEFAULT_PAGE_SIZE = 500
MAX_PAGE_SIZE = 800

SELLER_LIST_ENDPOINT = "/erp/sc/data/seller/lists"
LONG_TERM_ENDPOINT = "/erp/sc/data/fba_report/storageFeeLongTerm"
MONTHLY_ENDPOINT = "/erp/sc/data/fba_report/storageFeeMonth"


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


def page_size(conf: dict[str, Any]) -> int:
    raw = str(conf.get("page_size") or variable_get(PAGE_SIZE_VARIABLE, str(DEFAULT_PAGE_SIZE))).strip()
    try:
        return min(max(int(raw), 1), MAX_PAGE_SIZE)
    except ValueError:
        return DEFAULT_PAGE_SIZE


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


def month_bounds(month: str) -> tuple[str, str]:
    year, mon = map(int, month.split("-", 1))
    if mon == 12:
        end_year, end_mon = year + 1, 1
    else:
        end_year, end_mon = year, mon + 1
    return f"{year:04d}-{mon:02d}-01", f"{end_year:04d}-{end_mon:02d}-01"


def fetch_storage_fee_rows(conf: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    credentials = lingxing_credentials()
    host = str(conf.get("host") or variable_get("erp_lingxing_host", LINGXING_HOST)).strip()
    access_token = LingXingTokenManager(
        host=host,
        credentials=credentials,
        variable_store=AirflowVariableStore(),
        cache_key=TOKEN_CACHE_VARIABLE,
    ).access_token()
    client = LingXingOpenApi(host=host, app_key=credentials.app_key, access_token=access_token)
    size = page_size(conf)

    month = str(conf.get("month") or previous_month()).strip()
    start_date, end_date = month_bounds(month)

    store_names: dict[int, str] = {}
    for row in client.fetch_all(SELLER_LIST_ENDPOINT, {}, page_size=size):
        sid = row.get("sid")
        name = str(row.get("name") or "").strip()
        if sid is not None and name:
            store_names[int(sid)] = name

    long_term_rows: list[dict[str, Any]] = []
    monthly_rows: list[dict[str, Any]] = []
    for sid in sorted(store_names):
        for row in client.fetch_all(
            LONG_TERM_ENDPOINT,
            {"sid": sid, "start_date": start_date, "end_date": end_date},
            page_size=size,
        ):
            enriched = dict(row)
            enriched.setdefault("sid", sid)
            enriched.setdefault("shop", store_names[sid])
            long_term_rows.append(enriched)
        for row in client.fetch_all(
            MONTHLY_ENDPOINT,
            {"sid": sid, "month": month},
            page_size=size,
        ):
            enriched = dict(row)
            enriched.setdefault("sid", sid)
            enriched.setdefault("shop", store_names[sid])
            monthly_rows.append(enriched)

    return long_term_rows, monthly_rows


def previous_month() -> str:
    today = date.today().replace(day=1)
    prior = today - timedelta(days=1)
    return f"{prior.year:04d}-{prior.month:02d}"


@dag(
    dag_id=DAG_ID,
    schedule="0 6 3 * *",
    start_date=pendulum.datetime(2026, 6, 23, tz=REPORT_TIMEZONE),
    catchup=False,
    max_active_runs=1,
    tags=["lingxing", "erp", "postgres", "storage-fee"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
    },
    doc_md=__doc__,
)
def lingxing_erp_storage_fee_import():
    @task
    def import_lingxing_storage_fees() -> dict[str, int]:
        conf = current_dag_conf()
        long_term_rows, monthly_rows = fetch_storage_fee_rows(conf)
        return import_storage_fee_rows(
            database_url=postgres_database_url(POSTGRES_CONN_ID, ERP_LOGISTICS_DB),
            long_term_rows=long_term_rows,
            monthly_rows=monthly_rows,
            long_term_source=f"lingxing-api:{LONG_TERM_ENDPOINT}",
            monthly_source=f"lingxing-api:{MONTHLY_ENDPOINT}",
        )

    import_lingxing_storage_fees()


lingxing_erp_storage_fee_import()
