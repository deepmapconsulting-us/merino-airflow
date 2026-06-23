"""Import LingXing FBM logistics data into Cloud SQL every 4 hours.

Stable LingXing credentials are read from GSM-backed Airflow Variables:

- `erp_app_id`
- `erp_app_secret`
- `erp_docs_api_key` (optional; falls back to `erp_app_id`)
- `erp_access_token` / `erp_refresh_token` (optional seed values)

The rotating OAuth token pair is cached in Airflow Variable
`erp_lingxing_oauth_cache`; refreshed tokens are not written back to GKE
Secrets.

Each run upserts the latest ERP product/store/warehouse relationships and appends
an inventory snapshot. Reporting views read the newest snapshot per SKU and
warehouse.
"""

from __future__ import annotations

import sys
from datetime import timedelta
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
    parse_store_ids,
)
from merino_erp_jobs.logistics_import import import_lingxing_rows  # noqa: E402

DAG_ID = "lingxing_erp_logistics_import"
REPORT_TIMEZONE = "UTC"
POSTGRES_CONN_ID = "merino_analytics"
ERP_LOGISTICS_DB = "erp_logistics"

TOKEN_CACHE_VARIABLE = "erp_lingxing_oauth_cache"
STOCK_ENDPOINT_VARIABLE = "erp_lingxing_stock_endpoint"
LISTING_ENDPOINT_VARIABLE = "erp_lingxing_listing_endpoint"
STOCK_SIDS_VARIABLE = "erp_lingxing_stock_sids"
PAGE_SIZE_VARIABLE = "erp_lingxing_page_size"
STOCK_CHANNEL_VARIABLE = "erp_lingxing_stock_fulfillment_channel"

DEFAULT_STOCK_ENDPOINT = "/erp/sc/routing/fba/fbaStock/fbaList"
DEFAULT_STOCK_SIDS = "8804,8803,8811,8807,11986,8795,8805,8797,8793,8813,8812,13982"
DEFAULT_PAGE_SIZE = 500


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
        return max(int(raw), 1)
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
    app_key = variable_get("erp_docs_api_key", app_id).strip() or app_id
    if not app_id or not app_secret:
        raise RuntimeError("Set Airflow Variables `erp_app_id` and `erp_app_secret`.")
    return LingXingCredentials(app_id=app_id, app_secret=app_secret, app_key=app_key)


def fetch_lingxing_rows(conf: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str | None, str | None]:
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

    stock_endpoint = config_value(conf, "stock_endpoint", STOCK_ENDPOINT_VARIABLE, DEFAULT_STOCK_ENDPOINT)
    stock_sids = parse_store_ids(config_value(conf, "stock_sids", STOCK_SIDS_VARIABLE, DEFAULT_STOCK_SIDS))
    stock_params: dict[str, Any] = {
        "sort_field": "sku",
        "sort_type": "asc",
        "is_cost_page": "0",
        "is_hide_zero_stock": "0",
    }
    stock_channel = config_value(conf, "stock_fulfillment_channel", STOCK_CHANNEL_VARIABLE, "FBM")
    if stock_channel:
        stock_params["fulfillment_channel_type"] = stock_channel
    stock_rows = client.fetch_all_sids(stock_endpoint, stock_params, stock_sids, page_size=size)

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
        stock_rows,
        listing_rows,
        f"lingxing-api:{stock_endpoint}",
        f"lingxing-api:{listing_endpoint}" if listing_endpoint else None,
    )


@dag(
    dag_id=DAG_ID,
    schedule="30 */4 * * *",
    start_date=pendulum.datetime(2026, 6, 23, tz=REPORT_TIMEZONE),
    catchup=False,
    tags=["lingxing", "erp", "postgres", "logistics"],
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
        stock_rows, listing_rows, stock_source, listing_source = fetch_lingxing_rows(conf)
        return import_lingxing_rows(
            database_url=postgres_database_url(POSTGRES_CONN_ID, ERP_LOGISTICS_DB),
            stock_rows=stock_rows,
            listing_rows=listing_rows,
            stock_source=stock_source,
            listing_source=listing_source,
        )

    import_lingxing_logistics()


lingxing_erp_logistics_import()
