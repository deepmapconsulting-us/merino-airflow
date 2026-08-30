from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import types
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str((REPO / "airflow" / "dags").resolve()))

from merino_erp_jobs.lingxing import (  # noqa: E402  # type: ignore[import-not-found]
    DEFAULT_FBA_STORE_IDS,
    LingXingCredentials,
    LingXingOpenApi,
    LingXingTokenManager,
    canonical_params,
    fba_store_ids,
    lingxing_sign,
    parse_store_ids,
    post_form,
)
from merino_erp_jobs.logistics_import import (  # noqa: E402  # type: ignore[import-not-found]
    update_product_model,
    upsert_product,
    warehouse_code,
)


class FakeVariableStore:
    def __init__(self, values: dict[str, str] | None = None) -> None:
        self.values = values or {}

    def get(self, key: str, default: str = "") -> str:
        return self.values.get(key, default)

    def set(self, key: str, value: str) -> None:
        self.values[key] = value


class LingXingClientTest(unittest.TestCase):
    def test_canonical_params_sort_keys_and_skip_empty_values(self) -> None:
        self.assertEqual(
            canonical_params({"z": "", "b": 2, "a": {"y": 1, "x": 2}}),
            'a={"x":2,"y":1}&b=2',
        )

    def test_sign_is_stable_for_same_params(self) -> None:
        app_key = "1234567890abcdef"
        params = {"app_key": app_key, "timestamp": "100", "access_token": "tok", "offset": 0}

        self.assertEqual(lingxing_sign(app_key, params), lingxing_sign(app_key, dict(reversed(params.items()))))

    def test_token_manager_reuses_fresh_cached_token(self) -> None:
        store = FakeVariableStore(
            {
                "cache": json.dumps(
                    {
                        "access_token": "fresh-access",
                        "refresh_token": "fresh-refresh",
                        "expires_at": 2000,
                    }
                )
            }
        )
        calls: list[str] = []

        manager = LingXingTokenManager(
            host="https://openapi.lingxing.com",
            credentials=LingXingCredentials("app", "secret", "app"),
            variable_store=store,
            cache_key="cache",
            request_json=lambda url, params, body: calls.append(url) or {},
            now=lambda: 1000,
        )

        self.assertEqual(manager.access_token(), "fresh-access")
        self.assertEqual(calls, [])

    def test_token_manager_refreshes_and_persists_expired_token(self) -> None:
        store = FakeVariableStore(
            {
                "cache": json.dumps(
                    {
                        "access_token": "old-access",
                        "refresh_token": "old-refresh",
                        "expires_at": 900,
                    }
                )
            }
        )

        def request_json(url: str, params: dict[str, Any], body: dict[str, Any] | None) -> dict[str, Any]:
            self.assertTrue(url.endswith("/api/auth-server/oauth/refresh"))
            self.assertEqual(params["refreshToken"], "old-refresh")
            return {
                "code": 200,
                "data": {
                    "access_token": "new-access",
                    "refresh_token": "new-refresh",
                    "expires_in": 7199,
                },
            }

        manager = LingXingTokenManager(
            host="https://openapi.lingxing.com",
            credentials=LingXingCredentials("app", "secret", "app"),
            variable_store=store,
            cache_key="cache",
            request_json=request_json,
            now=lambda: 1000,
        )

        self.assertEqual(manager.access_token(), "new-access")
        cached = json.loads(store.values["cache"])
        self.assertEqual(cached["refresh_token"], "new-refresh")
        self.assertGreater(cached["expires_at"], 1000)

    def test_token_manager_falls_back_to_app_secret_when_refresh_fails(self) -> None:
        store = FakeVariableStore({"erp_refresh_token": "seed-refresh"})
        seen_urls: list[str] = []

        def request_json(url: str, params: dict[str, Any], body: dict[str, Any] | None) -> dict[str, Any]:
            del body
            seen_urls.append(url)
            if url.endswith("/api/auth-server/oauth/refresh"):
                return {"code": 500, "message": "expired"}
            self.assertEqual(params["appId"], "app")
            self.assertEqual(params["appSecret"], "secret")
            return {
                "code": 200,
                "data": {
                    "access_token": "access-from-secret",
                    "refresh_token": "refresh-from-secret",
                    "expires_in": 7199,
                },
            }

        manager = LingXingTokenManager(
            host="https://openapi.lingxing.com",
            credentials=LingXingCredentials("app", "secret", "app"),
            variable_store=store,
            cache_key="cache",
            request_json=request_json,
            now=lambda: 1000,
        )

        self.assertEqual(manager.access_token(), "access-from-secret")
        self.assertEqual(len(seen_urls), 2)

    def test_post_form_sends_oauth_params_in_request_body(self) -> None:
        seen: dict[str, Any] = {}

        class FakeResponse:
            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, *_args: Any) -> None:
                return None

            def read(self) -> bytes:
                return b'{"code":200,"data":{"access_token":"a","refresh_token":"r","expires_in":7200}}'

        def fake_urlopen(request: Any, timeout: int) -> FakeResponse:
            seen["url"] = request.full_url
            seen["data"] = request.data
            seen["content_type"] = request.get_header("Content-type")
            seen["timeout"] = timeout
            return FakeResponse()

        with patch("merino_erp_jobs.lingxing.urlopen", fake_urlopen):
            post_form(
                "https://openapi.lingxing.com/api/auth-server/oauth/access-token",
                {"appId": "app", "appSecret": "secret"},
                None,
            )

        self.assertEqual(seen["url"], "https://openapi.lingxing.com/api/auth-server/oauth/access-token")
        self.assertEqual(seen["data"], b"appId=app&appSecret=secret")
        self.assertEqual(seen["content_type"], "application/x-www-form-urlencoded")
        self.assertEqual(seen["timeout"], 60)

    def test_client_pages_until_short_page_when_total_missing(self) -> None:
        calls: list[dict[str, Any]] = []

        def request_json(url: str, params: dict[str, Any], body: dict[str, Any] | None) -> dict[str, Any]:
            del url, params
            assert body is not None
            calls.append(body)
            offset = int(body["offset"])
            rows = [{"id": offset + 1}, {"id": offset + 2}] if offset == 0 else [{"id": offset + 1}]
            return {"code": 1, "data": {"list": rows}, "total": 0}

        client = LingXingOpenApi(
            host="https://openapi.lingxing.com",
            app_key="1234567890abcdef",
            access_token="token",
            request_json=request_json,
            sleep=lambda seconds: None,
            clock=lambda: 100,
        )

        self.assertEqual(client.fetch_all("/endpoint", {"sid": "1"}, page_size=2), [{"id": 1}, {"id": 2}, {"id": 3}])
        self.assertEqual([call["offset"] for call in calls], [0, 2])

    def test_client_accepts_data_array_response(self) -> None:
        def request_json(url: str, params: dict[str, Any], body: dict[str, Any] | None) -> dict[str, Any]:
            del url, params, body
            return {"code": 0, "data": [{"wid": 13345, "name": "梦迪仓库"}], "total": 1}

        client = LingXingOpenApi(
            host="https://openapi.lingxing.com",
            app_key="1234567890abcdef",
            access_token="token",
            request_json=request_json,
            sleep=lambda seconds: None,
            clock=lambda: 100,
        )

        self.assertEqual(
            client.fetch_all("/erp/sc/data/local_inventory/warehouse", {"type": 1}, page_size=1000),
            [{"wid": 13345, "name": "梦迪仓库"}],
        )

    def test_parse_store_ids_accepts_comma_separated_values(self) -> None:
        self.assertEqual(parse_store_ids(["1, 2", "3"]), ["1", "2", "3"])

    def test_fba_store_ids_default_includes_all_configured_marketplaces(self) -> None:
        store_ids = fba_store_ids("")
        self.assertEqual(len(store_ids), 22)
        self.assertEqual(store_ids[0], 8793)
        self.assertEqual(store_ids[-1], 14482)
        self.assertIn("8803", DEFAULT_FBA_STORE_IDS)
        self.assertIn("11986", DEFAULT_FBA_STORE_IDS)

    def test_warehouse_code_maps_key_local_warehouses(self) -> None:
        self.assertEqual(warehouse_code("梦迪仓库"), "mengdi")
        self.assertEqual(warehouse_code("独立站"), "independent_site_fbm")


def load_dag_module():
    airflow_module = types.ModuleType("airflow")
    pendulum_module = types.ModuleType("pendulum")
    sdk_module = types.ModuleType("airflow.sdk")

    def fake_dag(**_kwargs):
        def decorate(func):
            return func

        return decorate

    def fake_task(func):
        def task_call(*_args, **_kwargs):
            return None

        task_call.python_callable = func
        return task_call

    class FakeVariable:
        @staticmethod
        def get(_key: str, default_var: str = "") -> str:
            return default_var

        @staticmethod
        def set(_key: str, _value: str) -> None:
            return None

    sdk_module.Variable = FakeVariable
    sdk_module.dag = fake_dag
    sdk_module.task = fake_task
    sys.modules["airflow"] = airflow_module
    pendulum_module.datetime = lambda *_args, **_kwargs: None
    sys.modules["pendulum"] = pendulum_module
    sys.modules["airflow.sdk"] = sdk_module

    spec = importlib.util.spec_from_file_location(
        "lingxing_erp_logistics_import_dag_for_test",
        REPO / "airflow" / "dags" / "lingxing_erp_logistics_import.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class LingXingDagTest(unittest.TestCase):
    def test_dag_schedule_and_token_cache_variable(self) -> None:
        module = load_dag_module()
        source = (REPO / "airflow" / "dags" / "lingxing_erp_logistics_import.py").read_text(encoding="utf-8")

        self.assertEqual(module.DAG_ID, "lingxing_erp_logistics_import")
        self.assertEqual(module.TOKEN_CACHE_VARIABLE, "erp_lingxing_oauth_cache")
        self.assertEqual(module.POSTGRES_CONN_ID, "merino_analytics")
        self.assertEqual(module.ERP_LOGISTICS_DB, "merino-analytics")
        self.assertIn('schedule="30 */4 * * *"', source)
        self.assertIn("max_active_runs=1", source)
        self.assertEqual(module.DEFAULT_STOCK_ENDPOINT, "/erp/sc/routing/data/local_inventory/inventoryDetails")
        self.assertEqual(module.DEFAULT_PRODUCT_ENDPOINT, "/erp/sc/routing/data/local_inventory/productList")
        self.assertEqual(module.DEFAULT_WAREHOUSE_NAMES, "梦迪仓库,独立站,SH-Blue")
        self.assertEqual(module.DEFAULT_FBA_STORE_IDS.count(","), 21)

    def test_selected_store_ids_defaults_to_configured_fba_stores(self) -> None:
        module = load_dag_module()

        self.assertEqual(
            module.selected_store_ids({}, {9999: "Other Store"}),
            fba_store_ids(module.DEFAULT_FBA_STORE_IDS),
        )

    def test_page_size_uses_default_for_bad_values(self) -> None:
        module = load_dag_module()

        self.assertEqual(module.page_size({"page_size": "bad"}), module.DEFAULT_PAGE_SIZE)
        self.assertEqual(module.page_size({"page_size": "1000"}), module.MAX_PAGE_SIZE)

    def test_stock_spu_rows_use_stock_list_endpoint(self) -> None:
        module = load_dag_module()
        calls: list[tuple[str, dict[str, Any], list[str], int]] = []

        class FakeClient:
            def fetch_all_sids(
                self,
                endpoint: str,
                params: dict[str, Any],
                store_ids: list[str],
                *,
                page_size: int,
            ) -> list[dict[str, Any]]:
                calls.append((endpoint, params, store_ids, page_size))
                return [{"product_id": 243225, "sku": "10004", "spu": "MT01"}]

        rows = module.stock_spu_rows(FakeClient(), {}, [101, 102], page_size=500)

        self.assertEqual(rows, [{"product_id": 243225, "sku": "10004", "spu": "MT01"}])
        self.assertEqual(calls[0][0], module.DEFAULT_STOCK_LIST_ENDPOINT)
        self.assertEqual(calls[0][1]["sort_field"], "sku")
        self.assertEqual(calls[0][1]["is_hide_zero_stock"], 0)
        self.assertEqual(calls[0][2], ["101", "102"])

    def test_adds_spu_from_stock_list_rows(self) -> None:
        module = load_dag_module()
        stock_rows = [
            {"product_id": 243225, "sku": "10004", "name": "梦迪仓库", "product_total": 12},
            {"product_id": 243245, "sku": "10012", "name": "独立站", "product_total": 5},
        ]
        stock_list_rows = [
            {
                "product_id": 243225,
                "sku": "10004",
                "seller_sku": "MT10004",
                "spu": "MT01",
                "spu_name": "男士圆领短袖",
                "product_name": "男士圆领短袖",
                "product_brand_text": "Merino Protect",
                "category_text": "MT01\\浅麻灰\\XXL",
            }
        ]

        enriched = module.stock_rows_with_spu(stock_rows, stock_list_rows)

        self.assertEqual(enriched[0]["spu"], "MT01")
        self.assertEqual(enriched[0]["spu_name"], "男士圆领短袖")
        self.assertEqual(enriched[0]["seller_sku"], "MT10004")
        self.assertEqual(enriched[1].get("spu"), None)
        self.assertNotIn("spu", stock_rows[0])

    def test_adds_model_from_product_list_rows(self) -> None:
        module = load_dag_module()
        stock_row = {"product_id": 243225, "sku": "10004"}

        module.add_missing_product_fields(
            stock_row,
            {"product_id": 243225, "sku": "10004", "model": "ZS05"},
        )

        self.assertEqual(stock_row["model"], "ZS05")

    def test_scheduled_import_fetches_product_list(self) -> None:
        module = load_dag_module()
        calls: list[tuple[str, dict[str, Any], int]] = []

        class FakeClient:
            def fetch_all(
                self,
                endpoint: str,
                params: dict[str, Any],
                *,
                page_size: int,
            ) -> list[dict[str, Any]]:
                calls.append((endpoint, params, page_size))
                return [{"id": 243225, "sku": "10004", "model": "ZS05"}]

        rows, source = module.product_rows(FakeClient(), {}, page_size=500)

        self.assertEqual(rows[0]["model"], "ZS05")
        self.assertEqual(calls, [(module.DEFAULT_PRODUCT_ENDPOINT, {}, 500)])
        self.assertEqual(source, f"lingxing-api:{module.DEFAULT_PRODUCT_ENDPOINT}")

    def test_upsert_product_writes_model(self) -> None:
        calls: list[tuple[str, tuple[Any, ...]]] = []

        class Result:
            def fetchone(self) -> dict[str, int]:
                return {"product_id": 243225}

        class FakeConnection:
            def execute(self, sql: str, params: tuple[Any, ...]) -> Result:
                calls.append((sql, params))
                return Result()

        product_id = upsert_product(
            FakeConnection(),  # type: ignore[arg-type]
            sku="10004",
            seller_sku=None,
            spu="MT01",
            spu_name=None,
            product_name="Merino shirt",
            brand="Merino Protect",
            category="Shirts",
            model="ZS05",
            api_product_id=243225,
        )

        self.assertEqual(product_id, 243225)
        insert_sql, params = calls[-1]
        self.assertIn("model", insert_sql)
        self.assertIn("model = coalesce(excluded.model, product.model)", insert_sql)
        self.assertIn("ZS05", params)

    def test_product_catalog_updates_existing_sku_when_api_omits_id(self) -> None:
        calls: list[tuple[str, tuple[Any, ...]]] = []

        class Result:
            def fetchone(self) -> dict[str, int]:
                return {"product_id": 243225}

        class FakeConnection:
            def execute(self, sql: str, params: tuple[Any, ...]) -> Result:
                calls.append((sql, params))
                return Result()

        product_id = update_product_model(
            FakeConnection(),  # type: ignore[arg-type]
            {"sku": "10004", "model": "ZS05"},
        )

        self.assertEqual(product_id, 243225)
        update_sql, params = calls[0]
        self.assertIn("update product", update_sql)
        self.assertIn("model = %s", update_sql)
        self.assertEqual(params, ("ZS05", "10004", "ZS05"))

    def test_model_backfill_script_triggers_products_only_run(self) -> None:
        script = REPO / "airflow" / "backfill_lingxing_product_models.sh"
        result = subprocess.run(
            ["bash", str(script), "--dry-run"],
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.returncode, 0)
        self.assertIn("lingxing_erp_logistics_import", result.stdout)
        self.assertIn('"products_only":true', result.stdout)
        self.assertIn('"skip_order_profit":true', result.stdout)

    def test_lingxing_dags_allow_only_one_active_run(self) -> None:
        for dag_file in (
            "lingxing_erp_logistics_import.py",
            "lingxing_erp_amazon_sales_import.py",
            "lingxing_erp_storage_fee_import.py",
        ):
            source = (REPO / "airflow" / "dags" / dag_file).read_text(encoding="utf-8")
            self.assertIn("max_active_runs=1", source, dag_file)

    def test_stock_warehouses_match_all_warehouse_types(self) -> None:
        module = load_dag_module()
        calls: list[int] = []

        class FakeClient:
            def fetch_all(self, endpoint: str, params: dict[str, Any], *, page_size: int) -> list[dict[str, Any]]:
                del endpoint, page_size
                calls.append(int(params["type"]))
                if params["type"] == 1:
                    return [{"wid": 13345, "name": "梦迪仓库", "type": 1}]
                if params["type"] == 3:
                    return [{"wid": 99999, "name": "SH-Blue", "type": 3}]
                return []

        warehouses = module.stock_warehouses(
            FakeClient(),
            {"warehouse_names": "梦迪仓库,SH-Blue"},
            page_size=500,
        )

        self.assertEqual(sorted(row["name"] for row in warehouses), ["SH-Blue", "梦迪仓库"])
        self.assertEqual(calls, [1, 3, 4, 6])

    def test_existing_stock_spu_is_not_overwritten(self) -> None:
        module = load_dag_module()
        stock_rows = [{"product_id": 1, "sku": "SKU1", "spu": "EXISTING", "spu_name": "Existing"}]
        stock_list_rows = [{"product_id": 1, "sku": "SKU1", "spu": "ERP", "spu_name": "ERP Name"}]

        enriched = module.stock_rows_with_spu(stock_rows, stock_list_rows)

        self.assertEqual(enriched[0]["spu"], "EXISTING")
        self.assertEqual(enriched[0]["spu_name"], "Existing")

    def test_logistics_dag_persists_fba_stock_rows(self) -> None:
        dag_source = (REPO / "airflow" / "dags" / "lingxing_erp_logistics_import.py").read_text(encoding="utf-8")
        import_source = (REPO / "airflow" / "dags" / "merino_erp_jobs" / "logistics_import.py").read_text(encoding="utf-8")

        self.assertIn("fba_stock_rows=fba_stock_rows", dag_source)
        self.assertIn("fba_stock_source=fba_stock_source", dag_source)
        self.assertIn("def import_fba_stock_rows", import_source)
        self.assertIn('source_object="fbm_stock"', import_source)
        self.assertIn('source_object="fba_stock"', import_source)
        self.assertIn("def mark_current_import_batch", import_source)
        self.assertIn("current_import_batch", import_source)
        self.assertIn("raw_lingxing_fba_stock", import_source)

    def test_erp_logistics_schema_exposes_fba_and_age_views(self) -> None:
        schema = (
            REPO / "metabase_schema" / "schema" / "merino-analytics" / "erp_logistics" / "erp_logistics.sql"
        ).read_text(encoding="utf-8")
        local_age_view = schema.split("CREATE OR REPLACE VIEW v_local_inventory_age_by_sku_warehouse", 1)[1].split(
            "CREATE OR REPLACE VIEW v_fba_stock_latest_by_seller_sku", 1
        )[0]
        fba_stock_view = schema.split("CREATE OR REPLACE VIEW v_fba_stock_latest_by_seller_sku", 1)[1].split(
            "CREATE OR REPLACE VIEW v_inventory_latest_by_warehouse_summary", 1
        )[0]

        self.assertIn("CREATE TABLE IF NOT EXISTS current_import_batch", schema)
        self.assertIn("CREATE TABLE IF NOT EXISTS raw_lingxing_fba_stock", schema)
        self.assertIn("idx_raw_lingxing_fbm_stock_import_run", schema)
        self.assertIn("idx_raw_lingxing_fba_stock_import_run", schema)
        self.assertIn("CREATE OR REPLACE VIEW v_fba_stock_latest_by_seller_sku", schema)
        self.assertIn("CREATE OR REPLACE VIEW v_local_inventory_age_by_sku_warehouse", schema)
        self.assertIn("fba_total_inventory", schema)
        self.assertIn("age_0_15_days", schema)
        self.assertIn("JOIN current_import_batch", local_age_view)
        self.assertIn("b.source_object = 'fbm_stock'", local_age_view)
        self.assertIn("b.import_run_id = r.import_run_id", local_age_view)
        self.assertIn("JOIN current_import_batch", fba_stock_view)
        self.assertIn("b.source_object = 'fba_stock'", fba_stock_view)
        self.assertIn("b.import_run_id = r.import_run_id", fba_stock_view)
        self.assertNotIn("MAX(snapshot_at)", local_age_view)
        self.assertNotIn("MAX(snapshot_at)", fba_stock_view)

    def test_order_profit_parser_maps_platform_order_name_and_costs(self) -> None:
        from merino_erp_jobs.order_profit_import import parse_mp_order_profit_records  # noqa: E402

        row = {
            "global_order_no": "103714945552109279",
            "store_id": "110494657014485504",
            "store_name": "MT shopify",
            "amount_currency": "USD",
            "global_purchase_time": 1782249652,
            "platform_info": [
                {
                    "platform_order_no": "7204044439863",
                    "platform_order_name": "#9732",
                    "purchase_time": 1782249652,
                }
            ],
            "item_info": [
                {
                    "platform_order_no": "7204044439863",
                    "cg_price_amount": "690.700000",
                    "transaction_fee_amount": "12.340000",
                }
            ],
            "transaction_info": [
                {
                    "cg_price_amount": "-￥690.700000",
                    "transaction_fee_amount": "$0.000000",
                }
            ],
        }

        records = parse_mp_order_profit_records(row)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].platform_order_name, "#9732")
        self.assertEqual(records[0].platform_order_no, "7204044439863")
        self.assertEqual(str(records[0].purchase_cost), "690.700000")
        self.assertEqual(str(records[0].platform_fee), "12.340000")
        self.assertEqual(records[0].currency_code, "USD")

    def test_order_profit_money_amount_strips_currency_symbols(self) -> None:
        from merino_erp_jobs.order_profit_import import money_amount  # noqa: E402

        self.assertEqual(str(money_amount("-￥690.700000")), "-690.700000")
        self.assertEqual(str(money_amount("$12.340000")), "12.340000")
        self.assertEqual(str(money_amount(None)), "0")

    def test_order_profit_period_chunks_split_by_calendar_month(self) -> None:
        from datetime import date

        from merino_erp_jobs.order_profit_import import period_chunks  # noqa: E402

        chunks = period_chunks(date(2026, 6, 1), date(2026, 7, 9))
        self.assertEqual(chunks, [(date(2026, 6, 1), date(2026, 7, 1)), (date(2026, 7, 1), date(2026, 7, 9))])

    def test_order_profit_dag_uses_mp_order_list_endpoint(self) -> None:
        source = (REPO / "airflow" / "dags" / "lingxing_erp_logistics_import.py").read_text(encoding="utf-8")
        import_source = (REPO / "airflow" / "dags" / "merino_erp_jobs" / "order_profit_import.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("import_lingxing_order_profit", source)
        self.assertIn("/pb/mp/order/list", source)
        self.assertIn("should_run_order_profit", source)
        self.assertIn("max_active_runs=1", source)
        self.assertIn("erp_purchase_cost", import_source)
        self.assertIn("raw_lingxing_order_profit", import_source)
        self.assertIn('source_object="order_profit"', import_source)

    def test_erp_logistics_schema_exposes_order_profit_raw_table(self) -> None:
        schema = (
            REPO / "metabase_schema" / "schema" / "merino-analytics" / "erp_logistics" / "erp_logistics.sql"
        ).read_text(encoding="utf-8")
        shopify_schema = (
            REPO / "metabase_schema" / "schema" / "merino-analytics" / "shopify" / "shopify_order.sql"
        ).read_text(encoding="utf-8")

        self.assertIn("CREATE TABLE IF NOT EXISTS raw_lingxing_order_profit", schema)
        self.assertIn("platform_order_name", schema)
        self.assertIn("erp_purchase_cost", shopify_schema)
        self.assertIn("erp_platform_fee", shopify_schema)

