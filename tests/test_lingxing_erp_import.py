from __future__ import annotations

import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str((REPO / "airflow" / "dags").resolve()))

from merino_erp_jobs.lingxing import (  # noqa: E402  # type: ignore[import-not-found]
    LingXingCredentials,
    LingXingOpenApi,
    LingXingTokenManager,
    canonical_params,
    lingxing_sign,
    parse_store_ids,
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

    def test_parse_store_ids_accepts_comma_separated_values(self) -> None:
        self.assertEqual(parse_store_ids(["1, 2", "3"]), ["1", "2", "3"])


def load_dag_module():
    airflow_module = types.ModuleType("airflow")
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
        self.assertEqual(module.ERP_LOGISTICS_DB, "erp_logistics")
        self.assertIn('schedule="30 */4 * * *"', source)

    def test_page_size_uses_default_for_bad_values(self) -> None:
        module = load_dag_module()

        self.assertEqual(module.page_size({"page_size": "bad"}), module.DEFAULT_PAGE_SIZE)

