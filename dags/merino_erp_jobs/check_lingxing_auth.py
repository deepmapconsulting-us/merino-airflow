#!/usr/bin/env python3
"""Check LingXing OAuth and signed OpenAPI connectivity without printing secrets."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from typing import Any
from urllib.error import HTTPError, URLError

from lingxing import (
    LINGXING_HOST,
    LingXingCredentials,
    LingXingOpenApi,
    LingXingToken,
    LingXingTokenManager,
)

CACHE_KEY = "erp_lingxing_oauth_cache"
DEFAULT_WAREHOUSE_ENDPOINT = "/erp/sc/data/local_inventory/warehouse"


class EnvVariableStore:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def get(self, key: str, default: str = "") -> str:
        if key in self.values:
            return self.values[key]
        for env_key in env_keys(key):
            value = os.environ.get(env_key)
            if value:
                return value
        return default

    def set(self, key: str, value: str) -> None:
        self.values[key] = value


class AirflowVariableStore:
    def __init__(self) -> None:
        try:
            from airflow.sdk import Variable  # type: ignore[import-not-found]
        except Exception as exc:  # pragma: no cover - depends on Airflow runtime
            raise RuntimeError("Airflow Variable is not available; use --source env instead") from exc

        self.variable = Variable

    def get(self, key: str, default: str = "") -> str:
        try:
            return str(self.variable.get(key))
        except Exception:
            return default

    def set(self, key: str, value: str) -> None:
        self.variable.set(key, value)


def env_keys(variable_key: str) -> list[str]:
    upper = variable_key.upper()
    keys = [upper]
    if variable_key.startswith("erp_"):
        keys.append(f"TF_VAR_{variable_key}")
        keys.append(f"TF_VAR_{upper}")
    return keys


def masked_presence(name: str, value: str) -> str:
    if not value:
        return f"{name}: missing"
    return f"{name}: present len={len(value)}"


def redacted(value: str) -> str:
    return re.sub(
        r'("(?:access_token|refresh_token|appSecret|app_secret)"\s*:\s*")[^"]+(")',
        r"\1***\2",
        value,
        flags=re.IGNORECASE,
    )


def exception_summary(exc: Exception) -> str:
    if isinstance(exc, HTTPError):
        try:
            body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        body = redacted(body.strip())
        return f"HTTP {exc.code} {exc.reason}" + (f": {body[:500]}" if body else "")
    if isinstance(exc, URLError):
        return f"URL error: {exc.reason}"
    return f"{type(exc).__name__}: {exc}"


def token_summary(token: LingXingToken) -> str:
    seconds_left = token.expires_at - int(time.time())
    return (
        f"access_token len={len(token.access_token)}, "
        f"refresh_token len={len(token.refresh_token)}, "
        f"expires_at={token.expires_at}, seconds_left={seconds_left}"
    )


def cached_token(store: Any) -> LingXingToken | None:
    raw = store.get(CACHE_KEY, "").strip()
    if not raw:
        return None
    try:
        return LingXingToken.from_payload(json.loads(raw))
    except Exception:
        return None


def credentials_from_store(store: Any) -> tuple[str, LingXingCredentials]:
    host = store.get("erp_lingxing_host", LINGXING_HOST).strip() or LINGXING_HOST
    app_id = store.get("erp_app_id", "").strip()
    app_secret = store.get("erp_app_secret", "").strip()
    app_key = store.get("erp_lingxing_app_key", app_id).strip() or app_id
    if not app_id or not app_secret:
        raise RuntimeError("Missing erp_app_id or erp_app_secret")
    return host, LingXingCredentials(app_id=app_id, app_secret=app_secret, app_key=app_key)


def check_api(host: str, credentials: LingXingCredentials, access_token: str, endpoint: str) -> None:
    client = LingXingOpenApi(host=host, app_key=credentials.app_key, access_token=access_token)
    response = client.post(endpoint, {"type": 1, "is_delete": 0, "offset": 0, "length": 1})
    data = response.get("data")
    row_count = len(data) if isinstance(data, list) else len(data.get("list", [])) if isinstance(data, dict) else 0
    print(f"OpenAPI check OK: endpoint={endpoint}, rows_returned={row_count}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=["env", "airflow"], default="env")
    parser.add_argument("--write-cache", action="store_true", help="Persist the successful token to erp_lingxing_oauth_cache")
    parser.add_argument("--skip-api-check", action="store_true", help="Only test OAuth; do not call a signed OpenAPI endpoint")
    parser.add_argument("--endpoint", default=DEFAULT_WAREHOUSE_ENDPOINT)
    args = parser.parse_args()

    store = AirflowVariableStore() if args.source == "airflow" else EnvVariableStore()
    host, credentials = credentials_from_store(store)

    print(f"host: {host}")
    print(masked_presence("erp_app_id", credentials.app_id))
    print(masked_presence("erp_app_secret", credentials.app_secret))
    print(masked_presence("erp_lingxing_app_key", credentials.app_key))

    manager = LingXingTokenManager(
        host=host,
        credentials=credentials,
        variable_store=store,
        cache_key=CACHE_KEY,
    )

    existing = cached_token(store)
    if existing:
        print(f"cache: present, fresh={existing.is_fresh()}, {token_summary(existing)}")
    else:
        print("cache: missing or invalid")

    access_token = ""
    refresh_seed = (existing.refresh_token if existing else "") or store.get("erp_refresh_token", "").strip()
    if refresh_seed:
        try:
            refreshed = manager.refresh_token(refresh_seed)
            print(f"RefreshToken OK: {token_summary(refreshed)}")
            access_token = refreshed.access_token
            if args.write_cache:
                store.set(CACHE_KEY, refreshed.to_json())
                print(f"cache written: {CACHE_KEY}")
        except Exception as exc:
            print(f"RefreshToken FAILED: {exception_summary(exc)}")
    else:
        print("RefreshToken skipped: no cached/seed refresh token")

    if not access_token:
        try:
            fetched = manager.fetch_token()
            print(f"GetToken OK: {token_summary(fetched)}")
            access_token = fetched.access_token
            if args.write_cache:
                store.set(CACHE_KEY, fetched.to_json())
                print(f"cache written: {CACHE_KEY}")
        except Exception as exc:
            print(f"GetToken FAILED: {exception_summary(exc)}")
            return 1

    if not args.skip_api_check:
        try:
            check_api(host, credentials, access_token, args.endpoint)
        except Exception as exc:
            print(f"OpenAPI check FAILED: {exception_summary(exc)}")
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
