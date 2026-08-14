from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any


def _api_client(region: str, environ: Mapping[str, str] | None = None) -> Any:
    environ = environ or os.environ
    client_id = _credential(environ, "SP_API_CLIENT_ID", "sp_api_client_id")
    client_secret = _credential(
        environ,
        "SP_API_CLIENT_SECRET",
        "sp_api_client_secret",
    )
    refresh_token = _credential(
        environ,
        "SP_API_REFRESH_TOKEN",
        "sp_api_refresh_token",
    )

    from spapi.auth.credentials import SPAPIConfig
    from spapi.client import SPAPIClient

    config = SPAPIConfig(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        region=region,
        scope=None,
    )
    return SPAPIClient(config).api_client


def reports_api(region: str, environ: Mapping[str, str] | None = None) -> Any:
    from spapi.api.reports_v2021_06_30.reports_api import ReportsApi

    return ReportsApi(_api_client(region, environ))


def fba_inventory_api(region: str, environ: Mapping[str, str] | None = None) -> Any:
    from spapi.api.fba_inventory_v1.fba_inventory_api import FbaInventoryApi

    return FbaInventoryApi(_api_client(region, environ))


def search_orders_api(region: str, environ: Mapping[str, str] | None = None) -> Any:
    from spapi.api.orders_v2026_01_01.search_orders_api import SearchOrdersApi

    return SearchOrdersApi(_api_client(region, environ))


def _credential(
    environ: Mapping[str, str],
    uppercase: str,
    lowercase: str,
) -> str:
    value = environ.get(uppercase) or environ.get(lowercase)
    if not value:
        raise RuntimeError(f"missing {uppercase} (or {lowercase})")
    return value
