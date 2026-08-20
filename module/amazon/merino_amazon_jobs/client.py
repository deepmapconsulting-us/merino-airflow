from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

REFRESH_TOKEN_ENV = {
    "NA": ("SP_API_NA_REFRESH_TOKEN", "sp_api_na_refresh_token"),
    "OC": ("SP_API_OC_REFRESH_TOKEN", "sp_api_oc_refresh_token"),
}
SDK_REGION_BY_CREDENTIAL_GROUP = {"NA": "NA", "OC": "FE"}


def _api_client(
    region: str,
    credential_group: str,
    environ: Mapping[str, str] | None = None,
) -> Any:
    environ = environ or os.environ
    expected_region = SDK_REGION_BY_CREDENTIAL_GROUP.get(credential_group)
    if expected_region != region:
        raise ValueError(
            f"credential group {credential_group!r} does not serve SP-API region {region!r}"
        )
    client_id = _credential(environ, "SP_API_CLIENT_ID", "sp_api_client_id")
    client_secret = _credential(
        environ,
        "SP_API_CLIENT_SECRET",
        "sp_api_client_secret",
    )
    refresh_token = _refresh_token(environ, credential_group)

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


def reports_api(
    region: str,
    credential_group: str,
    environ: Mapping[str, str] | None = None,
) -> Any:
    from spapi.api.reports_v2021_06_30.reports_api import ReportsApi

    return ReportsApi(_api_client(region, credential_group, environ))


def fba_inventory_api(
    region: str,
    credential_group: str,
    environ: Mapping[str, str] | None = None,
) -> Any:
    from spapi.api.fba_inventory_v1.fba_inventory_api import FbaInventoryApi

    return FbaInventoryApi(_api_client(region, credential_group, environ))


def search_orders_api(
    region: str,
    credential_group: str,
    environ: Mapping[str, str] | None = None,
) -> Any:
    from spapi.api.orders_v2026_01_01.search_orders_api import SearchOrdersApi

    return SearchOrdersApi(_api_client(region, credential_group, environ))


def sellers_api(
    region: str,
    credential_group: str,
    environ: Mapping[str, str] | None = None,
) -> Any:
    from spapi.api.sellers_v1.sellers_api import SellersApi

    return SellersApi(_api_client(region, credential_group, environ))


def _refresh_token(environ: Mapping[str, str], credential_group: str) -> str:
    try:
        uppercase, lowercase = REFRESH_TOKEN_ENV[credential_group]
    except KeyError as exc:
        raise ValueError(
            f"unsupported SP-API credential group {credential_group!r}"
        ) from exc
    return _credential(environ, uppercase, lowercase)


def _credential(
    environ: Mapping[str, str],
    uppercase: str,
    lowercase: str,
) -> str:
    value = environ.get(uppercase) or environ.get(lowercase)
    if not value:
        raise RuntimeError(f"missing {uppercase} (or {lowercase})")
    return value
