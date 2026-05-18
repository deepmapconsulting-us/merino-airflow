"""Small Meta Graph API client for Merino batch jobs."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
from collections.abc import Iterator, Mapping
from typing import Any

import requests

GRAPH_API_VERSION = "v24.0"
GRAPH_API_BASE = f"https://graph.facebook.com/{GRAPH_API_VERSION}"
USER_AGENT = "merino-meta-jobs/1.0"
DEFAULT_TIMEOUT_SECONDS = 30

logger = logging.getLogger(__name__)


class MetaGraphError(RuntimeError):
    """Raised when the Meta Graph API returns an error response."""

    def __init__(self, endpoint: str, error: Mapping[str, Any]) -> None:
        self.endpoint = endpoint
        self.error = dict(error)
        message = self.error.get("message") or json.dumps(self.error, sort_keys=True)
        super().__init__(f"Meta Graph API error for {endpoint}: {message}")


def ensure_act_prefix(account_id: str) -> str:
    """Return an ad account id with Meta's required `act_` prefix."""
    if account_id and not account_id.startswith("act_"):
        return f"act_{account_id}"
    return account_id


def access_token_from_env() -> str:
    """Read the Meta access token injected into the Airflow runtime."""
    token = os.environ.get("META_ACCESS_TOKEN", "").strip()
    if not token:
        raise RuntimeError("META_ACCESS_TOKEN is required for Meta Graph API calls")
    return token


class MetaGraphClient:
    """Thin requests-based client for Graph API reads used by Airflow jobs."""

    def __init__(
        self,
        access_token: str,
        *,
        app_secret: str | None = None,
        session: requests.Session | None = None,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        if not access_token:
            raise ValueError("access_token is required")

        self.access_token = access_token
        self.app_secret = app_secret if app_secret is not None else os.environ.get("META_APP_SECRET", "")
        self.session = session or requests.Session()
        self.timeout_seconds = timeout_seconds

    def get(self, endpoint: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Fetch one Graph API endpoint and return the decoded JSON response."""
        response = self.session.get(
            f"{GRAPH_API_BASE}/{endpoint.lstrip('/')}",
            params=self._query_params(params),
            headers={"User-Agent": USER_AGENT},
            timeout=self.timeout_seconds,
        )
        self._log_rate_limit_headers(endpoint, response.headers)

        try:
            payload = response.json()
        except ValueError as exc:
            response.raise_for_status()
            raise MetaGraphError(endpoint, {"message": f"Non-JSON Graph response: {response.text[:200]}"}) from exc

        if response.status_code >= 400 or "error" in payload:
            error = payload.get("error", payload)
            if isinstance(error, Mapping):
                raise MetaGraphError(endpoint, error)
            raise MetaGraphError(endpoint, {"message": str(error)})

        return payload

    def get_all(self, endpoint: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        """Fetch all pages from a Graph API list endpoint."""
        rows: list[dict[str, Any]] = []
        for page in self.pages(endpoint, params):
            page_rows = page.get("data", [])
            if not isinstance(page_rows, list):
                raise MetaGraphError(endpoint, {"message": "Expected list response in Graph `data` field"})
            rows.extend(row for row in page_rows if isinstance(row, dict))
        return rows

    def pages(self, endpoint: str, params: Mapping[str, Any] | None = None) -> Iterator[dict[str, Any]]:
        """Yield each page for a Graph API list endpoint."""
        page = self.get(endpoint, params)
        while True:
            yield page

            cursors = page.get("paging", {}).get("cursors", {})
            after = cursors.get("after") if isinstance(cursors, Mapping) else None
            if not after:
                return

            next_params = dict(params or {})
            next_params["after"] = after
            page = self.get(endpoint, next_params)

    def _query_params(self, params: Mapping[str, Any] | None) -> dict[str, Any]:
        query: dict[str, Any] = {}
        for key, value in (params or {}).items():
            if isinstance(value, (dict, list)):
                query[key] = json.dumps(value)
            else:
                query[key] = value

        query["access_token"] = self.access_token
        if self.app_secret:
            query["appsecret_proof"] = hmac.new(
                self.app_secret.encode("utf-8"),
                self.access_token.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()

        return query

    def _log_rate_limit_headers(self, endpoint: str, headers: Mapping[str, str]) -> None:
        usage_headers = {
            "x-app-usage": headers.get("x-app-usage"),
            "x-business-use-case-usage": headers.get("x-business-use-case-usage"),
            "x-ad-account-usage": headers.get("x-ad-account-usage"),
        }
        present = {key: value for key, value in usage_headers.items() if value}
        if present:
            logger.info("meta_rate_limit_usage endpoint=%s headers=%s", endpoint, present)
