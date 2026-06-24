"""LingXing OpenAPI client and Airflow token cache helpers."""

from __future__ import annotations

import base64
import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any, Callable, Protocol
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from Crypto.Cipher import AES  # type: ignore[import-not-found]

LINGXING_HOST = "https://openapi.lingxing.com"
ACCESS_TOKEN_ENDPOINT = "/api/auth-server/oauth/access-token"
REFRESH_TOKEN_ENDPOINT = "/api/auth-server/oauth/refresh"
DEFAULT_TOKEN_TTL_SECONDS = 7200
TOKEN_REFRESH_BUFFER_SECONDS = 300
DEFAULT_PAGE_DELAY_SECONDS = 1.05


class VariableStore(Protocol):
    def get(self, key: str, default: str = "") -> str: ...

    def set(self, key: str, value: str) -> None: ...


@dataclass(frozen=True)
class LingXingCredentials:
    app_id: str
    app_secret: str
    app_key: str


@dataclass(frozen=True)
class LingXingToken:
    access_token: str
    refresh_token: str
    expires_at: int

    def is_fresh(self, now: int | None = None, buffer_seconds: int = TOKEN_REFRESH_BUFFER_SECONDS) -> bool:
        now = int(time.time()) if now is None else now
        return bool(self.access_token and self.expires_at > now + buffer_seconds)

    def to_json(self) -> str:
        return json.dumps(
            {
                "access_token": self.access_token,
                "refresh_token": self.refresh_token,
                "expires_at": self.expires_at,
            },
            separators=(",", ":"),
        )

    @classmethod
    def from_payload(cls, payload: dict[str, Any], *, now: int | None = None) -> "LingXingToken":
        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        access_token = str(data.get("access_token") or "").strip()
        refresh_token = str(data.get("refresh_token") or "").strip()
        expires_at = integer(data.get("expires_at"))
        if expires_at is None:
            expires_in = integer(data.get("expires_in")) or DEFAULT_TOKEN_TTL_SECONDS
            expires_at = (int(time.time()) if now is None else now) + max(
                expires_in - TOKEN_REFRESH_BUFFER_SECONDS,
                60,
            )
        if not access_token or not refresh_token:
            raise ValueError(f"LingXing OAuth response missing token fields: {payload}")
        return cls(access_token=access_token, refresh_token=refresh_token, expires_at=expires_at)


class LingXingTokenManager:
    def __init__(
        self,
        *,
        host: str,
        credentials: LingXingCredentials,
        variable_store: VariableStore,
        cache_key: str,
        request_json: Callable[[str, dict[str, Any], dict[str, Any] | None], dict[str, Any]] | None = None,
        now: Callable[[], int] | None = None,
    ) -> None:
        self.host = host.rstrip("/")
        self.credentials = credentials
        self.variable_store = variable_store
        self.cache_key = cache_key
        self.request_json = request_json or post_form
        self.now = now or (lambda: int(time.time()))

    def access_token(self) -> str:
        cached = self.cached_token()
        if cached and cached.is_fresh(now=self.now()):
            return cached.access_token

        seed_refresh_token = cached.refresh_token if cached else self.variable_store.get("erp_refresh_token", "").strip()
        if seed_refresh_token:
            try:
                refreshed = self.refresh_token(seed_refresh_token)
                self.variable_store.set(self.cache_key, refreshed.to_json())
                return refreshed.access_token
            except Exception:
                pass

        fetched = self.fetch_token()
        self.variable_store.set(self.cache_key, fetched.to_json())
        return fetched.access_token

    def cached_token(self) -> LingXingToken | None:
        raw = self.variable_store.get(self.cache_key, "").strip()
        if not raw:
            raw_access_token = self.variable_store.get("erp_access_token", "").strip()
            raw_refresh_token = self.variable_store.get("erp_refresh_token", "").strip()
            if raw_access_token and raw_refresh_token:
                return LingXingToken(
                    access_token=raw_access_token,
                    refresh_token=raw_refresh_token,
                    expires_at=0,
                )
            return None
        try:
            return LingXingToken.from_payload(json.loads(raw), now=self.now())
        except (json.JSONDecodeError, ValueError, TypeError):
            return None

    def refresh_token(self, refresh_token: str) -> LingXingToken:
        payload = self.request_json(
            f"{self.host}{REFRESH_TOKEN_ENDPOINT}",
            {"appId": self.credentials.app_id, "refreshToken": refresh_token},
            None,
        )
        assert_success(payload, "LingXing OAuth refresh")
        return LingXingToken.from_payload(payload, now=self.now())

    def fetch_token(self) -> LingXingToken:
        payload = self.request_json(
            f"{self.host}{ACCESS_TOKEN_ENDPOINT}",
            {
                "appId": self.credentials.app_id,
                "appSecret": self.credentials.app_secret,
            },
            None,
        )
        assert_success(payload, "LingXing OAuth access-token")
        return LingXingToken.from_payload(payload, now=self.now())


class LingXingOpenApi:
    def __init__(
        self,
        *,
        host: str,
        app_key: str,
        access_token: str,
        request_json: Callable[[str, dict[str, Any], dict[str, Any] | None], dict[str, Any]] | None = None,
        sleep: Callable[[float], None] | None = None,
        clock: Callable[[], int] | None = None,
    ) -> None:
        self.host = host.rstrip("/")
        self.app_key = app_key
        self.access_token = access_token
        self.request_json = request_json or post_json
        self.sleep = sleep or time.sleep
        self.clock = clock or (lambda: int(time.time()))

    def fetch_all(
        self,
        endpoint: str,
        params: dict[str, Any],
        *,
        page_size: int,
        page_delay_seconds: float = DEFAULT_PAGE_DELAY_SECONDS,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            body = dict(params)
            body["offset"] = offset
            body["length"] = page_size
            response = self.post(endpoint, body)
            data = response.get("data") if isinstance(response, dict) else {}
            if isinstance(data, list):
                page = data
            else:
                page = data.get("list") if isinstance(data, dict) else None
            if not isinstance(page, list):
                page = []

            rows.extend(row for row in page if isinstance(row, dict))
            total = api_total(response, data)
            if not page:
                break
            if total > 0 and offset + page_size >= total:
                break
            if total <= 0 and len(page) < page_size:
                break
            offset += page_size
            self.sleep(page_delay_seconds)
        return rows

    def fetch_all_sids(
        self,
        endpoint: str,
        params: dict[str, Any],
        store_ids: list[str],
        *,
        page_size: int,
        page_delay_seconds: float = DEFAULT_PAGE_DELAY_SECONDS,
    ) -> list[dict[str, Any]]:
        if not store_ids:
            raise ValueError("LingXing stock fetch requires at least one sid.")

        rows: list[dict[str, Any]] = []
        for store_id in store_ids:
            scoped_params = dict(params)
            scoped_params["sid"] = store_id
            rows.extend(
                self.fetch_all(
                    endpoint,
                    scoped_params,
                    page_size=page_size,
                    page_delay_seconds=page_delay_seconds,
                )
            )
        return rows

    def post(self, endpoint: str, body: dict[str, Any]) -> dict[str, Any]:
        endpoint = endpoint if endpoint.startswith("/") else f"/{endpoint}"
        query = {
            "app_key": self.app_key,
            "access_token": self.access_token,
            "timestamp": str(self.clock()),
        }
        sign_params = dict(body)
        sign_params.update(query)
        query["sign"] = lingxing_sign(self.app_key, sign_params)
        payload = self.request_json(f"{self.host}{endpoint}", query, body)
        assert_success(payload, f"LingXing OpenAPI {endpoint}")
        return payload


def post_form(url: str, params: dict[str, Any], body: dict[str, Any] | None) -> dict[str, Any]:
    del body
    request = Request(
        f"{url}?{urlencode(params)}",
        data=b"",
        method="POST",
    )
    with urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode())


def post_json(url: str, params: dict[str, Any], body: dict[str, Any] | None) -> dict[str, Any]:
    request = Request(
        f"{url}?{urlencode(params)}",
        data=json.dumps(body or {}, ensure_ascii=False, separators=(",", ":")).encode(),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode())


def assert_success(payload: dict[str, Any], label: str) -> None:
    code = str(payload.get("code", ""))
    if code not in {"0", "1", "200"}:
        raise RuntimeError(f"{label} failed: {payload}")


def api_total(response: dict[str, Any], data: Any) -> int:
    for value in [
        response.get("total"),
        data.get("total") if isinstance(data, dict) else None,
    ]:
        parsed = integer(value)
        if parsed is not None:
            return parsed
    return 0


def lingxing_sign(app_key: str, params: dict[str, Any]) -> str:
    canonical = canonical_params(params)
    digest = hashlib.md5(canonical.encode()).hexdigest().upper()
    return aes_ecb_base64(app_key, digest)


def canonical_params(params: dict[str, Any]) -> str:
    pairs: list[str] = []
    for key in sorted(params):
        value = params[key]
        if value == "":
            continue
        if isinstance(value, (dict, list)):
            encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        else:
            encoded = str(value)
        pairs.append(f"{key}={encoded}")
    return "&".join(pairs)


def aes_ecb_base64(key: str, text_to_encrypt: str) -> str:
    raw = text_to_encrypt.encode()
    pad_size = AES.block_size - len(raw) % AES.block_size
    padded = raw + bytes([pad_size]) * pad_size
    encrypted = AES.new(key.encode(), AES.MODE_ECB).encrypt(padded)
    return base64.b64encode(encrypted).decode()


def parse_store_ids(raw: str | list[str]) -> list[str]:
    values = raw if isinstance(raw, list) else [raw]
    store_ids: list[str] = []
    for value in values:
        store_ids.extend(part.strip() for part in str(value).split(",") if part.strip())
    return store_ids


def integer(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None
