from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class Marketplace:
    code: str
    marketplace_id: str
    region: str
    credential_group: str
    currency: str
    timezone: str


MARKETPLACES = {
    "US": Marketplace("US", "ATVPDKIKX0DER", "NA", "NA", "USD", "America/Los_Angeles"),
    "CA": Marketplace("CA", "A2EUQ1WTGCTBG2", "NA", "NA", "CAD", "America/Toronto"),
    "MX": Marketplace("MX", "A1AM78C64UM0Y8", "NA", "NA", "MXN", "America/Mexico_City"),
    "BR": Marketplace("BR", "A2Q3Y263D00KWC", "NA", "NA", "BRL", "America/Sao_Paulo"),
    "AU": Marketplace("AU", "A39IBJ37TRP1C6", "FE", "OC", "AUD", "Australia/Sydney"),
}


def marketplace(code: str) -> Marketplace:
    try:
        return MARKETPLACES[code.upper()]
    except KeyError as exc:
        supported = ", ".join(MARKETPLACES)
        raise ValueError(
            f"unsupported marketplace {code!r}; choose {supported}"
        ) from exc


def date_windows(
    start_date: date,
    end_date: date,
    *,
    days: int = 30,
) -> Iterator[tuple[date, date]]:
    if start_date > end_date:
        raise ValueError("start date must be on or before end date")
    if not 1 <= days <= 30:
        raise ValueError("window days must be between 1 and 30")

    window_start = start_date
    while window_start <= end_date:
        window_end = min(window_start + timedelta(days=days - 1), end_date)
        yield window_start, window_end
        window_start = window_end + timedelta(days=1)
