from __future__ import annotations

import gzip
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from merino_amazon_jobs.marketplaces import Marketplace
from merino_amazon_jobs.quota import (
    mark_create_report_throttled,
    retry_backoff_seconds,
    wait_for_create_report,
)

REPORT_TYPE = "GET_SALES_AND_TRAFFIC_REPORT"
FINISHED_STATUSES = {"DONE", "CANCELLED", "FATAL"}


@dataclass(frozen=True)
class DownloadedReport:
    payload: dict[str, Any]
    report_id: str
    report_document_id: str


@dataclass(frozen=True)
class DownloadedDocument:
    content: bytes
    report_id: str
    report_document_id: str


class ReportFailed(RuntimeError):
    def __init__(self, message: str, *, status: str = "FATAL") -> None:
        super().__init__(message)
        self.status = status


class SalesTrafficReports:
    def __init__(
        self,
        api: Any,
        *,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        poll_seconds: float = 30,
        max_polls: int = 120,
        api_attempts: int = 4,
    ) -> None:
        self.api = api
        self.session = session or _http_session()
        self.sleep = sleep
        self.poll_seconds = poll_seconds
        self.max_polls = max_polls
        self.api_attempts = api_attempts

    def download(
        self,
        marketplace: Marketplace,
        start_time: datetime,
        end_time: datetime,
        granularity: str,
        *,
        on_status: Callable[[str, str | None, str], None] | None = None,
    ) -> DownloadedReport:
        specification = _report_specification(
            marketplace,
            start_time,
            end_time,
            granularity,
        )
        created = self._call(self.api.create_report, specification)
        report_id = created.report_id
        if on_status:
            on_status(report_id, None, "requested")
        report = self._wait(report_id, on_status=on_status)
        document = self._call(
            self.api.get_report_document,
            report.report_document_id,
        )
        response = self.session.get(document.url, timeout=(10, 120))
        response.raise_for_status()
        content = response.content
        if (document.compression_algorithm or "").upper() == "GZIP":
            content = gzip.decompress(content)
        return DownloadedReport(
            payload=json.loads(content.decode("utf-8-sig")),
            report_id=report_id,
            report_document_id=report.report_document_id,
        )

    def _wait(
        self,
        report_id: str,
        *,
        on_status: Callable[[str, str | None, str], None] | None,
    ) -> Any:
        processing_notified = False
        for _ in range(self.max_polls):
            report = self._call(self.api.get_report, report_id)
            status = report.processing_status.upper()
            if on_status and (
                not processing_notified or report.report_document_id is not None
            ):
                on_status(
                    report_id,
                    report.report_document_id,
                    "processing",
                )
                processing_notified = True
            if status == "DONE":
                if not report.report_document_id:
                    raise ReportFailed(
                        f"report {report_id} completed without a document"
                    )
                return report
            if status in FINISHED_STATUSES:
                raise ReportFailed(
                    f"report {report_id} ended with status {status}",
                    status=status,
                )
            self.sleep(self.poll_seconds)
        raise TimeoutError(
            f"report {report_id} did not complete after {self.max_polls} polls"
        )

    def _call(self, method: Callable[..., Any], *args: Any) -> Any:
        for attempt in range(self.api_attempts):
            if self._is_create_report(method):
                report_type = ""
                if args:
                    report_type = str(getattr(args[0], "report_type", "") or "")
                wait_for_create_report(report_type, sleep=self.sleep)
            try:
                return method(*args)
            except Exception as exc:
                status = getattr(exc, "status", None)
                if (
                    status not in {429, 500, 502, 503, 504}
                    or attempt + 1 == self.api_attempts
                ):
                    raise
                delay = retry_backoff_seconds(attempt, status)
                if status == 429:
                    mark_create_report_throttled(delay)
                self.sleep(delay)
        raise AssertionError("unreachable")

    def _is_create_report(self, method: Callable[..., Any]) -> bool:
        if method is getattr(self.api, "create_report", None):
            return True
        return getattr(method, "__name__", "") == "create_report"


class SpApiReports:
    def __init__(self, api: Any, **kwargs: Any) -> None:
        self.lifecycle = SalesTrafficReports(api, **kwargs)

    def download(
        self,
        *,
        report_type: str,
        marketplace_id: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        report_options: dict[str, str] | None = None,
        on_status: Callable[[str, str | None, str], None] | None = None,
    ) -> DownloadedDocument:
        from spapi.models.reports_v2021_06_30 import CreateReportSpecification

        specification = CreateReportSpecification(
            report_type=report_type,
            marketplace_ids=[marketplace_id],
            data_start_time=start_time,
            data_end_time=end_time,
            report_options=report_options,
        )
        created = self.lifecycle._call(self.lifecycle.api.create_report, specification)
        report_id = created.report_id
        if on_status:
            on_status(report_id, None, "requested")
        report = self.lifecycle._wait(report_id, on_status=on_status)
        document = self.lifecycle._call(
            self.lifecycle.api.get_report_document,
            report.report_document_id,
        )
        response = self.lifecycle.session.get(document.url, timeout=(10, 120))
        response.raise_for_status()
        content = response.content
        if (document.compression_algorithm or "").upper() == "GZIP":
            content = gzip.decompress(content)
        return DownloadedDocument(
            content=content,
            report_id=report_id,
            report_document_id=report.report_document_id,
        )


def _report_specification(
    marketplace: Marketplace,
    start_time: datetime,
    end_time: datetime,
    granularity: str,
) -> Any:
    granularity = granularity.upper()
    if granularity not in {"PARENT", "CHILD", "SKU"}:
        raise ValueError("granularity must be PARENT, CHILD, or SKU")

    from spapi.models.reports_v2021_06_30 import CreateReportSpecification

    return CreateReportSpecification(
        report_options={
            "dateGranularity": "DAY",
            "asinGranularity": granularity,
        },
        report_type=REPORT_TYPE,
        data_start_time=start_time,
        data_end_time=end_time,
        marketplace_ids=[marketplace.marketplace_id],
    )


def _http_session() -> requests.Session:
    retry = Retry(
        total=4,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session
