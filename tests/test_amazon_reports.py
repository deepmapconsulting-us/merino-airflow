from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from merino_amazon_jobs.reports import ReportFailed, SpApiReports


def test_cancelled_report_can_be_treated_as_empty() -> None:
    api = MagicMock()
    api.create_report.return_value = SimpleNamespace(report_id="report-1")
    api.get_report.return_value = SimpleNamespace(
        processing_status="CANCELLED",
        report_document_id=None,
    )

    with patch("merino_amazon_jobs.reports.wait_for_create_report"):
        document = SpApiReports(api, poll_seconds=0).download(
            report_type="GET_FBA_INVENTORY_PLANNING_DATA",
            marketplace_id="A1AM78C64UM0Y8",
            cancelled_as_empty=True,
        )

    assert document.content == b""
    assert document.report_id == "report-1"
    assert document.report_document_id is None
    api.get_report_document.assert_not_called()


def test_cancelled_report_still_fails_by_default() -> None:
    api = MagicMock()
    api.create_report.return_value = SimpleNamespace(report_id="report-1")
    api.get_report.return_value = SimpleNamespace(
        processing_status="CANCELLED",
        report_document_id=None,
    )

    with (
        patch("merino_amazon_jobs.reports.wait_for_create_report"),
        pytest.raises(ReportFailed, match="CANCELLED"),
    ):
        SpApiReports(api, poll_seconds=0).download(
            report_type="GET_FBA_INVENTORY_PLANNING_DATA",
            marketplace_id="A1AM78C64UM0Y8",
        )


def test_fatal_report_still_fails_when_cancelled_is_empty() -> None:
    api = MagicMock()
    api.create_report.return_value = SimpleNamespace(report_id="report-1")
    api.get_report.return_value = SimpleNamespace(
        processing_status="FATAL",
        report_document_id=None,
    )

    with (
        patch("merino_amazon_jobs.reports.wait_for_create_report"),
        pytest.raises(ReportFailed, match="FATAL"),
    ):
        SpApiReports(api, poll_seconds=0).download(
            report_type="GET_FBA_INVENTORY_PLANNING_DATA",
            marketplace_id="A1AM78C64UM0Y8",
            cancelled_as_empty=True,
        )
