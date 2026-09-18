from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from merino_amazon_jobs import source_cli


def test_brand_analytics_requests_midnight_period_boundaries() -> None:
    marketplace = SimpleNamespace(
        code="US",
        marketplace_id="ATVPDKIKX0DER",
        region="NA",
        credential_group="NA",
    )
    store = MagicMock()
    store.start_run.return_value = 42
    store.write_brand_analytics.return_value = 0
    reports = MagicMock()
    reports.download.return_value = SimpleNamespace(
        content=b'{"dataByAsin":[]}',
        report_id="report-1",
        report_document_id="document-1",
    )

    with (
        patch.object(source_cli, "_store", return_value=(marketplace, store)),
        patch.object(source_cli, "reports_api"),
        patch.object(source_cli, "SpApiReports", return_value=reports),
    ):
        result = source_cli._brand_analytics_main(
            [
                "--marketplace",
                "US",
                "--start-date",
                "2026-09-06",
                "--end-date",
                "2026-09-12",
                "--period",
                "WEEK",
            ]
        )

    assert result == 0
    assert reports.download.call_args.kwargs["start_time"] == datetime(
        2026, 9, 6, tzinfo=timezone.utc
    )
    assert reports.download.call_args.kwargs["end_time"] == datetime(
        2026, 9, 12, tzinfo=timezone.utc
    )
