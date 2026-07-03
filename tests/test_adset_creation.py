from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs.adset_creation import (  # noqa: E402  # type: ignore[import-not-found]
    AdsetCreationRequest,
    adset_create_payload,
    create_planned_adsets,
    merge_targeting,
)


def sample_request() -> AdsetCreationRequest:
    return AdsetCreationRequest(
        queue_id=7,
        planned_date=date(2026, 7, 2),
        source_account_id="act_123",
        campaign_id="camp_1",
        campaign_name="Campaign",
        adset_name="West - Women - Interest Group4",
        region_code="West",
        demographic_code="women",
        demographic_name="Women",
        interest_group_code="group4",
        interest_group_name="Interest Group4",
        targeting={"age_min": 25, "geo_locations": {"countries": ["US"]}},
        daily_budget=2500,
        lifetime_budget=None,
        optimization_goal="OFFSITE_CONVERSIONS",
        billing_event="IMPRESSIONS",
        bid_strategy="LOWEST_COST_WITHOUT_CAP",
        desired_meta_status="PAUSED",
    )


class FakeCursor:
    def __init__(self, statements: list[tuple[str, tuple[object, ...]]]) -> None:
        self.statements = statements

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.statements.append((sql, params))


class FakeConn:
    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple[object, ...]]] = []

    def cursor(self) -> FakeCursor:
        return FakeCursor(self.statements)


class FakeClient:
    def __init__(self, existing: list[dict[str, object]] | None = None) -> None:
        self.existing = existing or []
        self.posts: list[tuple[str, dict[str, object]]] = []

    def get_all(self, _endpoint: str, _params: dict[str, object]) -> list[dict[str, object]]:
        return self.existing

    def post(self, endpoint: str, data: dict[str, object]) -> dict[str, object]:
        self.posts.append((endpoint, data))
        return {"id": "new_adset"}


class AdsetCreationTest(unittest.TestCase):
    def test_merge_targeting_applies_queue_override(self) -> None:
        merged = merge_targeting(
            {"age_min": 25, "geo_locations": {"countries": ["US"]}},
            {"age_min": 30},
        )

        self.assertEqual(merged["age_min"], 30)
        self.assertEqual(merged["geo_locations"], {"countries": ["US"]})

    def test_adset_create_payload_uses_template_config(self) -> None:
        payload = adset_create_payload(sample_request())

        self.assertEqual(payload["name"], "West - Women - Interest Group4")
        self.assertEqual(payload["campaign_id"], "camp_1")
        self.assertEqual(payload["daily_budget"], 2500)
        self.assertEqual(payload["status"], "PAUSED")
        self.assertEqual(payload["optimization_goal"], "OFFSITE_CONVERSIONS")
        self.assertNotIn("lifetime_budget", payload)

    def test_existing_adset_marks_queue_without_post(self) -> None:
        conn = FakeConn()
        client = FakeClient(existing=[{"id": "existing_adset", "name": sample_request().adset_name, "status": "ACTIVE"}])
        with patch(
            "merino_meta_jobs.adset_creation.load_pending_requests",
            return_value=[sample_request()],
        ):
            result = create_planned_adsets(conn, client, planned_date="2026-07-02")

        self.assertEqual(result["exists"], 1)
        self.assertEqual(result["created"], 0)
        self.assertEqual(client.posts, [])
        self.assertTrue(any(params[0] == "exists" for _sql, params in conn.statements))

    def test_missing_adset_posts_and_marks_created(self) -> None:
        conn = FakeConn()
        client = FakeClient()
        with patch(
            "merino_meta_jobs.adset_creation.load_pending_requests",
            return_value=[sample_request()],
        ):
            result = create_planned_adsets(conn, client, planned_date="2026-07-02")

        self.assertEqual(result["created"], 1)
        self.assertEqual(client.posts[0][0], "act_123/adsets")
        self.assertTrue(any(params[0] == "created" for _sql, params in conn.statements))


if __name__ == "__main__":
    unittest.main()
