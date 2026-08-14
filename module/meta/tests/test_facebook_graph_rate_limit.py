from merino_meta_jobs import facebook_graph


class Response:
    def __init__(self, status_code: int, payload: dict) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers: dict[str, str] = {}
        self.text = ""

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        return None


class Session:
    def __init__(self) -> None:
        self.responses = [
            Response(429, {"error": {"code": 4, "message": "Application request limit reached"}}),
            Response(200, {"data": [{"id": "1"}]}),
        ]

    def get(self, *args, **kwargs) -> Response:
        return self.responses.pop(0)


def test_rate_limit_sets_shared_cooldown_and_retries(monkeypatch) -> None:
    cooldowns: list[int] = []
    monkeypatch.setattr(facebook_graph, "wait_for_meta_limit", lambda client: None)
    monkeypatch.setattr(
        facebook_graph,
        "set_meta_limit",
        lambda client, **kwargs: cooldowns.append(kwargs["ttl_seconds"]) or 0,
    )
    monkeypatch.setattr(facebook_graph.time, "sleep", lambda seconds: None)

    client = facebook_graph.MetaGraphClient("token", session=Session())

    assert client.get("act_1/ads") == {"data": [{"id": "1"}]}
    assert cooldowns == [15]
