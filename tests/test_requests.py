"""Requests on the wire, answers read back, errors and retries."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import httpx
import pytest
from support import BASE, KEY, Recorder, client, error, ok, point, prices_page

import qelso
from qelso import Qelso, _http

POST_URL = f"{BASE}/v2/anything"  # no operation is a POST today: these rules live in the request loop

HEALTH = {"status": "ok", "version": "2.0.0", "time": "2026-11-03T14:00:00.000Z"}


def test_key_agent_and_accept_go_on_every_api_request() -> None:
    rec = Recorder(ok(HEALTH))
    client(rec).get_health()
    headers = rec.last.headers
    assert headers["x-api-key"] == KEY
    assert headers["user-agent"] == f"qelso-python/{qelso.__version__}"
    assert headers["accept"] == "application/json"
    assert str(rec.last.url) == f"{BASE}/v2/health"


def test_key_and_base_url_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("QELSO_API_KEY", "pk_live_from_env")
    monkeypatch.setenv("QELSO_BASE_URL", "https://elsewhere.test/")
    rec = Recorder(ok(HEALTH))
    c = Qelso(http_client=httpx.Client(transport=httpx.MockTransport(rec)))
    c.get_health()
    assert rec.last.headers["x-api-key"] == "pk_live_from_env"
    assert str(rec.last.url) == "https://elsewhere.test/v2/health"
    assert "pk_live" not in repr(c)


def test_no_key_sends_no_key_header() -> None:
    rec = Recorder(ok(HEALTH))
    client(rec, api_key="").get_health()
    assert "x-api-key" not in rec.last.headers


def test_query_styles_times_and_path_escaping() -> None:
    rec = Recorder(ok({"data": [], "next_cursor": None}))
    c = client(rec)
    c.list_markets(venue=["kalshi", "polymarket"], status="open", ids=["kalshi:A", "kalshi:B"], limit=5)
    params = rec.last.url.params
    assert params.get_list("venue") == ["kalshi", "polymarket"]
    assert params.get_list("status") == ["open"]
    assert params["ids"] == "kalshi:A,kalshi:B"
    assert params["limit"] == "5"
    rec.answers = [ok(prices_page([]))]
    c.get_market_prices(
        "polymarket:will-it/rain?",
        start=datetime(2026, 11, 3, 15, 4, 5, 678901, tzinfo=timezone(timedelta(hours=1))),
        end=datetime(2026, 11, 3, 16),  # naive: UTC
        fields=["bid", "mid"],
    )
    assert rec.last.url.raw_path.startswith(b"/v2/markets/polymarket:will-it%2Frain%3F/prices?")
    params = rec.last.url.params
    assert params["start"] == "2026-11-03T14:04:05.678Z"
    assert params["end"] == "2026-11-03T16:00:00.000Z"
    assert params["fields"] == "bid,mid"
    assert "resolution" not in params  # unset parameters stay off the wire
    c.get_market_prices("kalshi:X", start=date(2026, 11, 3), end=1793750400000)
    assert rec.last.url.params["start"] == "2026-11-03"
    assert rec.last.url.params["end"] == "1793750400000"


def test_answer_and_its_headers() -> None:
    headers = {
        "x-request-id": "req-9",
        "qelso-units": "1",
        "x-ratelimit-limit": "500",
        "x-ratelimit-remaining": "499",
        "x-ratelimit-reset": "1793714460",
    }
    body = prices_page(
        [point("2026-11-03T14:00:00.000Z", 0.47)],
        attribution="Powered by Kalshi Research",
        data_type="delayed",
        later_field=1,
    )
    page = client(Recorder(ok(body, headers=headers))).get_market_prices("kalshi:X", start="a", end="b")
    assert page.data[0].t == datetime(2026, 11, 3, 14, tzinfo=timezone.utc)
    assert page.data[0].mid == 0.47 and page.data[0].bid is None
    assert page.attribution == "Powered by Kalshi Research"
    assert page.model_extra == {"later_field": 1}
    meta = page.response_meta
    assert meta is not None
    assert (meta.request_id, meta.units, meta.rate_limit, meta.rate_limit_remaining) == ("req-9", 1, 500, 499)


def test_health_answers_503_without_raising() -> None:
    down = {"status": "unavailable", "version": "2.0.0", "time": "2026-11-03T14:00:00.000Z"}
    rec = Recorder(ok(down, status=503))
    assert client(rec).get_health().status == "unavailable"
    assert len(rec.requests) == 1  # not retried


@pytest.mark.parametrize(
    ("status", "code", "cls"),
    [
        (400, "invalid_parameter", qelso.BadRequestError),
        (400, "window_too_large", qelso.BadRequestError),
        (401, "unauthorized", qelso.AuthenticationError),
        (403, "forbidden_venue", qelso.PermissionDeniedError),
        (403, "suspended", qelso.PermissionDeniedError),
        (404, "not_found", qelso.NotFoundError),
        (406, "not_acceptable", qelso.NotAcceptableError),
        (429, "quota_exceeded", qelso.QuotaExceededError),
        (504, "timeout", qelso.QueryTimeoutError),
    ],
)
def test_errors_are_raised_with_their_code_and_not_retried(status: int, code: str, cls: type) -> None:
    rec = Recorder(error(status, code, "what went wrong"))
    with pytest.raises(cls) as caught:
        client(rec).list_venues()
    assert caught.value.code == code
    assert caught.value.message == "what went wrong"
    assert caught.value.request_id == "req-1"
    assert caught.value.details == {"x": 1}
    assert len(rec.requests) == 1
    assert f"{status} {code}" in str(caught.value)


@pytest.mark.parametrize(
    "first",
    [
        error(429, "rate_limited", retry_after="0"),
        error(500, "internal_error"),
        error(503, "service_unavailable", retry_after="1"),
        error(503, "metadata_refreshing", retry_after="1"),
        httpx.Response(502, text="<html>bad gateway</html>"),
        httpx.Response(504, text="gateway timeout"),
    ],
)
def test_passing_failures_are_retried(first: httpx.Response) -> None:
    rec = Recorder(first, ok({"data": []}))
    assert client(rec).list_venues().data == []
    assert len(rec.requests) == 2


def test_retries_stop_at_max_retries() -> None:
    rec = Recorder(error(500, "internal_error"))
    with pytest.raises(qelso.InternalServerError):
        client(rec, max_retries=2).list_venues()
    assert len(rec.requests) == 3


def test_a_long_retry_after_is_not_waited_for() -> None:
    rec = Recorder(error(429, "rate_limited", retry_after="600"))
    with pytest.raises(qelso.RateLimitError) as caught:
        client(rec).list_venues()
    assert caught.value.retry_after == 600
    assert len(rec.requests) == 1


def test_a_proxy_page_is_an_error_without_code() -> None:
    rec = Recorder(httpx.Response(502, text="<html>bad gateway</html>"))
    with pytest.raises(qelso.InternalServerError) as caught:
        client(rec, max_retries=0).list_venues()
    assert caught.value.code is None
    assert "bad gateway" in caught.value.message


def test_network_errors_are_retried_then_raised() -> None:
    calls = []

    def broken(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        raise httpx.ReadTimeout("slow", request=request)

    with pytest.raises(qelso.APITimeoutError):
        client(Recorder(broken), max_retries=1).list_venues()
    assert len(calls) == 2

    def refused(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        raise httpx.ConnectError("refused", request=request)

    calls.clear()
    with pytest.raises(qelso.APIConnectionError):
        client(Recorder(refused), max_retries=1)._send("POST", POST_URL, body={"a": 1})
    assert len(calls) == 2  # never sent, so even a POST goes again


def test_a_key_read_from_a_file_loses_its_newline_and_a_bad_key_is_never_shown() -> None:
    rec = Recorder(ok(HEALTH))
    client(rec, api_key=KEY + "\n").get_health()
    assert rec.last.headers["x-api-key"] == KEY
    with pytest.raises(ValueError) as caught:
        client(rec, api_key="pk_live_abc\nsecret")
    assert "secret" not in str(caught.value)


def test_a_retry_after_that_makes_no_sense_is_ignored() -> None:
    for value in ("-5", "nan", "inf", "soon"):
        rec = Recorder(error(429, "rate_limited", retry_after=value), ok({"data": []}))
        assert client(rec).list_venues().data == []
        assert len(rec.requests) == 2


def test_a_pager_carries_on_from_a_cursor() -> None:
    rec = Recorder(ok(prices_page([point("2026-11-03T14:05:00.000Z")])))
    pager = client(rec).iter_market_prices("kalshi:X", start="a", end="b", cursor="c7")
    assert len(list(pager)) == 1 and pager.next_cursor is None
    assert rec.last.url.params["cursor"] == "c7"


def test_a_post_is_retried_only_when_refused_before_anything_happened() -> None:
    rec = Recorder(error(500, "internal_error"))
    with pytest.raises(qelso.InternalServerError):
        client(rec)._send("POST", POST_URL, body={"a": 1})
    assert len(rec.requests) == 1
    assert rec.last.method == "POST" and json.loads(rec.last.content) == {"a": 1}
    rec = Recorder(error(429, "rate_limited", retry_after="0"), error(400, "invalid_parameter"))
    with pytest.raises(qelso.BadRequestError):
        client(rec)._send("POST", POST_URL, body={"a": 1})
    assert len(rec.requests) == 2


def test_a_json_body_is_written_from_its_items() -> None:
    body = _http.encode_body(
        [
            ("venue", "kalshi", "scalar"),
            ("markets", {"event": "kalshi:KXFED-26DEC"}, "model"),
            ("start", datetime(2026, 11, 1, tzinfo=timezone.utc), "time"),
            ("end", "2026-11-08", "time"),
            ("left_out", None, "scalar"),
        ]
    )
    assert body == {
        "venue": "kalshi",
        "markets": {"event": "kalshi:KXFED-26DEC"},
        "start": "2026-11-01T00:00:00.000Z",
        "end": "2026-11-08",
    }


def test_epoch_milliseconds_stay_numbers_in_a_json_body() -> None:
    body = _http.encode_body([("start", 1793714400000, "time"), ("end", "2026-11-08", "time")])
    assert body == {"start": 1793714400000, "end": "2026-11-08"}


def test_the_base_url_scheme_may_be_in_capitals() -> None:
    assert Qelso(api_key="k", base_url="HTTPS://api.qelso.io/").base_url == "HTTPS://api.qelso.io"
