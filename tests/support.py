"""Test helpers: clients on a stand-in transport (no network), answers and streams that break on purpose."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import Path
from typing import Any

import httpx

from qelso import AsyncQelso, Qelso

_SDK = Path(__file__).resolve().parents[1]
# contract/ in the public SDK repo (scripts/sync_public.py), or api/'s copy in the qelso repo.
_CONTRACTS = (
    _SDK / "contract" / "openapi.v2.json",
    _SDK.parent / "api" / "qelso_api" / "contract" / "openapi.v2.json",
)
CONTRACT = json.loads(next((p for p in _CONTRACTS if p.is_file()), _CONTRACTS[0]).read_text())
KEY = "pk_test_abcdefghijkl_secret"
BASE = "https://api.test"

Handler = Callable[[httpx.Request], httpx.Response]


class Recorder:
    """A transport handler that answers from a list (or a function) and keeps every request."""

    def __init__(self, *answers: httpx.Response | Handler) -> None:
        self.answers = list(answers)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        answer = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        return answer(request) if callable(answer) else answer

    async def handle_async(self, request: httpx.Request) -> httpx.Response:
        return self(request)

    @property
    def last(self) -> httpx.Request:
        return self.requests[-1]


def client(recorder: Recorder, **kwargs: Any) -> Qelso:
    http = httpx.Client(transport=httpx.MockTransport(recorder))
    c = Qelso(api_key=kwargs.pop("api_key", KEY), base_url=BASE, http_client=http, **kwargs)
    c._sleep = lambda seconds: None  # type: ignore[method-assign]
    return c


def async_client(recorder: Recorder, **kwargs: Any) -> AsyncQelso:
    http = httpx.AsyncClient(transport=httpx.MockTransport(recorder.handle_async))
    c = AsyncQelso(api_key=kwargs.pop("api_key", KEY), base_url=BASE, http_client=http, **kwargs)

    async def no_sleep(seconds: float) -> None:
        return None

    c._sleep = no_sleep  # type: ignore[method-assign]
    return c


def ok(body: Any, status: int = 200, headers: dict[str, str] | None = None) -> httpx.Response:
    return httpx.Response(status, json=body, headers=headers)


def error(status: int, code: str, message: str = "nope", **headers: str) -> httpx.Response:
    body = {"error": {"code": code, "message": message, "request_id": "req-1", "details": {"x": 1}}}
    return httpx.Response(status, json=body, headers={k.replace("_", "-"): v for k, v in headers.items()})


class Chunks(httpx.SyncByteStream, httpx.AsyncByteStream):
    """A response body sent in these chunks, then cut (``error``) or finished normally."""

    def __init__(self, chunks: list[bytes], error: Exception | None = None) -> None:
        self.chunks = chunks
        self.error = error

    def __iter__(self) -> Iterator[bytes]:
        yield from self.chunks
        if self.error:
            raise self.error

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            yield chunk
        if self.error:
            raise self.error


def stream(
    chunks: list[bytes], *, cut: bool = False, media: str = "application/x-ndjson", **headers: str
) -> httpx.Response:
    error = (
        httpx.RemoteProtocolError("peer closed connection without sending complete message body")
        if cut
        else None
    )
    return httpx.Response(
        200,
        headers={"content-type": media, **{k.replace("_", "-"): v for k, v in headers.items()}},
        stream=Chunks(chunks, error),
    )


MARKET_REF = {
    "id": "kalshi:KXFED-26DEC-H0",
    "venue": "kalshi",
    "native_id": "KXFED-26DEC-H0",
    "title": "Fed holds in December?",
    "status": "open",
    "outcomes": {"yes": {"label": "Yes", "token_id": None}, "no": {"label": "No", "token_id": None}},
    "open_time": "2026-09-17T18:00:00.000Z",
    "close_time": None,
}


def prices_page(points: list[dict[str, Any]], next_cursor: str | None = None, **extra: Any) -> dict[str, Any]:
    return {
        "market": MARKET_REF,
        "outcome": "yes",
        "resolution": "1m",
        "data": points,
        "next_cursor": next_cursor,
        **extra,
    }


def point(t: str, mid: float | None = 0.5, carried: bool = False) -> dict[str, Any]:
    return {"t": t, "bid": None, "ask": None, "mid": mid, "last": None, "carried": carried}
