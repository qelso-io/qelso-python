"""Streams that break: reopening through an outage, the count that only rises without new rows, a stream that
stops inside a row, book-updates' starting book, files saved and resumed, and a real socket cut mid-chunk."""

from __future__ import annotations

import json
import re
import socket
import threading
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest
from support import Chunks, Recorder, async_client, client, stream

import qelso
from qelso import Qelso

T = [f"2026-11-03T14:0{i}:00.000Z" for i in range(10)]


def trade(i: int) -> bytes:
    return (
        f'{{"ts":"{T[i]}","seq":1,"trade_id":"t{i}","price":0.5,"size":1,"aggressor":"buy","is_block":false}}\n'
    ).encode()


def after_index(request: httpx.Request) -> int:
    """The row a request resumes after (-1: from the start)."""
    after = request.url.params.get("after")
    return -1 if after is None else T.index(after.split(",")[0])


def serving(total: int, cut_every: int | None) -> Callable[[httpx.Request], httpx.Response]:
    """Trades 0..total-1, honouring after=; cut after each `cut_every` rows."""

    def answer(request: httpx.Request) -> httpx.Response:
        first = after_index(request) + 1
        last = total if cut_every is None else min(total, first + cut_every)
        return stream([trade(i) for i in range(first, last)], cut=last < total)

    return answer


def test_a_break_after_new_rows_never_runs_out() -> None:
    rec = Recorder(serving(total=8, cut_every=1))
    s = client(rec).stream_market_trades("kalshi:X", start=T[0], end=T[9], max_resumes=1)
    assert [r.trade_id for r in s] == [f"t{i}" for i in range(8)]
    assert s.resumes == 7 and len(rec.requests) == 8


def test_reopening_waits_out_an_outage() -> None:
    serve = serving(total=3, cut_every=1)
    calls = {"n": 0}
    waits: list[float] = []

    def answer(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if 2 <= calls["n"] <= 4:  # the API is away for three tries
            raise httpx.ConnectError("refused", request=request)
        return serve(request)

    c = client(Recorder(answer), max_retries=0)
    c._sleep = waits.append  # type: ignore[method-assign]
    assert [r.trade_id for r in c.stream_market_trades("kalshi:X", start=T[0], end=T[9])] == [
        "t0",
        "t1",
        "t2",
    ]
    assert waits == [0.0, 1.0, 2.0, 4.0, 0.0]  # at once after new rows, then longer while it fails


def test_an_outage_longer_than_max_resumes_raises_with_where_to_resume() -> None:
    def answer(request: httpx.Request) -> httpx.Response:
        if "after" in request.url.params:
            raise httpx.ConnectError("refused", request=request)
        return stream([trade(0)], cut=True)

    s = client(Recorder(answer), max_retries=0).stream_market_trades(
        "kalshi:X", start=T[0], end=T[9], max_resumes=3
    )
    with pytest.raises(qelso.APIConnectionError, match="failed 4 times in a row"):
        list(s)
    assert s.resume_after == f"{T[0]},1"


def test_a_used_up_quota_on_reopening_is_not_retried() -> None:
    def answer(request: httpx.Request) -> httpx.Response:
        if "after" in request.url.params:
            body = {"error": {"code": "quota_exceeded", "message": "used up", "request_id": "r"}}
            return httpx.Response(429, json=body)
        return stream([trade(0)], cut=True)

    rec = Recorder(answer)
    with pytest.raises(qelso.QuotaExceededError):
        list(client(rec).stream_market_trades("kalshi:X", start=T[0], end=T[9]))
    assert len(rec.requests) == 2


def test_a_stream_that_stops_inside_a_row_was_cut() -> None:
    whole, partial = trade(0), trade(1)[:25]
    rec = Recorder(
        httpx.Response(
            200, headers={"content-type": "application/x-ndjson"}, stream=Chunks([whole, partial])
        ),
        stream([trade(1)]),
    )
    rows = list(client(rec).stream_market_trades("kalshi:X", start=T[0], end=T[9]))
    assert [r.trade_id for r in rows] == ["t0", "t1"]
    assert rec.requests[1].url.params["after"] == f"{T[0]},1"


def test_a_stream_answered_as_something_else_is_refused() -> None:
    page = httpx.Response(200, json={"data": []})
    with pytest.raises(qelso.UnexpectedResponseError, match="application/json"):
        list(client(Recorder(page)).stream_market_trades("kalshi:X", start=T[0], end=T[9]))


def book(kind: str, side: str, price: float, ts: int = 0, seq: int = 0) -> bytes:
    row = {"ts": T[ts], "seq": seq, "kind": kind, "side": side, "price": price, "size": 1, "delta": None}
    return json.dumps(row).encode() + b"\n"


def test_a_starting_book_cut_halfway_is_never_given_twice_even_if_it_changed() -> None:
    first = [book("snapshot", "bid", 0.40), book("snapshot", "bid", 0.39)]
    second = [book("snapshot", "bid", 0.41), book("snapshot", "ask", 0.43), book("update", "ask", 0.43, 1, 1)]
    rec = Recorder(stream(first, cut=True), stream(second))
    rows = list(client(rec).stream_market_book_updates("kalshi:X", start=T[0], end=T[5]))
    assert [(r.kind, r.price) for r in rows] == [("snapshot", 0.41), ("snapshot", 0.43), ("update", 0.43)]


def test_a_starting_book_with_no_change_after_it_comes_at_the_end() -> None:
    rows = list(
        client(
            Recorder(stream([book("snapshot", "bid", 0.4), book("snapshot", "ask", 0.5)]))
        ).stream_market_book_updates("kalshi:X", start=T[0], end=T[5])
    )
    assert [r.side for r in rows] == ["bid", "ask"]


def test_save_keeps_its_part_when_it_fails_and_resumes_from_it(tmp_path: Path) -> None:
    target = tmp_path / "trades.ndjson"
    rec = Recorder(serving(total=4, cut_every=2))
    with pytest.raises(qelso.APIConnectionError):
        client(rec).stream_market_trades("kalshi:X", start=T[0], end=T[9], max_resumes=0).save(target)
    part = tmp_path / "trades.ndjson.part"
    assert part.read_bytes() == trade(0) + trade(1) and not target.exists()
    with part.open("ab") as handle:
        handle.write(trade(2)[:10])  # a row cut while it was written
    rec = Recorder(serving(total=4, cut_every=None))
    assert client(rec).stream_market_trades("kalshi:X", start=T[0], end=T[9]).save(target, resume=True) == 4
    assert rec.requests[0].url.params["after"] == f"{T[1]},1"
    assert target.read_bytes() == b"".join(trade(i) for i in range(4)) and not part.exists()


def test_save_resumes_a_csv_file_with_its_one_header(tmp_path: Path) -> None:
    header = b"t,mid,carried\n"
    target = tmp_path / "prices.csv"
    (tmp_path / "prices.csv.part").write_bytes(header + f"{T[0]},0.4,false\n".encode())
    rec = Recorder(stream([header, f"{T[1]},0.5,false\n".encode()], media="text/csv"))
    s = client(rec).stream_market_prices("kalshi:X", start=T[0], end=T[2], format="csv")
    assert s.save(target, resume=True) == 2
    assert rec.requests[0].url.params["after"] == T[0]
    assert target.read_text() == f"t,mid,carried\n{T[0]},0.4,false\n{T[1]},0.5,false\n"


def test_save_without_a_single_row_leaves_nothing(tmp_path: Path) -> None:
    def answer(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403, json={"error": {"code": "forbidden_dataset", "message": "m", "request_id": "r"}}
        )

    with pytest.raises(qelso.PermissionDeniedError):
        client(Recorder(answer)).stream_market_trades("kalshi:X", start=T[0], end=T[9]).save(
            tmp_path / "t.ndjson"
        )
    assert list(tmp_path.iterdir()) == []


async def test_async_save_and_resume_after_an_outage(tmp_path: Path) -> None:
    serve = serving(total=3, cut_every=1)
    calls = {"n": 0}

    def answer(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 2:
            raise httpx.ConnectError("refused", request=request)
        return serve(request)

    c = async_client(Recorder(answer), max_retries=0)
    assert await c.stream_market_trades("kalshi:X", start=T[0], end=T[9]).save(tmp_path / "t.ndjson") == 3
    assert (tmp_path / "t.ndjson").read_bytes() == b"".join(trade(i) for i in range(3))


# ---------- a real socket ----------
class OneShotServer:
    """A TCP server that answers each connection with the next raw response, then closes it."""

    def __init__(self, responses: list[bytes]) -> None:
        self.responses = responses
        self.requests: list[bytes] = []
        self.sock = socket.create_server(("127.0.0.1", 0))
        self.port = self.sock.getsockname()[1]
        self.thread = threading.Thread(target=self.serve, daemon=True)
        self.thread.start()

    def serve(self) -> None:
        for response in self.responses:
            conn, _ = self.sock.accept()
            with conn:
                data = b""
                while b"\r\n\r\n" not in data:
                    chunk = conn.recv(4096)
                    if not chunk:  # the client gave up before sending a whole request
                        break
                    data += chunk
                self.requests.append(data)
                conn.sendall(response)
        self.sock.close()


def chunk(data: bytes) -> bytes:
    return f"{len(data):x}\r\n".encode() + data + b"\r\n"


HEAD = b"HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\n"


def test_a_chunked_stream_cut_on_a_real_socket_resumes() -> None:
    cut = HEAD + b"Transfer-Encoding: chunked\r\n\r\n" + chunk(trade(0) + trade(1)[:30])  # no last chunk
    whole = HEAD + b"Transfer-Encoding: chunked\r\n\r\n" + chunk(trade(1) + trade(2)) + b"0\r\n\r\n"
    server = OneShotServer([cut, whole])
    with Qelso(api_key="k", base_url=f"http://127.0.0.1:{server.port}", max_retries=0) as c:
        rows = list(c.stream_market_trades("kalshi:X", start=T[0], end=T[9]))
    assert [r.trade_id for r in rows] == ["t0", "t1", "t2"]
    assert b"after=2026-11-03T14%3A00%3A00.000Z%2C1" in server.requests[1]


def test_a_connection_closed_inside_a_row_on_a_real_socket_resumes() -> None:
    closed = HEAD + b"Connection: close\r\n\r\n" + trade(0) + trade(1)[:30]  # no length: ends when it closes
    whole = HEAD + b"Content-Length: " + str(len(trade(1))).encode() + b"\r\n\r\n" + trade(1)
    server = OneShotServer([closed, whole])
    with Qelso(api_key="k", base_url=f"http://127.0.0.1:{server.port}", max_retries=0) as c:
        rows = list(c.stream_market_trades("kalshi:X", start=T[0], end=T[9]))
    assert [r.trade_id for r in rows] == ["t0", "t1"]


def test_stopping_inside_a_released_book_reports_only_what_was_taken() -> None:
    rows = [book("snapshot", "bid", 0.40), book("snapshot", "bid", 0.39), book("snapshot", "ask", 0.42)]
    rows.append(book("update", "bid", 0.41, 1, 1))
    s = client(Recorder(stream(rows))).stream_market_book_updates("kalshi:X", start=T[0], end=T[5])
    first = next(iter(s))
    assert first.kind == "snapshot" and s.rows == 1
    assert s.resume_after is None  # inside the starting book: only a new stream from the start is whole
    s.close()


def test_a_stream_that_never_starts_follows_the_clients_retry_rule() -> None:
    rec = Recorder(
        lambda request: httpx.Response(
            500, json={"error": {"code": "internal_error", "message": "m", "request_id": "r"}}
        )
    )
    with pytest.raises(qelso.InternalServerError):
        list(client(rec, max_retries=2).stream_market_trades("kalshi:X", start=T[0], end=T[9]))
    assert len(rec.requests) == 3  # one layer of retries, not two


def test_a_long_retry_after_ends_a_stream_at_once() -> None:
    busy = {"error": {"code": "service_unavailable", "message": "m", "request_id": "r"}}
    rec = Recorder(lambda request: httpx.Response(503, json=busy, headers={"retry-after": "600"}))
    with pytest.raises(qelso.ServiceUnavailableError):
        list(client(rec).stream_market_trades("kalshi:X", start=T[0], end=T[9]))
    assert len(rec.requests) == 1

    def answer(request: httpx.Request) -> httpx.Response:
        if "after" in request.url.params:
            return httpx.Response(
                429,
                json={"error": {"code": "rate_limited", "message": "m", "request_id": "r"}},
                headers={"retry-after": "600"},
            )
        return stream([trade(0)], cut=True)

    rec = Recorder(answer)
    with pytest.raises(qelso.APIConnectionError, match=re.escape("after='2026-11-03T14:00:00.000Z,1'")):
        list(client(rec).stream_market_trades("kalshi:X", start=T[0], end=T[9]))
    assert len(rec.requests) == 2


def test_a_stream_waits_as_long_as_retry_after_asks() -> None:
    def answer(request: httpx.Request) -> httpx.Response:
        if "after" in request.url.params and not any("after" in r.url.params for r in rec.requests[:-1]):
            body = {"error": {"code": "service_unavailable", "message": "m", "request_id": "r"}}
            return httpx.Response(503, json=body, headers={"retry-after": "7"})
        return serving(total=2, cut_every=1)(request)

    rec = Recorder(answer)
    waits: list[float] = []
    c = client(rec)
    c._sleep = waits.append  # type: ignore[method-assign]
    assert [r.trade_id for r in c.stream_market_trades("kalshi:X", start=T[0], end=T[9])] == ["t0", "t1"]
    assert waits[:2] == [0.0, 7.0]


def test_a_malformed_base_url_or_header_is_never_retried() -> None:
    with pytest.raises(ValueError, match="base_url"):
        Qelso(api_key="k", base_url="api.qelso.io")

    def refuse(request: httpx.Request) -> httpx.Response:
        # What httpx raises for a header it can't send; its text may quote the header.
        raise httpx.LocalProtocolError(f"Illegal header value {request.headers['x-api-key']!r}")

    rec = Recorder(refuse)
    c = client(rec)
    with pytest.raises(qelso.InvalidRequestError) as caught:
        list(c.stream_market_trades("kalshi:X", start=T[0], end=T[9]))
    assert "pk_test" not in str(caught.value)
    with pytest.raises(qelso.InvalidRequestError):
        c.list_venues()
    assert len(rec.requests) == 2  # once each, never retried


async def test_async_save_resumes_from_a_part_file(tmp_path: Path) -> None:
    target = tmp_path / "trades.ndjson"
    (tmp_path / "trades.ndjson.part").write_bytes(trade(0))
    rec = Recorder(serving(total=3, cut_every=None))
    assert (
        await async_client(rec)
        .stream_market_trades("kalshi:X", start=T[0], end=T[9])
        .save(target, resume=True)
        == 3
    )
    assert target.read_bytes() == b"".join(trade(i) for i in range(3))
