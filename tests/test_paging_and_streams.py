"""Pagers follow cursors; streams deliver every row once, resuming after a broken connection."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
from support import Recorder, async_client, client, error, ok, point, prices_page, stream

import qelso

T = [f"2026-11-03T14:0{i}:00.000Z" for i in range(10)]


# ---------- pagers ----------
def test_a_pager_follows_next_cursor_to_the_end() -> None:
    rec = Recorder(
        ok(prices_page([point(T[0]), point(T[1])], next_cursor="c1")),
        ok(prices_page([point(T[2])], next_cursor=None)),
    )
    pager = client(rec).iter_market_prices("kalshi:X", start=T[0], end=T[3], limit=2)
    assert [p.t.minute for p in pager] == [0, 1, 2]
    assert "cursor" not in rec.requests[0].url.params
    assert rec.requests[1].url.params["cursor"] == "c1"
    assert rec.requests[1].url.params["limit"] == "2"  # the same parameters on every page
    assert pager.last_page.market.id == "kalshi:KXFED-26DEC-H0"
    with pytest.raises(RuntimeError):
        list(pager)


def test_a_pager_gives_pages_and_refuses_a_cursor_loop() -> None:
    rec = Recorder(ok(prices_page([point(T[0])], next_cursor="same")))
    pager = client(rec).iter_market_prices("kalshi:X", start=T[0], end=T[3])
    with pytest.raises(RuntimeError, match="same cursor"):
        list(pager.pages())


def test_a_pager_to_pandas_keeps_only_the_fields_asked_for() -> None:
    rec = Recorder(ok(prices_page([{"t": T[0], "mid": 0.4, "carried": False}])))
    df = client(rec).iter_market_prices("kalshi:X", start=T[0], end=T[1], fields="mid").to_pandas()
    assert list(df.columns) == ["t", "mid", "carried"]


def test_a_pager_to_pandas() -> None:
    rec = Recorder(ok(prices_page([point(T[0], 0.4), point(T[1], None, carried=True)])))
    df = client(rec).iter_market_prices("kalshi:X", start=T[0], end=T[2]).to_pandas()
    assert list(df.columns)[:2] == ["t", "bid"]
    assert str(df["t"].dtype) == "datetime64[ns, UTC]" or str(df["t"].dtype).startswith("datetime64")
    assert df["carried"].tolist() == [False, True]


async def test_an_async_pager() -> None:
    rec = Recorder(
        ok(prices_page([point(T[0])], next_cursor="c1")),
        ok(prices_page([point(T[1])])),
    )
    c = async_client(rec)
    rows = [row async for row in c.iter_market_prices("kalshi:X", start=T[0], end=T[2])]
    assert [r.t.minute for r in rows] == [0, 1]
    assert rec.requests[1].url.params["cursor"] == "c1"


# ---------- streams ----------
def lines(*rows: dict) -> list[bytes]:
    return [json.dumps(r).encode() + b"\n" for r in rows]


def trade(ts: str, seq: int, price: float = 0.47) -> dict:
    return {
        "ts": ts,
        "seq": seq,
        "trade_id": f"t{seq}",
        "price": price,
        "size": 10,
        "aggressor": "buy",
        "is_block": False,
    }


def raw_trade(ts: str, seq: int, price: str) -> bytes:
    # Numbers as the API writes them (exact text), not as Python would.
    return (
        f'{{"ts":"{ts}","seq":{seq},"trade_id":"t{seq}","price":{price},"size":10,'
        f'"aggressor":"buy","is_block":false}}\n'
    ).encode()


def test_a_stream_reads_ndjson_rows_and_the_attribution() -> None:
    rec = Recorder(
        stream(
            [*lines(trade(T[0], 1)), *lines(trade(T[0], 2))],
            qelso_attribution="Powered by Kalshi Research",
            qelso_data_type="historical",
        )
    )
    with client(rec).stream_market_trades("kalshi:X", start=T[0], end=T[5]) as s:
        rows = list(s)
    assert [r.seq for r in rows] == [1, 2]
    assert s.attribution == "Powered by Kalshi Research" and s.data_type == "historical"
    assert rec.last.headers["accept"] == "application/x-ndjson"
    assert "after" not in rec.last.url.params


def test_a_broken_stream_resumes_after_the_last_whole_row() -> None:
    first = [raw_trade(T[0], 1, "0.000001"), raw_trade(T[0], 2, "0.5")[:20]]  # the second row is cut
    rec = Recorder(
        stream(first, cut=True),
        stream([raw_trade(T[0], 2, "0.5"), raw_trade(T[1], 1, "0.51")]),
    )
    s = client(rec).stream_market_trades("kalshi:X", start=T[0], end=T[5])
    assert [(r.ts.minute, r.seq) for r in s] == [(0, 1), (0, 2), (1, 1)]
    assert s.resumes == 1 and s.rows == 3
    # The key is the last row's own text: the price exactly as sent, never re-formatted (1e-06).
    assert rec.requests[1].url.params["after"] == f"{T[0]},1"
    assert rec.requests[1].url.params["start"] == T[0]


def test_book_updates_resume_with_all_four_key_fields() -> None:
    upd = {"ts": T[1], "seq": 3, "kind": "update", "side": "bid", "price": 0.000001, "size": 5, "delta": 5}
    snap = {"ts": T[0], "seq": 0, "kind": "snapshot", "side": "bid", "price": 0.4, "size": 5, "delta": None}
    rec = Recorder(
        stream(
            [
                *lines(snap),
                b'{"ts":"'
                + T[1].encode()
                + b'","seq":3,"kind":"update","side":"bid","price":0.000001,"size":5,"delta":5}\n',
            ],
            cut=True,
        ),
        stream(lines(upd | {"seq": 4})),
    )
    rows = list(client(rec).stream_market_book_updates("kalshi:X", start=T[0], end=T[5]))
    assert [r.kind for r in rows] == ["snapshot", "update", "update"]
    assert rec.requests[1].url.params["after"] == f"{T[1]},3,bid,0.000001"


def test_book_updates_cut_in_the_starting_book_start_again_without_repeating_it() -> None:
    snaps = [
        {"ts": T[0], "seq": 0, "kind": "snapshot", "side": side, "price": p, "size": 1, "delta": None}
        for side, p in (("bid", 0.40), ("bid", 0.39), ("ask", 0.42))
    ]
    upd = {"ts": T[1], "seq": 1, "kind": "update", "side": "ask", "price": 0.42, "size": 0, "delta": -1}
    rec = Recorder(
        stream(lines(*snaps[:2]), cut=True),  # two of three snapshot rows, then cut
        stream(lines(*snaps, upd)),  # the whole book again, then the change
    )
    rows = list(client(rec).stream_market_book_updates("kalshi:X", start=T[0], end=T[5]))
    assert [(r.kind, r.price) for r in rows] == [
        ("snapshot", 0.40),
        ("snapshot", 0.39),
        ("snapshot", 0.42),
        ("update", 0.42),
    ]
    assert "after" not in rec.requests[1].url.params


def test_a_stream_cut_before_its_first_row_asks_again_from_the_same_place() -> None:
    rec = Recorder(stream([b'{"ts":'], cut=True), stream(lines(trade(T[0], 1))))
    s = client(rec).stream_market_trades("kalshi:X", start=T[0], end=T[5], after=f"{T[0]},0")
    assert [r.seq for r in s] == [1]
    assert [r.url.params["after"] for r in rec.requests] == [f"{T[0]},0", f"{T[0]},0"]


def test_a_stream_that_keeps_failing_without_new_rows_raises() -> None:
    def answer(request: httpx.Request) -> httpx.Response:
        rows = [] if "after" in request.url.params else lines(trade(T[0], 1))
        return stream(rows, cut=True)  # the server keeps cutting, and nothing new comes after the first row

    s = client(Recorder(answer)).stream_market_trades("kalshi:X", start=T[0], end=T[5], max_resumes=2)
    with pytest.raises(qelso.APIConnectionError, match="failed 3 times in a row") as caught:
        list(s)
    assert "after='2026-11-03T14:00:00.000Z,1'" in str(caught.value)
    assert s.resume_after == f"{T[0]},1" and s.rows == 1


def test_a_stream_refused_at_the_start_raises_the_apis_error() -> None:
    rec = Recorder(error(403, "forbidden_dataset"))
    with pytest.raises(qelso.PermissionDeniedError):
        list(client(rec).stream_market_trades("kalshi:X", start=T[0], end=T[5]))


def test_a_csv_stream_resumes_and_skips_the_repeated_header() -> None:
    header = b"t,mid,carried\n"
    rec = Recorder(
        stream([header, f"{T[0]},0.4,false\n{T[1]},,true\n{T[2]},0.".encode()], cut=True, media="text/csv"),
        stream([header, f"{T[2]},0.41,false\n".encode()], media="text/csv"),
    )
    s = client(rec).stream_market_prices("kalshi:X", start=T[0], end=T[3], fields="mid", format="csv")
    rows = list(s)
    assert [(r.t.minute, r.mid, r.carried) for r in rows] == [
        (0, 0.4, False),
        (1, None, True),
        (2, 0.41, False),
    ]
    assert s.columns == ["t", "mid", "carried"]
    assert rec.requests[0].headers["accept"] == "text/csv"
    assert rec.requests[1].url.params["after"] == T[1]


def test_save_writes_one_header_and_appears_only_when_done(tmp_path: Path) -> None:
    header = b"t,mid,carried\n"
    rec = Recorder(
        stream([header, f"{T[0]},0.4,false\n".encode()], cut=True, media="text/csv"),
        stream([header, f"{T[1]},0.5,false\n".encode()], media="text/csv"),
    )
    target = tmp_path / "prices.csv"
    s = client(rec).stream_market_prices("kalshi:X", start=T[0], end=T[2], format="csv")
    assert s.save(target) == 2
    assert target.read_text() == f"t,mid,carried\n{T[0]},0.4,false\n{T[1]},0.5,false\n"
    assert not (tmp_path / "prices.csv.part").exists()


def test_stream_to_pandas_from_csv_and_ndjson() -> None:
    header = b"ts,seq,trade_id,price,size,aggressor,is_block\n"
    body = f"{T[0]},1,0012,0.47,10,buy,false\n{T[0]},2,0013,0.48,5,sell,true\n".encode()
    df = (
        client(Recorder(stream([header, body], media="text/csv")))
        .stream_market_trades("kalshi:X", start=T[0], end=T[1], format="csv")
        .to_pandas()
    )
    assert df["trade_id"].tolist() == ["0012", "0013"]  # an ID stays text
    assert df["is_block"].tolist() == [False, True]
    assert str(df["ts"].dtype).startswith("datetime64") and str(df["ts"].dt.tz) == "UTC"
    df = (
        client(Recorder(stream(lines(trade(T[0], 1)))))
        .stream_market_trades("kalshi:X", start=T[0], end=T[1])
        .to_pandas()
    )
    assert df["price"].tolist() == [0.47] and df["seq"].tolist() == [1]


def test_books_to_pandas_one_row_per_level() -> None:
    books = [
        {"t": T[0], "bids": [[0.47, 600], [0.46, 10]], "asks": [[0.49, 800]], "last_change": T[0]},
        {"t": T[1], "bids": None, "asks": None, "last_change": None},
    ]
    df = (
        client(Recorder(stream(lines(*books))))
        .stream_market_books("kalshi:X", start=T[0], end=T[2])
        .to_pandas()
    )
    assert list(df.columns) == ["t", "last_change", "side", "level", "price", "size"]
    assert df["side"].tolist()[:3] == ["bid", "bid", "ask"]
    assert df["level"].tolist()[:3] == [0, 1, 0]
    assert len(df) == 4  # the empty time keeps one row


def test_books_stream_only_as_ndjson() -> None:
    with pytest.raises(ValueError, match="format"):
        client(Recorder(ok({}))).stream_market_books("kalshi:X", start=T[0], end=T[2], format="csv")  # type: ignore[arg-type]


def test_a_stream_runs_once() -> None:
    s = client(Recorder(stream(lines(trade(T[0], 1))))).stream_market_trades("kalshi:X", start=T[0], end=T[1])
    list(s)
    with pytest.raises(RuntimeError):
        list(s)


async def test_an_async_stream_resumes() -> None:
    rec = Recorder(
        stream([raw_trade(T[0], 1, "0.4"), b'{"ts"'], cut=True),
        stream([raw_trade(T[0], 2, "0.41")]),
    )
    c = async_client(rec)
    async with c.stream_market_trades("kalshi:X", start=T[0], end=T[5]) as s:
        rows = [row async for row in s]
    assert [r.seq for r in rows] == [1, 2]
    assert rec.requests[1].url.params["after"] == f"{T[0]},1"


async def test_async_errors_and_retries() -> None:
    rec = Recorder(error(429, "rate_limited", retry_after="0"), ok({"data": []}))
    assert (await async_client(rec).list_venues()).data == []
    rec = Recorder(error(429, "quota_exceeded"))
    with pytest.raises(qelso.QuotaExceededError):
        await async_client(rec).list_venues()
    assert len(rec.requests) == 1
