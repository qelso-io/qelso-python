# qelso

The Python client for the [Qelso Data API](https://www.qelso.io): historical prediction-market data from
Polymarket and Kalshi, from daily bars down to single order-book changes.

```bash
pip install qelso            # add [pandas] for DataFrames: pip install "qelso[pandas]"
```

Python 3.10 or later. Every endpoint of the API is a method, with typed responses (pydantic models). The full
guide is at [docs.qelso.io](https://docs.qelso.io).

## Authenticate

Send your API key with every request. Pass it to the client, or set `QELSO_API_KEY`.

```python
from qelso import Qelso

client = Qelso()  # reads QELSO_API_KEY
client = Qelso(api_key="pk_live_...")
```

Your plan decides which venues and data your key can read, and how far back. `client.get_usage()` shows them:

```python
for grant in client.get_usage().data.entitlements:
    print(grant.venue, grant.entitlement, grant.enabled, grant.max_history_days)
```

## Find a market

Markets, events and series have IDs made of the venue and the venue's own ID: `polymarket:561229`,
`kalshi:KXFEDDECISION-26DEC-H0`. A Polymarket market can also be named by its slug (`polymarket:<slug>`).

```python
for market in client.list_markets(venue="polymarket", search="2028 presidential election", limit=20).data:
    print(market.id, market.title, market.status)

market = client.get_market("polymarket:561229").data  # open until November 2028
print(market.title, market.outcomes.yes.label, market.close_time)
```

The `list_*` and `get_*` methods return one page, with `next_cursor` for the next one; `iter_*` methods go
through every page for you (`limit` is the page size).

## Prices, candles and statistics

Every history endpoint takes `start` and `end` (an RFC 3339 string, a date, epoch milliseconds, or a
`datetime`; times without an offset are UTC) and a resolution from `1d` down to `100ms`, as your plan allows.

```python
from datetime import date, timedelta

end = date.today()
start = end - timedelta(days=7)

page = client.get_market_prices("polymarket:561229", start=start, end=end, resolution="1h")
for p in page.data:
    print(p.t, p.mid, p.last)

df = client.iter_market_candles("polymarket:561229", start=start, end=end, resolution="1h").to_pandas()
```

Each row covers the interval `[t, t + resolution)` and gives its values as of the interval's end. On
Polymarket, prices at `1m` and coarser give the mid and the last trade; bid and ask come at `1s` and `100ms`.
Every market has one book, the YES side; pass `outcome="no"` for the NO view on any price endpoint.

## Books, ticks and trades

On plans that include them: the order book at any millisecond, every best bid and ask change, every book change
and every trade.

```python
from datetime import datetime, timezone

at = datetime.now(timezone.utc) - timedelta(days=1)
book = client.get_market_books("polymarket:561229", at=at, depth=10).data[0]
print(book.bids[:3], book.asks[:3])

trades = client.get_market_trades("polymarket:561229", start=start, end=end).data

with client.stream_market_trades("polymarket:561229", start=start, end=end) as stream:
    for trade in stream:
        print(trade.ts, trade.price, trade.size, trade.aggressor)
```

## Streams: a whole window in one request

History and tick endpoints also stream. If the connection breaks, a stream resumes by itself after the last
row it received, so no row is repeated or skipped. Once rows are flowing, it also waits out an outage: a
failed request is tried again after 1, 2, 4 … 30 seconds.

```python
# CSV is the fastest way into pandas or a file.
prices = client.stream_market_prices(
    "polymarket:561229", start=start, end=end, resolution="10m", format="csv"
).to_pandas()
```

`save(path)` writes to `path.part` and renames it once the stream has ended. If the stream fails anyway, the
`.part` file keeps what arrived: `save(path, resume=True)` on a new stream with the same parameters carries on
from its last row. `stream.resume_after` gives the `after=` to continue from.

`book-updates` starts with the book as it stood at `start` (`kind="snapshot"` rows), then every level change
(`kind="update"`). Apply them in order to keep a full book.

## Kalshi data

Every response and stream with Kalshi data carries the line `Powered by Kalshi Research` (`page.attribution`,
`stream.attribution`). Show it wherever you display Kalshi data. Kalshi data is available to eligible
organisations whose plan includes it.

## Errors and retries

Errors carry the API's stable `code`, its `message`, `details` and the `request_id` to quote to support.

```python
import qelso

try:
    client.get_market_prices("polymarket:561229", start="2020-01-01", end="2026-11-01")
except qelso.PermissionDeniedError as e:
    print(e.code, e.details)  # plan_history_limit {'earliest_allowed': '...'}
except qelso.APIStatusError as e:
    print(e.status_code, e.code, e.message, e.request_id)
```

The client retries rate limits (after `Retry-After`), temporary outages and network errors, up to
`max_retries` (3) times. It never retries a used-up monthly quota (`QuotaExceededError`) or a query that ran
over the API's time limit (`QueryTimeoutError`: narrow the window, or stream it).

Each response's `response_meta` gives its request ID, the units it cost and your remaining rate limit.

## Async

`AsyncQelso` has the same methods as coroutines; `iter_*` and `stream_*` give async iterables.

```python
import asyncio

from qelso import AsyncQelso


async def main() -> None:
    async with AsyncQelso() as client:
        page = await client.list_markets(venue="polymarket", search="2028 presidential election")
        for market in page.data:
            print(market.id, market.title)


asyncio.run(main())
```

## Versions

The client follows the API's v2 contract. v2 changes only by addition, so fields and values added later are
read without breaking your code: new fields appear in `model_extra`, and new venues, datasets or error codes
come through as plain strings.
