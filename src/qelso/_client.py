"""The two clients: ``Qelso`` and ``AsyncQelso``."""

from __future__ import annotations

from qelso._operations import AsyncOperations, SyncOperations


class Qelso(SyncOperations):
    """Client for the Qelso Data API (``https://api.qelso.io/v2``).

    One method per endpoint (``get_market_prices``, ``list_markets``...), plus:

    - ``iter_*``: every row of a list or history request, page after page;
    - ``stream_*``: every row of a window from one request (NDJSON or CSV), resumed if the connection breaks.

    ::

        from qelso import Qelso

        with Qelso() as client:  # the key from QELSO_API_KEY
            for market in client.iter_markets(venue="kalshi", status="open", search="fed"):
                print(market.id, market.title)
    """


class AsyncQelso(AsyncOperations):
    """The async client: the same methods as coroutines; ``iter_*`` and ``stream_*`` give async iterables.

    ::

        async with AsyncQelso() as client:
            page = await client.get_market_prices(
                "kalshi:KXFEDDECISION-26DEC-H0", start="2026-11-03", end="2026-11-04"
            )
    """
