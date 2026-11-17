"""Every page of a list or history request, following ``next_cursor``.

A paging run serves the data available when its first page was served (the contract's "Pagination"), so
its pages never skip or repeat a row; data that arrives later appears when you ask again.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from qelso import _pandas

if TYPE_CHECKING:
    import pandas as pd

RowT = TypeVar("RowT")


class _Cursor:
    def __init__(self, cursor: str | None) -> None:
        self.cursor = cursor
        self.started = False
        self.done = False

    def advance(self, page: Any) -> None:
        cursor = page.next_cursor
        if cursor is not None and cursor == self.cursor:
            raise RuntimeError("the API sent the same cursor twice; stopping rather than looping")
        self.cursor = cursor
        self.done = cursor is None


class _Collect:
    """Pages' rows for a DataFrame, as their fields were sent (a field not asked for stays out)."""

    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.model: Any = None
        self.attrs: dict[str, Any] = {}

    def add(self, page: Any) -> None:
        for row in page.data:
            self.model = type(row)
            self.rows.append(row.model_dump(exclude_unset=True))
        if getattr(page, "attribution", None):
            self.attrs = {"attribution": page.attribution, "data_type": page.data_type}

    def frame(self) -> pd.DataFrame:
        df = _pandas.frame(self.rows, self.model)
        df.attrs.update(self.attrs)
        return df


class SyncPager(Generic[RowT]):
    """Iterate to get every row (``for row in pager``); ``pages()`` gives whole pages instead.

    A pager runs once: iterate it again and it raises. The last page fetched is ``last_page``: its ``market``,
    ``attribution`` and ``response_meta`` describe the run.
    """

    def __init__(self, fetch: Callable[[str | None], Any], cursor: str | None = None) -> None:
        self._fetch = fetch
        self._state = _Cursor(cursor)
        self.last_page: Any = None

    @property
    def next_cursor(self) -> str | None:
        """The cursor of the next page to fetch: pass it as ``cursor`` to carry on a run that stopped."""
        return self._state.cursor

    def pages(self) -> Iterator[Any]:
        """Every page, the first one first."""
        if self._state.started:
            raise RuntimeError("a pager runs once; call the iter_* method again for a new run")
        self._state.started = True
        while not self._state.done:
            page = self._fetch(self._state.cursor)
            self.last_page = page
            self._state.advance(page)
            yield page

    def __iter__(self) -> Iterator[RowT]:
        for page in self.pages():
            yield from page.data

    def to_list(self) -> list[RowT]:
        return list(self)

    def to_pandas(self) -> pd.DataFrame:
        """Every row in a DataFrame (needs ``qelso[pandas]``). Books come one row per price level; Kalshi's
        attribution is in ``df.attrs``."""
        collect = _Collect()
        for page in self.pages():
            collect.add(page)
        return collect.frame()


class AsyncPager(Generic[RowT]):
    """``async for row in pager`` gives every row; ``pages()`` gives whole pages instead. Runs once."""

    def __init__(self, fetch: Callable[[str | None], Awaitable[Any]], cursor: str | None = None) -> None:
        self._fetch = fetch
        self._state = _Cursor(cursor)
        self.last_page: Any = None

    @property
    def next_cursor(self) -> str | None:
        return self._state.cursor

    async def pages(self) -> AsyncIterator[Any]:
        if self._state.started:
            raise RuntimeError("a pager runs once; call the iter_* method again for a new run")
        self._state.started = True
        while not self._state.done:
            page = await self._fetch(self._state.cursor)
            self.last_page = page
            self._state.advance(page)
            yield page

    async def __aiter__(self) -> AsyncIterator[RowT]:
        async for page in self.pages():
            for row in page.data:
                yield row

    async def to_list(self) -> list[RowT]:
        return [row async for row in self]

    async def to_pandas(self) -> pd.DataFrame:
        collect = _Collect()
        async for page in self.pages():
            collect.add(page)
        return collect.frame()
