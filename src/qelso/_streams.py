"""Streams: one request for every row of a window, as NDJSON or CSV, resumed when the connection breaks.

The contract's "Streams": a stream that ends normally has delivered every row we hold for the window; one that
fails after its first byte is cut without finishing, so the HTTP client raises. Every row ends with a
newline, so a stream that stops inside a row was cut too, even if the HTTP client saw a normal end.

- **Resuming.** After a break the stream asks again with ``after=`` set to the last row's key (``<t>``,
  ``<ts>,<seq>`` or ``<ts>,<seq>,<side>,<price>``), taken from that row's own text, never re-formatted, so no
  row is repeated or skipped.
- **Waiting.** A break that follows new rows is resumed at once. A failed request, or a break with no new row
  since the last one, waits 1, 2, 4 … 30 seconds first, so an outage (a redeploy, a DNS failover) is ridden
  out. ``max_resumes`` counts such failures in a row; any new row resets the count.
- **book-updates** starts with the book at ``start`` (``kind: snapshot`` rows), and a resumed stream skips
  that book. So the starting book is held back until its first ``update`` row (or the end) arrives: a break
  inside it starts the stream again without ``after``, and nothing is given twice.
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import httpx

from qelso import _http, _pandas
from qelso._model import QelsoModel, ResponseMeta
from qelso.errors import (
    APIConnectionError,
    APIStatusError,
    InternalServerError,
    QelsoError,
    RateLimitError,
    ServiceUnavailableError,
    UnexpectedResponseError,
)

if TYPE_CHECKING:
    import pandas as pd

RowT = TypeVar("RowT")
BOOK_KEY = ("ts", "seq", "side", "price")  # book-updates: the stream that starts with a book
WAIT_MAX = 30.0


class _Cut(Exception):
    """The stream stopped inside a row."""


# What breaks a stream mid-way: the connection reset or cut before the last chunk, a read timeout, a
# compressed body cut short, or a stream that stops inside a row.
BROKEN = (httpx.TransportError, httpx.DecodingError, _Cut)
# What a new request may fail with and still be worth another try.
PASSING = (APIConnectionError, RateLimitError, ServiceUnavailableError, InternalServerError)


class _Lines:
    """Splits a byte stream into whole lines; a partial line waits for its newline."""

    def __init__(self) -> None:
        self.pending = b""

    def feed(self, chunk: bytes) -> list[bytes]:
        data = self.pending + chunk if self.pending else chunk
        lines = data.split(b"\n")
        self.pending = lines.pop()
        return [line.rstrip(b"\r") for line in lines if line.strip()]


def _csv_cells(line: bytes) -> list[str]:
    return next(csv.reader([line.decode("utf-8")]))


class _Reader:
    """The state a stream keeps across its requests: what it delivered, and where to resume."""

    def __init__(self, row: type[QelsoModel], key: tuple[str, ...], csv: bool, after: str | None) -> None:
        self.row = row
        self.key = key
        self.csv = csv
        self.first_after = after
        self.header: bytes | None = None
        self.columns: list[str] | None = None
        self.delivered = 0
        self.last: bytes | None = None
        self.resumes = 0
        self.failures = 0  # in a row, without a new row
        self.holds_book = key == BOOK_KEY
        # per request
        self.expect_header = csv
        self.in_book = False
        self.held: list[bytes] = []

    def start(self, after: str | None) -> None:
        """A new request, asked with this ``after``."""
        self.expect_header = self.csv
        self.in_book = self.holds_book and after is None
        self.held = []

    def take(self, line: bytes) -> list[bytes]:
        """The rows to deliver now, from this line: none for the CSV header or a held starting-book row."""
        if self.expect_header:
            self.expect_header = False
            columns = _csv_cells(line)
            if self.columns is None:
                self.header, self.columns = line, columns
            elif columns != self.columns:
                raise UnexpectedResponseError(
                    "the resumed stream's CSV columns differ from the first request's"
                )
            return []
        if self.in_book:
            if self.raw(line).get("kind") == "snapshot":
                self.held.append(line)
                return []
            self.in_book = False
            rows, self.held = [*self.held, line], []
            return rows
        return [line]

    def end(self) -> list[bytes]:
        """A normal end: a starting book with no change after it is whole now."""
        self.in_book = False
        rows, self.held = self.held, []
        return rows

    def mark(self, line: bytes) -> bytes:
        """A row handed to the caller, counted just before it goes: the stream resumes after it from now."""
        self.last = line
        self.delivered += 1
        self.failures = 0
        return line

    def raw(self, line: bytes) -> dict[str, Any]:
        """A row's values as the API wrote them (numbers kept as their text)."""
        if self.csv:
            if self.columns is None:
                raise UnexpectedResponseError("a CSV stream sent a row before its header")
            return dict(zip(self.columns, _csv_cells(line), strict=False))
        value = json.loads(line, parse_float=str, parse_int=str)
        if not isinstance(value, dict):
            raise UnexpectedResponseError("the stream sent a line that isn't a JSON object")
        return value

    @property
    def resume_after(self) -> str | None:
        if self.last is None:
            return self.first_after
        row = self.raw(self.last)
        if self.holds_book and row.get("kind") == "snapshot":
            return self.first_after  # inside the starting book: a resumed stream would skip the rest of it
        try:
            return ",".join(str(row[k]) for k in self.key)
        except KeyError as missing:
            raise UnexpectedResponseError(f"can't resume: the last row has no {missing}") from None

    def failed(self, exc: BaseException, max_resumes: int, max_retries: int) -> float:
        """A break or a failed request: the seconds to wait before asking again (raises when out of tries).

        Until the stream has given a row, the client's own rule applies: ``max_retries``, short waits, and the
        error raised as it came. Once it has (or when it resumes from ``after``), failures in a row up to
        ``max_resumes`` are waited out: at once after new rows, then 1, 2, 4 … 30 s. ``Retry-After`` is
        honoured either way, and one longer than the client waits for ends it."""
        self.failures += 1
        self.resumes += 1
        self.held, self.in_book = [], False
        retry_after = exc.retry_after if isinstance(exc, APIStatusError) else None
        patient = self.delivered > 0 or self.first_after is not None
        out = self.failures > (max_resumes if patient else max_retries)
        if out or (retry_after is not None and retry_after > _http.RETRY_AFTER_MAX):
            if patient:
                raise APIConnectionError(
                    f"the stream failed {self.failures} times in a row without a new row ({exc}); "
                    f"{self.delivered} rows delivered; ask again with after={self.resume_after!r}"
                ) from exc
            if isinstance(exc, QelsoError):
                raise exc
            raise APIConnectionError(
                f"the stream broke before its first row ({type(exc).__name__}: {exc})"
            ) from None
        if patient:
            wait = 0.0 if self.failures == 1 else min(2.0 ** (self.failures - 2), WAIT_MAX)
        else:
            wait = _http.retry_delay(self.failures - 1) or 0.0
        return max(wait, retry_after or 0.0)

    def adopt(self, path: Path) -> int:
        """Take over a file a stream was saving (``save(path, resume=True)``): its whole lines become the rows
        delivered so far. Returns how many of its bytes to keep (0: start the file again). Reads only the
        file's ends and counts its lines, so a large file costs no memory."""
        with path.open("rb") as handle:
            first = handle.readline()
            size = handle.seek(0, os.SEEK_END)
            end = size
            while end > 0 and not _ends_line(handle, end):
                end -= 1  # drop a partial last line
            if end == 0:
                return 0
            last = _line_before(handle, end)
            count = _count_lines(handle, end)
        if self.csv:
            self.header, self.columns = first.rstrip(b"\r\n"), _csv_cells(first)
            count -= 1
        if count == 0:
            return len(first) if self.csv else 0
        if self.holds_book and self.raw(last).get("kind") == "snapshot":
            self.header = self.columns = None  # only part of a starting book: begin again
            return 0
        self.last, self.delivered = last, count
        self.first_after = self.resume_after
        return end

    def parse(self, line: bytes) -> Any:
        if not self.csv:
            return self.row.model_validate_json(line)
        assert self.columns is not None
        cells = _csv_cells(line)
        return self.row.model_validate(
            {c: (v if v != "" else None) for c, v in zip(self.columns, cells, strict=False)}
        )


def _ends_line(handle: Any, end: int) -> bool:
    handle.seek(end - 1)
    return bool(handle.read(1) == b"\n")


def _line_before(handle: Any, end: int) -> bytes:
    """The whole line that ends at byte ``end`` (its newline included in ``end``)."""
    start, block = end - 1, 1 << 16
    while start > 0:
        step = min(block, start)
        handle.seek(start - step)
        chunk = handle.read(step)
        cut = chunk.rfind(b"\n")
        if cut != -1:
            start = start - step + cut + 1
            break
        start -= step
    handle.seek(start)
    return bytes(handle.read(end - start)).rstrip(b"\r\n")


def _count_lines(handle: Any, end: int) -> int:
    handle.seek(0)
    count, left = 0, end
    while left:
        chunk = handle.read(min(1 << 20, left))
        count += chunk.count(b"\n")
        left -= len(chunk)
    return count


def _check_media(response: httpx.Response, media: str) -> None:
    content_type = response.headers.get("content-type", "").split(";")[0].strip().lower()
    if content_type != media:
        raise UnexpectedResponseError(
            f"asked for a {media} stream; the API answered {content_type or 'nothing'}"
        )


class _StreamBase:
    _reader: _Reader
    response_meta: ResponseMeta | None

    @property
    def attribution(self) -> str | None:
        """``Powered by Kalshi Research`` when the stream holds Kalshi data; show it with the data."""
        return self.response_meta.attribution if self.response_meta else None

    @property
    def data_type(self) -> str | None:
        """``historical`` or ``delayed`` when the stream holds Kalshi data."""
        return self.response_meta.data_type if self.response_meta else None

    @property
    def columns(self) -> list[str] | None:
        """A CSV stream's columns, once its header has arrived."""
        return self._reader.columns

    @property
    def resumes(self) -> int:
        """How many times the stream asked again after a break or a failed request."""
        return self._reader.resumes

    @property
    def rows(self) -> int:
        """Rows delivered so far."""
        return self._reader.delivered

    @property
    def resume_after(self) -> str | None:
        """The ``after=`` that continues this stream from the last row delivered (None: from the start)."""
        return self._reader.resume_after

    def _frame(self, lines: list[bytes]) -> pd.DataFrame:
        reader = self._reader
        if reader.csv:
            df = _pandas.frame_from_csv(b"\n".join([reader.header or b"", *lines]), reader.row)
        else:
            df = _pandas.frame([json.loads(line) for line in lines], reader.row)
        if self.attribution:
            df.attrs["attribution"] = self.attribution
            df.attrs["data_type"] = self.data_type
        return df

    def _meta(self, response: httpx.Response) -> None:
        if self.response_meta is None:
            self.response_meta = ResponseMeta.from_response(response)


def _paths(path: str | os.PathLike[str]) -> tuple[Path, Path]:
    target = Path(path)
    return target, target.with_name(target.name + ".part")


class _Saving:
    """The file a stream is saved to: ``<path>.part`` until the stream ends normally, then ``<path>``."""

    def __init__(self, reader: _Reader, path: str | os.PathLike[str], resume: bool) -> None:
        self.reader = reader
        self.target, self.part = _paths(path)
        kept = reader.adopt(self.part) if resume and self.part.exists() else 0
        self.adopted = reader.delivered
        self.out = self.part.open("r+b" if kept else "wb")
        self.out.truncate(kept)
        self.out.seek(kept)
        self.wrote_header = bool(kept and reader.header)

    def write(self, line: bytes) -> None:
        if not self.wrote_header and self.reader.header:
            self.out.write(self.reader.header + b"\n")
            self.wrote_header = True
        self.out.write(line + b"\n")

    def finish(self) -> int:
        if not self.wrote_header and self.reader.header:
            self.out.write(self.reader.header + b"\n")
        self.out.close()
        os.replace(self.part, self.target)
        return self.reader.delivered

    def fail(self) -> None:
        """Keep what arrived, for ``save(path, resume=True)``; a file that got nothing is removed."""
        self.out.close()
        if self.reader.delivered == self.adopted == 0:
            self.part.unlink(missing_ok=True)


class SyncStream(_StreamBase, Generic[RowT]):
    """Every row of a window from one request: ``for row in stream`` (models), ``iter_lines()`` (raw lines),
    ``to_pandas()`` or ``save(path)``. Runs once; use it in a ``with`` block to close it early."""

    def __init__(
        self,
        open_stream: Callable[[str | None], httpx.Response],
        *,
        row: type[QelsoModel],
        key: tuple[str, ...],
        csv: bool,
        media: str,
        after: str | None,
        max_resumes: int,
        max_retries: int,
        sleep: Callable[[float], None],
    ) -> None:
        self._open = open_stream
        self._reader = _Reader(row, key, csv, after)
        self._media = media
        self._max_resumes = max_resumes
        self._max_retries = max_retries
        self._sleep = sleep
        self._lines: Iterator[bytes] | None = None
        self.response_meta: ResponseMeta | None = None
        """The first answer's headers (request ID, units, attribution)."""

    def iter_lines(self) -> Iterator[bytes]:
        """The rows as the API sent them, one line each, without the CSV header (``columns`` has it)."""
        if self._lines is not None:
            raise RuntimeError("a stream runs once; call the stream_* method again for a new one")
        self._lines = self._run()
        return self._lines

    def _run(self) -> Iterator[bytes]:
        reader = self._reader
        while True:
            after = reader.resume_after
            try:
                response = self._open(after)
            except PASSING as exc:
                self._sleep(reader.failed(exc, self._max_resumes, self._max_retries))
                continue
            try:
                _check_media(response, self._media)
                self._meta(response)
                reader.start(after)
                lines = _Lines()
                for chunk in response.iter_bytes():
                    for line in lines.feed(chunk):
                        for row in reader.take(line):
                            yield reader.mark(row)
                if lines.pending.strip():
                    raise _Cut("the stream stopped inside a row")
                for row in reader.end():
                    yield reader.mark(row)
                return
            except BROKEN as exc:
                wait = reader.failed(exc, self._max_resumes, self._max_retries)
            finally:
                response.close()
            self._sleep(wait)

    def __iter__(self) -> Iterator[RowT]:
        for line in self.iter_lines():
            yield self._reader.parse(line)

    def to_list(self) -> list[RowT]:
        return list(self)

    def to_pandas(self) -> pd.DataFrame:
        """Every row in a DataFrame (needs ``qelso[pandas]``). Times are UTC; books come one row per level;
        Kalshi's attribution is in ``df.attrs``."""
        return self._frame(list(self.iter_lines()))

    def save(self, path: str | os.PathLike[str], *, resume: bool = False) -> int:
        """Write the stream to a file as it arrives (CSV with one header, or NDJSON); returns the rows in it.

        The file is ``path.part`` until the stream ends normally, then ``path``. If the stream fails, the
        ``.part`` file keeps what arrived: call ``save(path, resume=True)`` on a new stream with the same
        parameters to continue it from its last row.
        """
        saving = _Saving(self._reader, path, resume)
        try:
            for line in self.iter_lines():
                saving.write(line)
        except BaseException:
            saving.fail()
            raise
        return saving.finish()

    def close(self) -> None:
        """Stop reading and close the connection."""
        if self._lines is not None and hasattr(self._lines, "close"):
            self._lines.close()

    def __enter__(self) -> SyncStream[RowT]:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


class AsyncStream(_StreamBase, Generic[RowT]):
    """``async for row in stream`` gives every row of the window; ``aiter_lines()``, ``to_pandas()`` and
    ``save(path)`` as on the sync stream. Runs once; use ``async with`` to close it early."""

    def __init__(
        self,
        open_stream: Callable[[str | None], Awaitable[httpx.Response]],
        *,
        row: type[QelsoModel],
        key: tuple[str, ...],
        csv: bool,
        media: str,
        after: str | None,
        max_resumes: int,
        max_retries: int,
        sleep: Callable[[float], Awaitable[None]],
    ) -> None:
        self._open = open_stream
        self._reader = _Reader(row, key, csv, after)
        self._media = media
        self._max_resumes = max_resumes
        self._max_retries = max_retries
        self._sleep = sleep
        self._lines: AsyncIterator[bytes] | None = None
        self.response_meta: ResponseMeta | None = None

    def aiter_lines(self) -> AsyncIterator[bytes]:
        if self._lines is not None:
            raise RuntimeError("a stream runs once; call the stream_* method again for a new one")
        self._lines = self._run()
        return self._lines

    async def _run(self) -> AsyncIterator[bytes]:
        reader = self._reader
        while True:
            after = reader.resume_after
            try:
                response = await self._open(after)
            except PASSING as exc:
                await self._sleep(reader.failed(exc, self._max_resumes, self._max_retries))
                continue
            try:
                _check_media(response, self._media)
                self._meta(response)
                reader.start(after)
                lines = _Lines()
                async for chunk in response.aiter_bytes():
                    for line in lines.feed(chunk):
                        for row in reader.take(line):
                            yield reader.mark(row)
                if lines.pending.strip():
                    raise _Cut("the stream stopped inside a row")
                for row in reader.end():
                    yield reader.mark(row)
                return
            except BROKEN as exc:
                wait = reader.failed(exc, self._max_resumes, self._max_retries)
            finally:
                await response.aclose()
            await self._sleep(wait)

    async def __aiter__(self) -> AsyncIterator[RowT]:
        async for line in self.aiter_lines():
            yield self._reader.parse(line)

    async def to_list(self) -> list[RowT]:
        return [row async for row in self]

    async def to_pandas(self) -> pd.DataFrame:
        return self._frame([line async for line in self.aiter_lines()])

    async def save(self, path: str | os.PathLike[str], *, resume: bool = False) -> int:
        saving = await asyncio.to_thread(_Saving, self._reader, path, resume)  # may count a large file
        try:
            async for line in self.aiter_lines():
                saving.write(line)
        except BaseException:
            saving.fail()
            raise
        return saving.finish()

    async def close(self) -> None:
        if self._lines is not None and hasattr(self._lines, "aclose"):
            await self._lines.aclose()

    async def __aenter__(self) -> AsyncStream[RowT]:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()
