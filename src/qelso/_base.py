"""The clients' plumbing: sending a request with retries, and building pages, pagers and streams.

The generated operations (``_operations.py``) call ``_request_model``, ``_paginate`` and ``_stream``.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Awaitable, Iterable, Mapping
from types import TracebackType
from typing import Any, TypeVar

import httpx

from qelso import _http
from qelso._http import Item
from qelso._model import QelsoModel, QelsoResponse, ResponseMeta
from qelso._paging import AsyncPager, SyncPager
from qelso._streams import AsyncStream, SyncStream
from qelso.errors import (
    APIConnectionError,
    APITimeoutError,
    InvalidRequestError,
    QelsoError,
    error_from_response,
)

ModelT = TypeVar("ModelT", bound=QelsoResponse)
SelfT = TypeVar("SelfT")


class _Config:
    def __init__(
        self,
        api_key: str | None,
        base_url: str | None,
        max_retries: int,
        default_headers: Mapping[str, str] | None,
    ) -> None:
        self.api_key = _http.clean_key(api_key if api_key is not None else os.environ.get(_http.API_KEY_ENV))
        self.base_url = (base_url or os.environ.get(_http.BASE_URL_ENV) or _http.DEFAULT_BASE_URL).rstrip("/")
        if not self.base_url.lower().startswith(("https://", "http://")):
            raise ValueError(f"base_url must start with https:// (or http://): {self.base_url!r}")
        self.max_retries = max_retries
        self.default_headers = dict(default_headers or {})

    def url(self, template: str, path: Mapping[str, Any]) -> str:
        return self.base_url + _http.build_path(template, path)

    def headers(self, accept: str) -> dict[str, str]:
        return _http.request_headers(self.api_key, accept, self.default_headers)

    def __repr__(self) -> str:
        return f"base_url={self.base_url!r}, api_key={'set' if self.api_key else 'not set'}"


def _connection_error(
    method: str, request: httpx.Request, exc: httpx.TransportError, api_key: str | None
) -> QelsoError:
    """The error for a request that got no answer. The httpx error isn't chained: its text may hold the
    request's headers; ours names the kind of failure, with the key taken out."""
    text = _http.scrub(f"{method} {request.url.path}: {type(exc).__name__}: {exc}", api_key)
    if _http.our_fault(exc):
        return InvalidRequestError(text)
    return (APITimeoutError if isinstance(exc, httpx.TimeoutException) else APIConnectionError)(text)


def _model(response: httpx.Response, model: type[ModelT]) -> ModelT:
    obj = model.model_validate_json(response.content)
    obj._meta = ResponseMeta.from_response(response)
    return obj


class SyncBase:
    """A client's connection: key, base URL, retries and the HTTP client."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_url: str | None = None,
        timeout: float | httpx.Timeout | None = _http.DEFAULT_TIMEOUT,
        max_retries: int = _http.DEFAULT_MAX_RETRIES,
        default_headers: Mapping[str, str] | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        """
        Args:
            api_key: Your API key. Defaults to the ``QELSO_API_KEY`` environment variable.
            base_url: Defaults to ``QELSO_BASE_URL``, then ``https://api.qelso.io``.
            timeout: Seconds to wait for a connection or for data, or an ``httpx.Timeout``.
            max_retries: How often a failed request is retried: rate limits (after ``Retry-After``), outages
                and network errors. A used-up quota and the API's query timeout are never retried.
            default_headers: Extra headers on every API request.
            http_client: Your own ``httpx.Client`` (proxies, certificates); you close it.
        """
        self._config = _Config(api_key, base_url, max_retries, default_headers)
        self._owns_http = http_client is None
        self._http = http_client or httpx.Client(timeout=timeout)

    @property
    def base_url(self) -> str:
        return self._config.base_url

    def _sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def close(self) -> None:
        """Close the HTTP connections (unless you passed your own client)."""
        if self._owns_http:
            self._http.close()

    def __enter__(self: SelfT) -> SelfT:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._config!r})"

    # ---------- requests ----------
    def _send(
        self,
        method: str,
        url: str,
        *,
        query: Iterable[Item] = (),
        body: dict[str, Any] | None = None,
        accept: str = _http.JSON,
        ok: tuple[int, ...] = (),
        stream: bool = False,
        retries: int | None = None,
    ) -> httpx.Response:
        """One request, retried as ``should_retry`` allows. Returns a success (or an ``ok`` status) only.

        With ``stream=True`` the body isn't read: the caller closes the response.
        """
        params = _http.encode_query(query)
        idempotent = method == "GET"
        limit = self._config.max_retries if retries is None else retries
        attempt = 0
        while True:
            request = self._http.build_request(
                method, url, params=params, json=body, headers=self._config.headers(accept)
            )
            try:
                response = self._http.send(request, stream=stream)
            except httpx.TransportError as exc:
                retry = not _http.our_fault(exc) and (idempotent or _http.not_sent(exc))
                if retry and attempt < limit:
                    self._sleep(_http.retry_delay(attempt) or 0)
                    attempt += 1
                    continue
                raise _connection_error(method, request, exc, self._config.api_key) from None
            if response.is_success or response.status_code in ok:
                return response
            if stream:
                response.read()
                response.close()
            error = error_from_response(response)
            delay = _http.retry_delay(attempt, error.retry_after)
            retry = delay is not None and _http.should_retry(error, idempotent=idempotent)
            if retry and attempt < limit:
                self._sleep(delay or 0)
                attempt += 1
                continue
            raise error

    def _request_model(
        self,
        method: str,
        template: str,
        *,
        path: Mapping[str, Any],
        model: type[ModelT],
        query: Iterable[Item] = (),
        body: Iterable[Item] | None = None,
        accept: tuple[int, ...] = (),
    ) -> ModelT:
        response = self._send(
            method,
            self._config.url(template, path),
            query=query,
            body=_http.encode_body(body) if body is not None else None,
            ok=accept,
        )
        return _model(response, model)

    def _paginate(
        self,
        method: str,
        template: str,
        *,
        path: Mapping[str, Any],
        model: type[QelsoResponse],
        query: Iterable[Item] = (),
        cursor: str | None = None,
    ) -> SyncPager[Any]:
        query = tuple(query)

        def fetch(cursor: str | None) -> Any:
            items = (*query, ("cursor", cursor, "scalar")) if cursor else query
            return self._request_model(method, template, path=path, model=model, query=items)

        return SyncPager(fetch, cursor)

    def _stream(
        self,
        template: str,
        *,
        path: Mapping[str, Any],
        row: type[QelsoModel],
        query: Iterable[Item],
        key: tuple[str, ...],
        formats: tuple[str, ...],
        format: str,
        after: Any,
        max_resumes: int,
    ) -> SyncStream[Any]:
        accept = _http.check_format(format, formats)
        url = self._config.url(template, path)
        query = tuple(query)

        def open_stream(resume_after: str | None) -> httpx.Response:
            items = (*query, ("after", resume_after, "scalar")) if resume_after else query
            return self._send(
                "GET", url, query=items, accept=accept, stream=True, retries=0
            )  # the stream retries

        return SyncStream(
            open_stream,
            row=row,
            key=key,
            csv=format == "csv",
            media=accept,
            after=_http.format_time(after) if after is not None else None,
            max_resumes=max_resumes,
            max_retries=self._config.max_retries,
            sleep=self._sleep,
        )


class AsyncBase:
    """An async client's connection: key, base URL, retries and the HTTP client."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_url: str | None = None,
        timeout: float | httpx.Timeout | None = _http.DEFAULT_TIMEOUT,
        max_retries: int = _http.DEFAULT_MAX_RETRIES,
        default_headers: Mapping[str, str] | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        """
        Args:
            api_key: Your API key. Defaults to the ``QELSO_API_KEY`` environment variable.
            base_url: Defaults to ``QELSO_BASE_URL``, then ``https://api.qelso.io``.
            timeout: Seconds to wait for a connection or for data, or an ``httpx.Timeout``.
            max_retries: How often a failed request is retried: rate limits (after ``Retry-After``), outages
                and network errors. A used-up quota and the API's query timeout are never retried.
            default_headers: Extra headers on every API request.
            http_client: Your own ``httpx.AsyncClient`` (proxies, certificates); you close it.
        """
        self._config = _Config(api_key, base_url, max_retries, default_headers)
        self._owns_http = http_client is None
        self._http = http_client or httpx.AsyncClient(timeout=timeout)

    @property
    def base_url(self) -> str:
        return self._config.base_url

    async def _sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    async def close(self) -> None:
        """Close the HTTP connections (unless you passed your own client)."""
        if self._owns_http:
            await self._http.aclose()

    async def __aenter__(self: SelfT) -> SelfT:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._config!r})"

    # ---------- requests ----------
    async def _send(
        self,
        method: str,
        url: str,
        *,
        query: Iterable[Item] = (),
        body: dict[str, Any] | None = None,
        accept: str = _http.JSON,
        ok: tuple[int, ...] = (),
        stream: bool = False,
        retries: int | None = None,
    ) -> httpx.Response:
        params = _http.encode_query(query)
        idempotent = method == "GET"
        limit = self._config.max_retries if retries is None else retries
        attempt = 0
        while True:
            request = self._http.build_request(
                method, url, params=params, json=body, headers=self._config.headers(accept)
            )
            try:
                response = await self._http.send(request, stream=stream)
            except httpx.TransportError as exc:
                retry = not _http.our_fault(exc) and (idempotent or _http.not_sent(exc))
                if retry and attempt < limit:
                    await self._sleep(_http.retry_delay(attempt) or 0)
                    attempt += 1
                    continue
                raise _connection_error(method, request, exc, self._config.api_key) from None
            if response.is_success or response.status_code in ok:
                return response
            if stream:
                await response.aread()
                await response.aclose()
            error = error_from_response(response)
            delay = _http.retry_delay(attempt, error.retry_after)
            retry = delay is not None and _http.should_retry(error, idempotent=idempotent)
            if retry and attempt < limit:
                await self._sleep(delay or 0)
                attempt += 1
                continue
            raise error

    async def _request_model(
        self,
        method: str,
        template: str,
        *,
        path: Mapping[str, Any],
        model: type[ModelT],
        query: Iterable[Item] = (),
        body: Iterable[Item] | None = None,
        accept: tuple[int, ...] = (),
    ) -> ModelT:
        response = await self._send(
            method,
            self._config.url(template, path),
            query=query,
            body=_http.encode_body(body) if body is not None else None,
            ok=accept,
        )
        return _model(response, model)

    def _paginate(
        self,
        method: str,
        template: str,
        *,
        path: Mapping[str, Any],
        model: type[QelsoResponse],
        query: Iterable[Item] = (),
        cursor: str | None = None,
    ) -> AsyncPager[Any]:
        query = tuple(query)

        def fetch(cursor: str | None) -> Awaitable[Any]:
            items = (*query, ("cursor", cursor, "scalar")) if cursor else query
            return self._request_model(method, template, path=path, model=model, query=items)

        return AsyncPager(fetch, cursor)

    def _stream(
        self,
        template: str,
        *,
        path: Mapping[str, Any],
        row: type[QelsoModel],
        query: Iterable[Item],
        key: tuple[str, ...],
        formats: tuple[str, ...],
        format: str,
        after: Any,
        max_resumes: int,
    ) -> AsyncStream[Any]:
        accept = _http.check_format(format, formats)
        url = self._config.url(template, path)
        query = tuple(query)

        def open_stream(resume_after: str | None) -> Awaitable[httpx.Response]:
            items = (*query, ("after", resume_after, "scalar")) if resume_after else query
            return self._send(
                "GET", url, query=items, accept=accept, stream=True, retries=0
            )  # the stream retries

        return AsyncStream(
            open_stream,
            row=row,
            key=key,
            csv=format == "csv",
            media=accept,
            after=_http.format_time(after) if after is not None else None,
            max_resumes=max_resumes,
            max_retries=self._config.max_retries,
            sleep=self._sleep,
        )
