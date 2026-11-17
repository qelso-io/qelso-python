"""What the sync and async clients share: parameters on the wire, headers, and when to retry."""

from __future__ import annotations

import random
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import quote

import httpx

from qelso._version import __version__
from qelso.errors import APIStatusError

DEFAULT_BASE_URL = "https://api.qelso.io"
API_KEY_ENV = "QELSO_API_KEY"
BASE_URL_ENV = "QELSO_BASE_URL"
DEFAULT_TIMEOUT = httpx.Timeout(60.0, connect=10.0)
DEFAULT_MAX_RETRIES = 3
USER_AGENT = f"qelso-python/{__version__}"

JSON = "application/json"
NDJSON = "application/x-ndjson"
CSV = "text/csv"
MEDIA_TYPES = {"ndjson": NDJSON, "csv": CSV}

# One query or body item, as the generated code passes it: (name on the wire, value, how to write it).
Item = tuple[str, Any, str]
# Query parameters as httpx takes them, repeated names allowed.
QueryList = list[tuple[str, str | int | float | bool | None]]

BACKOFF_BASE = 0.5
BACKOFF_MAX = 8.0
RETRY_AFTER_MAX = 120.0  # wait at most this long for a Retry-After; longer, and the error is raised


def format_time(value: Any) -> str:
    """A time parameter as the API reads it.

    Strings and integers (epoch milliseconds) go as they are. A ``datetime`` is written in UTC with
    milliseconds (a naive one is taken as UTC, as the API reads a time without an offset); a ``date`` as
    ``YYYY-MM-DD``.
    """
    if isinstance(value, datetime):
        moment: datetime = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        moment = moment.astimezone(timezone.utc)
        return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        raise TypeError("a time must be a string, epoch milliseconds, a datetime or a date, not a bool")
    if isinstance(value, (int, str)):
        return str(value)
    raise TypeError(
        f"a time must be a string, epoch milliseconds, a datetime or a date, not {type(value).__name__}"
    )


def _date(value: Any) -> str:
    if isinstance(value, datetime):
        return format_time(value)[:10]
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _values(value: Any) -> list[str]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Iterable):
        return [_scalar(value)]
    return [_scalar(v) for v in value]


def encode_query(items: Iterable[Item]) -> QueryList:
    """Query parameters in the contract's styles: repeated (``venue=a&venue=b``) or comma-separated."""
    out: QueryList = []
    for name, value, how in items:
        if value is None:
            continue
        if how == "time":
            out.append((name, format_time(value)))
        elif how == "date":
            out.append((name, _date(value)))
        elif how == "repeat":
            out.extend((name, v) for v in _values(value))
        elif how == "csv":
            out.append((name, ",".join(_values(value))))
        else:
            out.append((name, _scalar(value)))
    return out


def encode_body(items: Iterable[Item]) -> dict[str, Any]:
    body: dict[str, Any] = {}
    for name, value, how in items:
        if value is None:
            continue
        if how == "time":
            # In a JSON body, epoch milliseconds are a number (the contract's TimestampInput), not a string.
            body[name] = (
                value if isinstance(value, int) and not isinstance(value, bool) else format_time(value)
            )
        elif how == "model":
            body[name] = (
                value.model_dump(mode="json", exclude_none=True)
                if hasattr(value, "model_dump")
                else dict(value)
            )
        else:
            body[name] = value
    return body


def build_path(template: str, params: Mapping[str, Any]) -> str:
    """The path with its parameters filled in: a market ID keeps its colon, anything unsafe is escaped."""
    return template.format(**{name: quote(str(value), safe=":") for name, value in params.items()})


def clean_key(api_key: str | None) -> str | None:
    """The key as sent: surrounding whitespace dropped (a key read from a file ends in a newline). A key
    with any other character a header can't carry is refused, without showing it."""
    if api_key is None:
        return None
    key = api_key.strip()
    if any(not (32 < ord(c) < 127) for c in key):
        raise ValueError(
            "the API key has a character an HTTP header can't carry (a space, newline or non-ASCII)"
        )
    return key


def scrub(text: str, api_key: str | None) -> str:
    """An error text with the key taken out, should a library have quoted it."""
    return text.replace(api_key, "***") if api_key else text


def request_headers(api_key: str | None, accept: str, extra: Mapping[str, str]) -> dict[str, str]:
    headers = {"Accept": accept, "User-Agent": USER_AGENT, **extra}
    if api_key:
        headers["X-API-Key"] = api_key
    return headers


def should_retry(error: APIStatusError, *, idempotent: bool) -> bool:
    """Retry rate limits (never a used-up quota), outages and our 500s; never the API's query timeout.

    A request that changes something (a POST) is retried only when the API refused it before doing
    anything: a rate limit.
    """
    if error.status_code == 429:
        return error.code != "quota_exceeded"
    if not idempotent:
        return False
    if error.code == "timeout":
        return False
    return error.status_code in (500, 502, 503, 504)


def our_fault(exc: httpx.TransportError) -> bool:
    """A request httpx refused to send (a bad header or URL): sending it again can't help."""
    return isinstance(exc, (httpx.LocalProtocolError, httpx.UnsupportedProtocol))


def not_sent(exc: httpx.TransportError) -> bool:
    """True when the request never reached the server, so even a POST may be sent again."""
    return isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout))


def retry_delay(attempt: int, retry_after: float | None = None) -> float | None:
    """Seconds to wait before retry number ``attempt + 1``; None if the API asks for longer than we wait."""
    if retry_after is not None:
        if retry_after > RETRY_AFTER_MAX:
            return None
        return retry_after + random.uniform(0, 0.25)
    delay = min(BACKOFF_BASE * 2.0**attempt, BACKOFF_MAX)
    return delay + random.uniform(0, delay * 0.25)


def check_format(format: str, formats: Sequence[str]) -> str:
    if format not in formats:
        raise ValueError(f"format must be one of {', '.join(formats)}, not {format!r}")
    return MEDIA_TYPES[format]
