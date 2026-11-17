"""The errors this package raises.

Every error the API answers carries the contract's stable ``code`` (``invalid_parameter``,
``forbidden_venue``, ``rate_limited``, ...), its ``message``, the ``request_id`` to quote to support, and
``details`` (the parameter at fault, a limit, a reason). The class follows the HTTP status; ``code`` says
exactly what happened.
"""

from __future__ import annotations

import json
import math
from typing import Any

import httpx


class QelsoError(Exception):
    """Base class of every error this package raises."""


class APIConnectionError(QelsoError):
    """No usable answer: the network failed, or a stream broke more often than ``max_resumes``."""


class APITimeoutError(APIConnectionError):
    """The request ran over the client's own ``timeout``."""


class APIStatusError(QelsoError):
    """The API answered with an error (HTTP 4xx or 5xx)."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        code: str | None,
        request_id: str | None,
        details: dict[str, Any],
        headers: httpx.Headers,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.code = code
        """The contract's error code; None when the answer didn't come from the API (a proxy's page)."""
        self.request_id = request_id
        self.details = details
        self.headers = headers

    @property
    def retry_after(self) -> float | None:
        """Seconds to wait before retrying, from ``Retry-After``; None when the API sent none."""
        value = self.headers.get("retry-after")
        try:
            seconds = float(value) if value is not None else None
        except ValueError:
            return None
        if seconds is None or not math.isfinite(seconds):
            return None
        return max(seconds, 0.0)

    def __str__(self) -> str:
        code = f" {self.code}" if self.code else ""
        request = f" (request {self.request_id})" if self.request_id else ""
        return f"{self.status_code}{code}: {self.message}{request}"


class BadRequestError(APIStatusError):
    """400: ``invalid_parameter``, ``invalid_cursor`` or ``window_too_large``."""


class AuthenticationError(APIStatusError):
    """401 ``unauthorized``: the key is missing, invalid, revoked or expired."""


class PermissionDeniedError(APIStatusError):
    """403: ``forbidden_venue``, ``forbidden_dataset``, ``plan_history_limit``,
    ``plan_resolution_not_allowed`` or ``suspended``. ``details`` says which venue, dataset or limit."""


class NotFoundError(APIStatusError):
    """404 ``not_found``: unknown market, event, series or path."""


class NotAcceptableError(APIStatusError):
    """406 ``not_acceptable``: the endpoint can't answer in the format asked for."""


class RateLimitError(APIStatusError):
    """429 ``rate_limited``: over the units per minute or the requests at once. Retried by the client, after
    ``Retry-After`` seconds, up to ``max_retries`` times."""


class QuotaExceededError(APIStatusError):
    """429 ``quota_exceeded``: a monthly quota is used up. Never retried; ``details["resets_at"]`` says
    when it resets."""


class InternalServerError(APIStatusError):
    """500 ``internal_error``, or another 5xx from on the way. Retried with backoff."""


class ServiceUnavailableError(APIStatusError):
    """503 ``service_unavailable`` or ``metadata_refreshing``. Retried after ``Retry-After`` seconds."""


class QueryTimeoutError(APIStatusError):
    """504 ``timeout``: the query ran over the API's time limit. Not retried: narrow the window, or use a
    stream."""


class InvalidRequestError(QelsoError, ValueError):
    """The request can't be sent as configured (a malformed base URL or header). Never retried."""


class UnexpectedResponseError(QelsoError):
    """The API answered something the contract doesn't allow here (a JSON page for a stream, a resumed CSV
    stream with other columns)."""


_BY_STATUS: dict[int, type[APIStatusError]] = {
    400: BadRequestError,
    401: AuthenticationError,
    403: PermissionDeniedError,
    404: NotFoundError,
    406: NotAcceptableError,
    429: RateLimitError,
    500: InternalServerError,
    503: ServiceUnavailableError,
    504: QueryTimeoutError,
}


def error_from_response(response: httpx.Response) -> APIStatusError:
    """The error an answer stands for. The body must already be read."""
    status = response.status_code
    code: str | None = None
    message = ""
    request_id = response.headers.get("x-request-id")
    details: dict[str, Any] = {}
    try:
        body = json.loads(response.content)
    except ValueError:
        body = None
    error = body.get("error") if isinstance(body, dict) else None
    if isinstance(error, dict):
        code = error.get("code") if isinstance(error.get("code"), str) else None
        message = str(error.get("message") or "")
        request_id = error.get("request_id") or request_id
        if isinstance(error.get("details"), dict):
            details = error["details"]
    if not message:
        text = response.text.strip()
        message = text[:200] if text else (response.reason_phrase or "HTTP error")
    if status == 429 and code == "quota_exceeded":
        cls: type[APIStatusError] = QuotaExceededError
    elif status == 504 and code != "timeout":
        cls = InternalServerError  # a gateway's 504, not the API's query timeout: worth a retry
    else:
        cls = _BY_STATUS.get(status) or (InternalServerError if status >= 500 else APIStatusError)
    return cls(
        message,
        status_code=status,
        code=code,
        request_id=request_id,
        details=details,
        headers=response.headers,
    )
