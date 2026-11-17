"""The base classes of the generated models, and what every response carries besides its body."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

import httpx
from pydantic import BaseModel, ConfigDict, PrivateAttr


def _int(value: str | None) -> int | None:
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None


@dataclass(frozen=True)
class ResponseMeta:
    """What an answer's headers say."""

    status_code: int
    request_id: str | None
    """Quote it when you contact support."""
    units: int | None
    """Units this request cost (``Qelso-Units``)."""
    rate_limit: int | None
    """Units your organisation may use per minute; None when unlimited."""
    rate_limit_remaining: int | None
    """Units left in the current minute."""
    rate_limit_reset: int | None
    """Epoch seconds when the current minute resets."""
    attribution: str | None
    """``Powered by Kalshi Research`` when the answer holds Kalshi data."""
    data_type: str | None
    """``historical`` or ``delayed`` when the answer holds Kalshi data."""
    headers: Mapping[str, str] = field(repr=False)

    @classmethod
    def from_response(cls, response: httpx.Response) -> ResponseMeta:
        h = response.headers
        return cls(
            status_code=response.status_code,
            request_id=h.get("x-request-id"),
            units=_int(h.get("qelso-units")),
            rate_limit=_int(h.get("x-ratelimit-limit")),
            rate_limit_remaining=_int(h.get("x-ratelimit-remaining")),
            rate_limit_reset=_int(h.get("x-ratelimit-reset")),
            attribution=h.get("qelso-attribution"),
            data_type=h.get("qelso-data-type"),
            headers=h,
        )


class QelsoModel(BaseModel):
    """An object of the contract. Fields the contract adds later are kept in ``model_extra``."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class QelsoResponse(QelsoModel):
    """A whole response body: a page, or one object."""

    _meta: ResponseMeta | None = PrivateAttr(default=None)

    @property
    def response_meta(self) -> ResponseMeta | None:
        """The answer's request ID, cost and rate-limit state; None for an object you built yourself."""
        return self._meta
