"""Python client for the Qelso Data API: historical prediction-market data from Kalshi and Polymarket.

    from qelso import Qelso

    client = Qelso()  # reads QELSO_API_KEY
    page = client.get_market_prices("kalshi:KXFEDDECISION-26DEC-H0", start="2026-11-03", end="2026-11-04")

Every endpoint of the contract is a method; ``qelso.models`` holds the response models.
"""

from qelso import models
from qelso._client import AsyncQelso, Qelso
from qelso._model import QelsoModel, QelsoResponse, ResponseMeta
from qelso._paging import AsyncPager, SyncPager
from qelso._streams import AsyncStream, SyncStream
from qelso._version import __version__
from qelso.errors import (
    APIConnectionError,
    APIStatusError,
    APITimeoutError,
    AuthenticationError,
    BadRequestError,
    InternalServerError,
    InvalidRequestError,
    NotAcceptableError,
    NotFoundError,
    PermissionDeniedError,
    QelsoError,
    QueryTimeoutError,
    QuotaExceededError,
    RateLimitError,
    ServiceUnavailableError,
    UnexpectedResponseError,
)
from qelso.models import API_VERSION

__all__ = [
    "API_VERSION",
    "APIConnectionError",
    "APIStatusError",
    "APITimeoutError",
    "AsyncPager",
    "AsyncQelso",
    "AsyncStream",
    "AuthenticationError",
    "BadRequestError",
    "InternalServerError",
    "InvalidRequestError",
    "NotAcceptableError",
    "NotFoundError",
    "PermissionDeniedError",
    "Qelso",
    "QelsoError",
    "QelsoModel",
    "QelsoResponse",
    "QueryTimeoutError",
    "QuotaExceededError",
    "RateLimitError",
    "ResponseMeta",
    "ServiceUnavailableError",
    "SyncPager",
    "SyncStream",
    "UnexpectedResponseError",
    "__version__",
    "models",
]
