"""DataFrames from rows. pandas is optional: ``pip install 'qelso[pandas]'``.

Columns follow the contract's field order; times become UTC timestamps. Books come in long form, one row per
price level: ``t``, ``last_change``, ``side`` (``bid`` or ``ask``), ``level`` (0 = best), ``price``, ``size``;
a time with no levels (an empty book, or no data) keeps one row with an empty ``side``.
"""

from __future__ import annotations

import io
import types
import typing
from datetime import datetime
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

if TYPE_CHECKING:
    import pandas as pd

BOOK_COLUMNS = ["t", "last_change", "side", "level", "price", "size"]


def _pandas() -> Any:
    try:
        import pandas
    except ImportError as exc:
        raise ImportError("DataFrames need pandas: pip install 'qelso[pandas]'") from exc
    return pandas


def _types(annotation: Any) -> set[Any]:
    if typing.get_origin(annotation) in (typing.Union, types.UnionType):
        return set().union(*(_types(arg) for arg in typing.get_args(annotation)))
    if typing.get_origin(annotation) is typing.Literal:
        return {str}
    return {annotation}


def _columns_of(model: type[BaseModel] | None, kind: type) -> list[str]:
    if model is None:
        return []
    return [name for name, info in model.model_fields.items() if kind in _types(info.annotation)]


def _is_book(model: type[BaseModel] | None) -> bool:
    return model is not None and {"bids", "asks"} <= set(model.model_fields)


def _book_levels(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for record in records:
        t, last_change = record.get("t"), record.get("last_change")
        levels = [
            (side, i, level)
            for side, key in (("bid", "bids"), ("ask", "asks"))
            for i, level in enumerate(record.get(key) or [])
        ]
        if not levels:
            out.append(dict.fromkeys(BOOK_COLUMNS) | {"t": t, "last_change": last_change})
        for side, i, (price, size) in levels:
            out.append(
                {"t": t, "last_change": last_change, "side": side, "level": i, "price": price, "size": size}
            )
    return out


def _finish(df: pd.DataFrame, order: list[str], times: list[str]) -> pd.DataFrame:
    pd = _pandas()
    known = [c for c in order if c in df.columns]
    df = df[known + [c for c in df.columns if c not in known]]
    for column in times:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], utc=True, format="ISO8601")
    return df


def frame(records: list[dict[str, Any]], model: type[BaseModel] | None) -> pd.DataFrame:
    """Rows (dicts with the contract's field names) in a DataFrame."""
    pd = _pandas()
    if _is_book(model):
        records, order, times = _book_levels(records), BOOK_COLUMNS, ["t", "last_change"]
    else:
        order = list(model.model_fields) if model is not None else []
        times = _columns_of(model, datetime)
    df = pd.DataFrame.from_records(records) if records else pd.DataFrame(columns=order)
    return _finish(df, order, times)


def frame_from_csv(data: bytes, model: type[BaseModel]) -> pd.DataFrame:
    """A CSV stream's bytes (header first) in a DataFrame: pandas' own parser, no Python object per row."""
    pd = _pandas()
    strings = {c: str for c in _columns_of(model, str) if c not in _columns_of(model, datetime)}
    df = pd.read_csv(
        io.BytesIO(data),
        dtype=strings,
        keep_default_na=False,
        na_values=[""],
        true_values=["true"],
        false_values=["false"],
    )
    return _finish(df, list(model.model_fields), _columns_of(model, datetime))
