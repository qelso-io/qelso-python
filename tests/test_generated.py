"""The generated code against the contract: every operation is a method, every object a model, and the
models read whatever the contract allows (nulls, missing optional fields, later additions)."""

from __future__ import annotations

import inspect
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, ValidationError
from support import CONTRACT

from qelso import AsyncQelso, Qelso, models

SDK = Path(__file__).resolve().parents[1]
SCHEMAS = CONTRACT["components"]["schemas"]
PARAMETERS = CONTRACT["components"]["parameters"]
OPERATIONS = [(path, method, op) for path, item in CONTRACT["paths"].items() for method, op in item.items()]


def snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def stem(op: dict[str, Any]) -> str:
    return re.sub(r"^(get|list)_", "", snake(op["operationId"]))


def wire_params(op: dict[str, Any]) -> list[str]:
    names = []
    for spec in op.get("parameters", []):
        if "$ref" in spec:
            spec = PARAMETERS[spec["$ref"].rsplit("/", 1)[-1]]
        names.append(spec["name"])
    if "requestBody" in op:
        ref = op["requestBody"]["content"]["application/json"]["schema"]["$ref"].rsplit("/", 1)[-1]
        names += list(SCHEMAS[ref]["properties"])
    return names


def python_params(method: Any) -> list[str]:
    return [p for p in inspect.signature(method).parameters if p != "self"]


def py(name: str) -> str:
    return name + "_" if name == "from" else name


def pages(op: dict[str, Any]) -> bool:
    schema = (
        op["responses"]["200"]["content"]["application/json"]["schema"] if "200" in op["responses"] else {}
    )
    ref = schema.get("$ref", "").rsplit("/", 1)[-1]
    return "next_cursor" in SCHEMAS.get(ref, {}).get("properties", {})


def streams(op: dict[str, Any]) -> bool:
    return "application/x-ndjson" in op["responses"].get("200", {}).get("content", {})


def test_the_generated_files_are_up_to_date() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/generate.py", "--check"], cwd=SDK, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("client_class", [Qelso, AsyncQelso])
@pytest.mark.parametrize(
    ("path", "method", "op"), OPERATIONS, ids=[op["operationId"] for *_, op in OPERATIONS]
)
def test_every_operation_is_a_method_with_the_contracts_parameters(
    client_class: type, path: str, method: str, op: dict[str, Any]
) -> None:
    name = snake(op["operationId"])
    call = getattr(client_class, name)
    assert inspect.iscoroutinefunction(call) == (client_class is AsyncQelso)
    expected = [py(n) for n in wire_params(op) if n != "after"]
    assert sorted(python_params(call)) == sorted(expected)
    if pages(op):
        pager = getattr(client_class, f"iter_{stem(op)}")
        assert sorted(python_params(pager)) == sorted(expected)  # cursor: carry on a run
    else:
        assert not hasattr(client_class, f"iter_{stem(op)}")
    if streams(op):
        stream = getattr(client_class, f"stream_{stem(op)}")
        own = {"after", "format", "max_resumes"}
        assert sorted(python_params(stream)) == sorted(
            [p for p in expected if p not in ("cursor", "limit")] + sorted(own)
        )
    else:
        assert not hasattr(client_class, f"stream_{stem(op)}")


def test_no_method_without_an_operation() -> None:
    known = {snake(op["operationId"]) for *_, op in OPERATIONS}
    known |= {f"iter_{stem(op)}" for *_, op in OPERATIONS if pages(op)}
    known |= {f"stream_{stem(op)}" for *_, op in OPERATIONS if streams(op)}
    helpers = {"close", "base_url"}
    public = {n for n in dir(Qelso) if not n.startswith("_")}
    assert public == known | helpers


# ---------- models ----------
MODEL_SCHEMAS = {name: s for name, s in SCHEMAS.items() if s.get("type") == "object" and "properties" in s}


def resolve(schema: dict[str, Any]) -> dict[str, Any]:
    while "$ref" in schema:
        schema = SCHEMAS[schema["$ref"].rsplit("/", 1)[-1]]
    return schema


def sample(schema: dict[str, Any], mode: str) -> Any:
    """A value the schema allows: ``nulls`` makes every nullable value null and leaves optional fields out;
    ``full`` fills everything; ``new`` also adds unknown fields and unknown enum values (later additions)."""
    schema = resolve(schema)
    if "anyOf" in schema:
        options = [o for o in schema["anyOf"] if o.get("type") != "null"]
        if mode == "nulls" and len(options) < len(schema["anyOf"]):
            return None
        return sample(options[-1] if mode == "new" else options[0], mode)
    kind = schema.get("type")
    if isinstance(kind, list):
        if mode == "nulls" and "null" in kind:
            return None
        kind = next(k for k in kind if k != "null")
    if "enum" in schema:
        values = [v for v in schema["enum"] if v is not None]
        return "something_new" if mode == "new" and kind != "boolean" else values[0]
    if kind == "object":
        props = schema.get("properties", {})
        required = schema.get("required", [])
        keep = required if mode == "nulls" else list(props)
        value = {k: sample(props[k], mode) for k in keep}
        if mode == "new":
            value["added_later"] = {"any": ["thing"]}
        return value
    if kind == "array":
        if "prefixItems" in schema:
            return [sample(item, mode) for item in schema["prefixItems"]]
        return [sample(schema["items"], mode)]
    if kind == "string":
        fmt = schema.get("format")
        if fmt == "date-time":
            return "2026-11-03T14:03:07.250Z"
        if fmt == "date":
            return "2026-11-03"
        if "pattern" in schema and "0-9a-f]{64}" in schema["pattern"]:
            return "a" * 64
        return "kalshi:KXFED-26DEC-H0" if ":" in schema.get("pattern", "") else "text"
    if kind == "integer":
        return 1
    if kind == "number":
        return 0.5
    if kind == "boolean":
        return True
    return {"anything": 1}


@pytest.mark.parametrize("mode", ["nulls", "full", "new"])
@pytest.mark.parametrize("name", sorted(MODEL_SCHEMAS))
def test_every_model_reads_what_the_contract_allows(name: str, mode: str) -> None:
    model: type[BaseModel] = getattr(models, name)
    value = sample(MODEL_SCHEMAS[name], mode)
    obj = model.model_validate_json(json.dumps(value))
    if mode == "new":
        assert obj.model_extra and "added_later" in obj.model_extra


@pytest.mark.parametrize("name", sorted(MODEL_SCHEMAS))
def test_required_non_nullable_fields_are_required(name: str) -> None:
    schema = MODEL_SCHEMAS[name]
    model: type[BaseModel] = getattr(models, name)
    for field_name in schema.get("required", []):
        prop = schema["properties"][field_name]
        value = sample(schema, "full")
        if sample(prop, "nulls") is None:
            continue  # nullable: a missing value reads as null
        del value[field_name]
        with pytest.raises(ValidationError):
            model.model_validate(value)


def test_the_contracts_examples_read() -> None:
    books = CONTRACT["paths"]["/v2/markets/{market_id}/books"]["get"]["responses"]["200"]["content"]
    example = books["application/json"]["examples"]["book_at_a_millisecond"]["value"]
    page = models.BooksPage.model_validate(example)
    assert page.data[0].bids == [(0.47, 600.0)]
    assert page.attribution == "Powered by Kalshi Research"
    bbo = CONTRACT["paths"]["/v2/markets/{market_id}/bbo"]["get"]["responses"]["200"]["content"]
    rows = [
        models.BboRow.model_validate_json(line)
        for line in bbo["application/x-ndjson"]["example"].splitlines()
    ]
    assert [r.seq for r in rows] == [1, 2]
    for response in CONTRACT["components"]["responses"].values():
        example = response["content"]["application/json"].get("example")
        if example:
            assert models.Error.model_validate(example).error.code


def test_enums_take_values_the_contract_adds_later() -> None:
    market = models.MarketRef.model_validate(sample(MODEL_SCHEMAS["MarketRef"], "full") | {"venue": "novig"})
    assert market.venue == "novig"
    error = models.Error.model_validate({"error": {"code": "brand_new", "message": "m", "request_id": "r"}})
    assert error.error.code == "brand_new"
