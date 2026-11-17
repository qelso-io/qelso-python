# Contributing

The client is generated from the Qelso Data API's contract (`contract/openapi.v2.json`, the same file the API
serves at `https://api.qelso.io/openapi.json`) by `scripts/generate.py`, and developed in Qelso's own
repository, which this one mirrors for each release. So we don't merge pull requests here: please write to
support@qelso.io with what you found or need, and we'll make the change at the source.

To run the checks locally:

```bash
uv sync
uv run ruff check . && uv run ruff format --check .
uv run mypy
uv run python scripts/generate.py --check
uv run pytest -q
```
