from __future__ import annotations

import json

from dbt_dag_opt.formatters import Format, render
from dbt_dag_opt.models import PathResult

_RESULTS = [
    PathResult(source="src.a", path=["src.a", "mid.a", "end.a"], distance=30.0),
    PathResult(source="src.b", path=["src.b", "end.b"], distance=10.0),
    PathResult(source="src.c", path=["src.c"], distance=1.0),
]


def test_render_json_is_valid_and_sorted() -> None:
    out = render(_RESULTS, Format.JSON)
    payload = json.loads(out)
    keys = list(payload.keys())
    assert keys == ["src.a", "src.b", "src.c"]
    assert payload["src.a"]["distance"] == 30.0
    assert payload["src.a"]["length"] == 3


def test_render_jsonl_is_one_object_per_line() -> None:
    out = render(_RESULTS, Format.JSONL)
    lines = out.splitlines()
    assert len(lines) == 3
    for line in lines:
        obj = json.loads(line)
        assert {"source", "path", "distance", "length"} <= obj.keys()


def test_top_limits_results() -> None:
    out = render(_RESULTS, Format.JSON, top=2)
    payload = json.loads(out)
    assert list(payload.keys()) == ["src.a", "src.b"]


def test_render_table_contains_all_sources() -> None:
    out = render(_RESULTS, Format.TABLE)
    assert "src.a" in out
    assert "30.00" in out
