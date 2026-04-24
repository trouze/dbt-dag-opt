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


def test_render_table_show_full_path_renders_chain() -> None:
    out = render(_RESULTS, Format.TABLE, show_full_path=True)
    # full chain joined by arrows should appear verbatim for the longest path
    assert "src.a → mid.a → end.a" in out


def test_render_table_weights_adds_bottleneck_column() -> None:
    weights = {"src.a": 5.0, "mid.a": 20.0, "end.a": 5.0, "src.b": 4.0, "end.b": 6.0, "src.c": 1.0}
    out = render(_RESULTS, Format.TABLE, weights=weights)
    # Bottleneck of the top path is mid.a at 20s
    assert "Bottleneck" in out
    assert "mid.a" in out
    assert "20.00" in out


def test_render_table_show_full_path_and_weights_together() -> None:
    weights = {"src.a": 5.0, "mid.a": 20.0, "end.a": 5.0, "src.b": 4.0, "end.b": 6.0, "src.c": 1.0}
    out = render(_RESULTS, Format.TABLE, show_full_path=True, weights=weights)
    assert "src.a → mid.a → end.a" in out
    assert "mid.a" in out
    assert "20.00" in out
