"""Render longest-path results as JSON, JSONL, or a human-readable table."""

from __future__ import annotations

import json
from enum import Enum
from io import StringIO

from rich.console import Console
from rich.table import Table

from dbt_dag_opt.models import PathResult


class Format(str, Enum):
    JSON = "json"
    JSONL = "jsonl"
    TABLE = "table"


def render(results: list[PathResult], fmt: Format, *, top: int | None = None) -> str:
    ordered = sorted(results, key=lambda r: r.distance, reverse=True)
    if top is not None:
        ordered = ordered[:top]

    if fmt is Format.JSON:
        return _render_json(ordered)
    if fmt is Format.JSONL:
        return _render_jsonl(ordered)
    if fmt is Format.TABLE:
        return _render_table(ordered)
    raise ValueError(f"unknown format: {fmt}")


def _render_json(results: list[PathResult]) -> str:
    payload = {
        r.source: {"path": r.path, "distance": r.distance, "length": len(r.path)}
        for r in results
    }
    return json.dumps(payload, indent=2)


def _render_jsonl(results: list[PathResult]) -> str:
    lines = [
        json.dumps(
            {"source": r.source, "path": r.path, "distance": r.distance, "length": len(r.path)}
        )
        for r in results
    ]
    return "\n".join(lines)


def _render_table(results: list[PathResult]) -> str:
    buffer = StringIO()
    console = Console(file=buffer, force_terminal=False, width=120)
    table = Table(title="Longest paths by total execution time", show_lines=False)
    table.add_column("#", justify="right", style="dim")
    table.add_column("Source", overflow="fold")
    table.add_column("End of path", overflow="fold")
    table.add_column("Length", justify="right")
    table.add_column("Total time (s)", justify="right", style="bold")

    for idx, r in enumerate(results, start=1):
        end = r.path[-1] if r.path else r.source
        table.add_row(str(idx), r.source, end, str(len(r.path)), f"{r.distance:.2f}")

    console.print(table)
    return buffer.getvalue()
