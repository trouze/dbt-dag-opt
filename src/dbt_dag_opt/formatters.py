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


def render(
    results: list[PathResult],
    fmt: Format,
    *,
    top: int | None = None,
    show_full_path: bool = False,
    weights: dict[str, float] | None = None,
) -> str:
    ordered = sorted(results, key=lambda r: r.distance, reverse=True)
    if top is not None:
        ordered = ordered[:top]

    if fmt is Format.JSON:
        return _render_json(ordered)
    if fmt is Format.JSONL:
        return _render_jsonl(ordered)
    if fmt is Format.TABLE:
        return _render_table(ordered, show_full_path=show_full_path, weights=weights)
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


def _render_table(
    results: list[PathResult],
    *,
    show_full_path: bool = False,
    weights: dict[str, float] | None = None,
) -> str:
    buffer = StringIO()
    console = Console(file=buffer, force_terminal=False, width=140)
    table = Table(
        title="Longest paths by total execution time",
        show_lines=show_full_path,  # row separators help when Path cell wraps
    )
    table.add_column("#", justify="right", style="dim")
    table.add_column("Source", overflow="fold")
    if show_full_path:
        table.add_column("Path", overflow="fold")
    else:
        table.add_column("End of path", overflow="fold")
    table.add_column("Length", justify="right")
    table.add_column("Total time (s)", justify="right", style="bold")
    if weights is not None:
        table.add_column("Bottleneck (slowest on path)", overflow="fold")
        table.add_column("Bottleneck time (s)", justify="right")

    for idx, r in enumerate(results, start=1):
        row: list[str] = [str(idx), r.source]
        if show_full_path:
            row.append(_format_path(r.path))
        else:
            row.append(r.path[-1] if r.path else r.source)
        row.extend([str(len(r.path)), f"{r.distance:.2f}"])
        if weights is not None:
            node, seconds = _bottleneck(r.path, weights)
            row.extend([node, f"{seconds:.2f}"])
        table.add_row(*row)

    console.print(table)
    return buffer.getvalue()


def _format_path(path: list[str]) -> str:
    return " → ".join(path) if path else "(empty)"


def _bottleneck(path: list[str], weights: dict[str, float]) -> tuple[str, float]:
    """Return (node_id, seconds) for the heaviest node along this path."""
    if not path:
        return ("-", 0.0)
    best_node = path[0]
    best_seconds = weights.get(best_node, 0.0)
    for node in path[1:]:
        seconds = weights.get(node, 0.0)
        if seconds > best_seconds:
            best_node = node
            best_seconds = seconds
    return (best_node, best_seconds)
