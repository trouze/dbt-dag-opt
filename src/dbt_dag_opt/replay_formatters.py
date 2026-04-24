"""Render a ReplayReport as JSON or a human-readable text summary."""

from __future__ import annotations

import json
from datetime import datetime
from enum import Enum
from io import StringIO

from rich.console import Console
from rich.table import Table

from dbt_dag_opt.replay import CriticalStep, IdleGap, ReplayEvent, ReplayReport, ThreadStats


class ReplayFormat(str, Enum):
    TEXT = "text"
    JSON = "json"


def render_replay(report: ReplayReport, fmt: ReplayFormat) -> str:
    if fmt is ReplayFormat.JSON:
        return _render_json(report)
    if fmt is ReplayFormat.TEXT:
        return _render_text(report)
    raise ValueError(f"unknown format: {fmt}")


def _render_json(report: ReplayReport) -> str:
    return json.dumps(
        {
            "wall_clock_seconds": report.wall_clock_seconds,
            "elapsed_time_seconds": report.elapsed_time_seconds,
            "total_cpu_seconds": report.total_cpu_seconds,
            "thread_count": report.thread_count,
            "critical_path_seconds": report.critical_path_seconds,
            "thread_stats": [_thread_stats_dict(ts) for ts in report.thread_stats],
            "critical_path": [_critical_step_dict(s) for s in report.critical_path],
            "top_idle_gaps": [_idle_gap_dict(g) for g in report.top_idle_gaps],
            "events": [_event_dict(e) for e in report.events],
        },
        indent=2,
    )


def _thread_stats_dict(ts: ThreadStats) -> dict[str, float | int | str]:
    return {
        "thread_id": ts.thread_id,
        "busy_seconds": ts.busy_seconds,
        "idle_seconds": ts.idle_seconds,
        "utilization": ts.utilization,
        "event_count": ts.event_count,
    }


def _critical_step_dict(s: CriticalStep) -> dict[str, float | str | None]:
    return {
        "unique_id": s.unique_id,
        "started_at": _iso(s.started_at),
        "completed_at": _iso(s.completed_at),
        "execution_time": s.execution_time,
        "waited_on": s.waited_on,
        "idle_before_seconds": s.idle_before,
    }


def _idle_gap_dict(g: IdleGap) -> dict[str, float | str | None]:
    return {
        "thread_id": g.thread_id,
        "started_at": _iso(g.started_at),
        "completed_at": _iso(g.completed_at),
        "seconds": g.seconds,
        "before_node": g.before_node,
        "waited_on": g.waited_on,
    }


def _event_dict(e: ReplayEvent) -> dict[str, float | str]:
    return {
        "unique_id": e.unique_id,
        "thread_id": e.thread_id,
        "started_at": _iso(e.started_at),
        "completed_at": _iso(e.completed_at),
        "execution_time": e.execution_time,
        "status": e.status,
    }


def _iso(ts: datetime) -> str:
    return ts.isoformat()


def _render_text(report: ReplayReport) -> str:
    buffer = StringIO()
    console = Console(file=buffer, force_terminal=False, width=120)

    summary = Table(title="Run summary", show_header=False, show_lines=False)
    summary.add_column("key", style="dim")
    summary.add_column("value")
    summary.add_row("Wall-clock", f"{report.wall_clock_seconds:.3f} s")
    summary.add_row("dbt elapsed_time", f"{report.elapsed_time_seconds:.3f} s")
    summary.add_row("Total CPU-seconds", f"{report.total_cpu_seconds:.3f} s")
    summary.add_row("Threads", str(report.thread_count))
    summary.add_row("Events", str(len(report.events)))
    summary.add_row("Observed critical path", f"{report.critical_path_seconds:.3f} s")
    console.print(summary)

    threads = Table(title="Thread utilization", show_lines=False)
    threads.add_column("Thread", overflow="fold")
    threads.add_column("Busy (s)", justify="right")
    threads.add_column("Idle (s)", justify="right")
    threads.add_column("Util", justify="right", style="bold")
    threads.add_column("Events", justify="right")
    for ts in report.thread_stats:
        threads.add_row(
            ts.thread_id,
            f"{ts.busy_seconds:.3f}",
            f"{ts.idle_seconds:.3f}",
            f"{ts.utilization:.1%}",
            str(ts.event_count),
        )
    console.print(threads)

    critical = Table(
        title=f"Observed critical path ({report.critical_path_seconds:.3f} s)",
        show_lines=False,
    )
    critical.add_column("#", justify="right", style="dim")
    critical.add_column("Node", overflow="fold")
    critical.add_column("Exec (s)", justify="right")
    critical.add_column("Waited on", overflow="fold", style="dim")
    critical.add_column("Scheduler lag (s)", justify="right", style="dim")
    for idx, step in enumerate(report.critical_path, start=1):
        critical.add_row(
            str(idx),
            step.unique_id,
            f"{step.execution_time:.3f}",
            step.waited_on or "(source)",
            f"{step.idle_before:.3f}" if step.waited_on else "-",
        )
    console.print(critical)

    if report.top_idle_gaps:
        idle = Table(title="Top idle gaps", show_lines=False)
        idle.add_column("Thread", overflow="fold")
        idle.add_column("Gap (s)", justify="right", style="bold")
        idle.add_column("Before node", overflow="fold")
        idle.add_column("Waited on", overflow="fold", style="dim")
        for g in report.top_idle_gaps:
            idle.add_row(
                g.thread_id,
                f"{g.seconds:.3f}",
                g.before_node,
                g.waited_on or "(none — scheduler gap)",
            )
        console.print(idle)

    return buffer.getvalue()
