"""Replay a dbt run: reconstruct the observed schedule from run_results + manifest.

Unlike ``longest_path`` (which predicts a lower bound on wall-clock from the
DAG topology), ``replay`` reads the *actual* schedule that happened: each
result carries a ``thread_id`` and a ``timing`` array of phase boundaries, so
we can reconstruct the per-thread Gantt, attribute idle gaps to the parent a
thread was waiting on, and trace the observed critical path backwards from
the last-completing node through ``parent_map``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dbt_dag_opt.errors import InvalidArtifactError
from dbt_dag_opt.models import DagArtifacts


@dataclass(frozen=True)
class ReplayEvent:
    """A single node execution on a specific thread, bounded by wall-clock timestamps."""

    unique_id: str
    thread_id: str
    started_at: datetime
    completed_at: datetime
    execution_time: float
    status: str

    @property
    def duration_seconds(self) -> float:
        return (self.completed_at - self.started_at).total_seconds()


@dataclass(frozen=True)
class ThreadStats:
    thread_id: str
    busy_seconds: float
    idle_seconds: float
    utilization: float
    event_count: int


@dataclass(frozen=True)
class IdleGap:
    """A stretch of time where a thread had no work running.

    ``waited_on`` is the parent of ``before_node`` whose completion time is
    closest to (but no later than) ``before_node.started_at`` — i.e. the
    upstream node whose delivery actually unblocked the thread. ``None``
    means the thread sat idle even though all parents were already done,
    which points at scheduler overhead or priority choices rather than
    DAG-structural blocking.
    """

    thread_id: str
    started_at: datetime
    completed_at: datetime
    seconds: float
    before_node: str
    waited_on: str | None


@dataclass(frozen=True)
class CriticalStep:
    unique_id: str
    started_at: datetime
    completed_at: datetime
    execution_time: float
    waited_on: str | None
    idle_before: float


@dataclass(frozen=True)
class ReplayReport:
    wall_clock_seconds: float
    elapsed_time_seconds: float
    total_cpu_seconds: float
    thread_count: int
    thread_stats: list[ThreadStats]
    critical_path: list[CriticalStep]
    critical_path_seconds: float
    events: list[ReplayEvent]
    top_idle_gaps: list[IdleGap]


def parse_events(run_results: dict[str, Any]) -> list[ReplayEvent]:
    raw_results = run_results.get("results")
    if not isinstance(raw_results, list):
        raise InvalidArtifactError("run_results.json: 'results' must be a list")

    events: list[ReplayEvent] = []
    for entry in raw_results:
        event = _event_from_result(entry)
        if event is not None:
            events.append(event)
    return events


def _event_from_result(entry: Any) -> ReplayEvent | None:
    if not isinstance(entry, dict):
        return None
    unique_id = entry.get("unique_id")
    thread_id = entry.get("thread_id")
    timing = entry.get("timing")
    if not isinstance(unique_id, str) or not isinstance(thread_id, str):
        return None
    if not isinstance(timing, list) or not timing:
        return None

    start = _earliest_started(timing)
    end = _latest_completed(timing)
    if start is None or end is None:
        return None

    raw_exec = entry.get("execution_time", 0.0)
    try:
        exec_time = float(raw_exec) if raw_exec is not None else 0.0
    except (TypeError, ValueError):
        exec_time = 0.0

    status = entry.get("status")
    return ReplayEvent(
        unique_id=unique_id,
        thread_id=thread_id,
        started_at=start,
        completed_at=end,
        execution_time=exec_time,
        status=status if isinstance(status, str) else "unknown",
    )


def _earliest_started(timing: list[Any]) -> datetime | None:
    starts: list[datetime] = []
    for phase in timing:
        if not isinstance(phase, dict):
            continue
        raw = phase.get("started_at")
        if not isinstance(raw, str):
            continue
        parsed = _try_parse_iso(raw)
        if parsed is not None:
            starts.append(parsed)
    return min(starts) if starts else None


def _latest_completed(timing: list[Any]) -> datetime | None:
    ends: list[datetime] = []
    for phase in timing:
        if not isinstance(phase, dict):
            continue
        raw = phase.get("completed_at")
        if not isinstance(raw, str):
            continue
        parsed = _try_parse_iso(raw)
        if parsed is not None:
            ends.append(parsed)
    return max(ends) if ends else None


def _try_parse_iso(ts: str) -> datetime | None:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def compute_thread_stats(events: list[ReplayEvent]) -> list[ThreadStats]:
    per_thread: dict[str, list[ReplayEvent]] = {}
    for ev in events:
        per_thread.setdefault(ev.thread_id, []).append(ev)

    stats: list[ThreadStats] = []
    for thread_id, thread_events in per_thread.items():
        ordered = sorted(thread_events, key=lambda e: e.started_at)
        span = (ordered[-1].completed_at - ordered[0].started_at).total_seconds()
        busy = sum(ev.duration_seconds for ev in ordered)
        idle = max(0.0, span - busy)
        utilization = busy / span if span > 0 else 1.0
        stats.append(
            ThreadStats(
                thread_id=thread_id,
                busy_seconds=busy,
                idle_seconds=idle,
                utilization=utilization,
                event_count=len(ordered),
            )
        )
    return sorted(stats, key=lambda s: s.thread_id)


def observed_critical_path(
    events: list[ReplayEvent], parent_map: dict[str, list[str]]
) -> list[CriticalStep]:
    """Walk backwards from the last-completing event, following parent edges.

    At each step, the "blocker" is the parent (in ``parent_map``) that
    finished latest among those completing before this node started. That
    parent is what we were waiting on. Chain terminates at a node with no
    in-graph parents (a source) or when the chain revisits itself.
    """
    if not events:
        return []

    by_id = {ev.unique_id: ev for ev in events}
    terminal = max(events, key=lambda e: e.completed_at)

    chain: list[CriticalStep] = []
    current: ReplayEvent | None = terminal
    visited: set[str] = set()

    while current is not None and current.unique_id not in visited:
        visited.add(current.unique_id)
        blocker = _pick_blocker(current, by_id, parent_map)
        idle_before = (
            (current.started_at - blocker.completed_at).total_seconds() if blocker else 0.0
        )
        chain.append(
            CriticalStep(
                unique_id=current.unique_id,
                started_at=current.started_at,
                completed_at=current.completed_at,
                execution_time=current.execution_time,
                waited_on=blocker.unique_id if blocker else None,
                idle_before=idle_before,
            )
        )
        current = blocker

    chain.reverse()
    return chain


def compute_idle_gaps(
    events: list[ReplayEvent],
    parent_map: dict[str, list[str]],
    *,
    run_started_at: datetime | None = None,
) -> list[IdleGap]:
    by_id = {ev.unique_id: ev for ev in events}
    per_thread: dict[str, list[ReplayEvent]] = {}
    for ev in events:
        per_thread.setdefault(ev.thread_id, []).append(ev)

    gaps: list[IdleGap] = []
    for thread_id, thread_events in per_thread.items():
        ordered = sorted(thread_events, key=lambda e: e.started_at)
        prev_end = run_started_at
        for ev in ordered:
            if prev_end is not None and ev.started_at > prev_end:
                seconds = (ev.started_at - prev_end).total_seconds()
                if seconds > 0:
                    blocker = _pick_blocker(ev, by_id, parent_map)
                    gaps.append(
                        IdleGap(
                            thread_id=thread_id,
                            started_at=prev_end,
                            completed_at=ev.started_at,
                            seconds=seconds,
                            before_node=ev.unique_id,
                            waited_on=blocker.unique_id if blocker else None,
                        )
                    )
            prev_end = ev.completed_at

    return sorted(gaps, key=lambda g: g.seconds, reverse=True)


def _pick_blocker(
    ev: ReplayEvent,
    by_id: dict[str, ReplayEvent],
    parent_map: dict[str, list[str]],
) -> ReplayEvent | None:
    parents = parent_map.get(ev.unique_id, [])
    blocker: ReplayEvent | None = None
    for pid in parents:
        pev = by_id.get(pid)
        if pev is None or pev.completed_at > ev.started_at:
            continue
        if blocker is None or pev.completed_at > blocker.completed_at:
            blocker = pev
    return blocker


def build_replay(artifacts: DagArtifacts, *, top_idle_gaps_limit: int = 10) -> ReplayReport:
    events = parse_events(artifacts.run_results)
    if not events:
        raise InvalidArtifactError("no replayable events found in run_results")

    parent_map_raw = artifacts.manifest.get("parent_map", {})
    if not isinstance(parent_map_raw, dict):
        raise InvalidArtifactError("manifest.json: 'parent_map' must be an object")
    parent_map: dict[str, list[str]] = {
        k: list(v) for k, v in parent_map_raw.items() if isinstance(v, list)
    }

    run_start = min(ev.started_at for ev in events)
    run_end = max(ev.completed_at for ev in events)
    wall_clock = (run_end - run_start).total_seconds()

    raw_elapsed = artifacts.run_results.get("elapsed_time", 0.0)
    try:
        elapsed = float(raw_elapsed) if raw_elapsed is not None else 0.0
    except (TypeError, ValueError):
        elapsed = 0.0

    total_cpu = sum(ev.execution_time for ev in events)
    thread_stats = compute_thread_stats(events)
    critical = observed_critical_path(events, parent_map)
    critical_seconds = (
        (critical[-1].completed_at - critical[0].started_at).total_seconds() if critical else 0.0
    )
    gaps = compute_idle_gaps(events, parent_map, run_started_at=run_start)[:top_idle_gaps_limit]

    return ReplayReport(
        wall_clock_seconds=wall_clock,
        elapsed_time_seconds=elapsed,
        total_cpu_seconds=total_cpu,
        thread_count=len(thread_stats),
        thread_stats=thread_stats,
        critical_path=critical,
        critical_path_seconds=critical_seconds,
        events=events,
        top_idle_gaps=gaps,
    )
