"""Generate a larger, longer-running dbt artifact pair for demos.

Produces ``tests/fixtures/demo_project/manifest.json`` and
``tests/fixtures/demo_project/run_results.json`` for a synthetic
25-model, 30-test dbt project simulated across 8 threads.

The topology models a baseball analytics warehouse. It's intentionally
shaped so ``dbt-dag-opt analyze`` surfaces a **shared bottleneck**
(``int_game_events`` sits on three of the longest paths) and so
``replay --warehouse-size L`` produces meaningful headroom and idle
cost (wall-clock > 60s, so no min-billing floor; thread idleness
produces real waste).

Run with::

    uv run python tests/fixtures/generate_demo_fixture.py

The output JSONs are committed to the repo. Re-run after changing
the topology or times below.
"""

from __future__ import annotations

import heapq
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

THREAD_COUNT = 4
START = datetime(2026, 4, 24, 12, 0, 0, tzinfo=timezone.utc)
OUT_DIR = Path(__file__).parent / "demo_project"


@dataclass(frozen=True)
class Node:
    unique_id: str
    parents: tuple[str, ...]
    execution_time: float
    resource_type: str  # "source" | "model" | "test"


def _build_topology() -> list[Node]:
    nodes: list[Node] = []

    # 6 sources (instant; not scheduled)
    for t in ("games", "players", "teams", "rosters", "at_bats", "pitches"):
        nodes.append(Node(f"source.demo.raw.{t}", (), 0.0, "source"))

    # 8 staging models — fast, fan-out from sources
    staging = [
        ("stg_games",            ("games",),    18.5),
        ("stg_players",          ("players",),  22.3),
        ("stg_teams",            ("teams",),    12.1),
        ("stg_rosters",          ("rosters",),  15.7),
        ("stg_at_bats",          ("at_bats",),  28.4),
        ("stg_pitches",          ("pitches",),  35.9),
        ("stg_game_umpires",     ("games",),    11.2),
        ("stg_player_contracts", ("players",),  14.6),
    ]
    for name, srcs, t in staging:
        parents = tuple(f"source.demo.raw.{s}" for s in srcs)
        nodes.append(Node(f"model.demo.{name}", parents, t, "model"))

    # 6 intermediates — the shared bottleneck lives here
    intermediates = [
        # int_game_events is heavy (182s) AND feeds three downstream models →
        # optimizing it has multiplicative ROI. This is the shared-bottleneck
        # signal the analyze Bottleneck column surfaces.
        ("int_game_events",      ("stg_games", "stg_at_bats", "stg_pitches"), 182.4),
        ("int_player_stats",     ("stg_players", "stg_at_bats"),              92.1),
        ("int_team_roster",      ("stg_teams", "stg_rosters",
                                  "stg_player_contracts"),                    68.5),
        ("int_pitching_log",     ("stg_pitches", "stg_players"),              71.8),
        ("int_batter_matchups",  ("int_player_stats", "int_game_events"),     84.3),
        ("int_pitcher_matchups", ("int_pitching_log", "int_game_events"),     79.9),
    ]
    for name, parents, t in intermediates:
        parent_ids = tuple(f"model.demo.{p}" for p in parents)
        nodes.append(Node(f"model.demo.{name}", parent_ids, t, "model"))

    # 5 marts
    marts = [
        ("fct_game_box_score",    ("int_game_events", "int_team_roster"),        95.2),
        ("fct_player_season",     ("int_player_stats", "int_team_roster"),      103.7),
        ("fct_pitcher_season",    ("int_pitching_log", "int_team_roster"),       88.4),
        ("fct_batter_vs_pitcher", ("int_batter_matchups",
                                   "int_pitcher_matchups"),                     124.6),
        ("dim_roster",            ("stg_rosters", "stg_player_contracts"),       42.8),
    ]
    for name, parents, t in marts:
        parent_ids = tuple(f"model.demo.{p}" for p in parents)
        nodes.append(Node(f"model.demo.{name}", parent_ids, t, "model"))

    # Tests — 1 not_null per model, plus a unique test on every other model
    model_ids = [n.unique_id for n in nodes if n.resource_type == "model"]
    for i, mid in enumerate(model_ids):
        model_name = mid.split(".")[-1]
        # not_null: 2.8 - 4.7s deterministic
        exec_t = 2.8 + (i * 0.15) % 2.0
        nodes.append(Node(
            f"test.demo.not_null_{model_name}_id.nn{i:02d}",
            (mid,),
            round(exec_t, 2),
            "test",
        ))
        if i % 2 == 0:
            exec_t = 2.0 + (i * 0.11) % 1.8
            nodes.append(Node(
                f"test.demo.unique_{model_name}_id.uq{i:02d}",
                (mid,),
                round(exec_t, 2),
                "test",
            ))

    return nodes


@dataclass
class ScheduledEvent:
    unique_id: str
    thread_id: str
    started_at: datetime
    completed_at: datetime
    execution_time: float


@dataclass
class _SimState:
    by_id: dict[str, Node]
    children: dict[str, list[str]]
    in_deg: dict[str, int]
    done_at: dict[str, datetime]
    ready_queue: list[tuple[datetime, int, str]]
    tie_break: int = 0

    def release(self, finished: str, end_time: datetime) -> None:
        if finished in self.done_at:
            return
        self.done_at[finished] = end_time
        for child in self.children[finished]:
            self.in_deg[child] -= 1
            if self.in_deg[child] == 0:
                last_parent = max(self.done_at[p] for p in self.by_id[child].parents)
                heapq.heappush(self.ready_queue, (last_parent, self.tie_break, child))
                self.tie_break += 1


def _simulate(nodes: list[Node]) -> list[ScheduledEvent]:
    """Discrete-event scheduler matching dbt's 'take earliest-free thread' model."""
    children: dict[str, list[str]] = {n.unique_id: [] for n in nodes}
    in_deg: dict[str, int] = {n.unique_id: 0 for n in nodes}
    for n in nodes:
        for p in n.parents:
            if p in children:
                children[p].append(n.unique_id)
                in_deg[n.unique_id] += 1

    state = _SimState(
        by_id={n.unique_id: n for n in nodes},
        children=children,
        in_deg=in_deg,
        done_at={},
        ready_queue=[],
    )

    # Sources complete instantly at t=0; unblock their children.
    for n in nodes:
        if n.resource_type == "source":
            state.release(n.unique_id, START)

    events: list[ScheduledEvent] = []
    threads_free_at: list[datetime] = [START] * THREAD_COUNT
    thread_ids = [f"Thread-{i + 1} (worker)" for i in range(THREAD_COUNT)]
    completions: list[tuple[datetime, int, str]] = []

    while state.ready_queue or completions:
        if state.ready_queue:
            ready_time, _, node_id = heapq.heappop(state.ready_queue)
            thread_idx = min(range(THREAD_COUNT), key=lambda i: threads_free_at[i])
            start_time = max(ready_time, threads_free_at[thread_idx])
            end_time = start_time + timedelta(seconds=state.by_id[node_id].execution_time)
            events.append(ScheduledEvent(
                unique_id=node_id,
                thread_id=thread_ids[thread_idx],
                started_at=start_time,
                completed_at=end_time,
                execution_time=state.by_id[node_id].execution_time,
            ))
            threads_free_at[thread_idx] = end_time
            heapq.heappush(completions, (end_time, state.tie_break, node_id))
            state.tie_break += 1
        else:
            end_time, _, finished = heapq.heappop(completions)
            state.release(finished, end_time)

        # Drain completions whose end_time has already passed so newly-ready
        # children can enter the ready queue before the next assignment.
        earliest_thread = min(threads_free_at)
        while completions and completions[0][0] <= earliest_thread:
            end_time, _, finished = heapq.heappop(completions)
            state.release(finished, end_time)

    return events


def _iso(ts: datetime) -> str:
    return ts.isoformat()


def _build_manifest(nodes: list[Node]) -> dict:
    parent_map: dict[str, list[str]] = {n.unique_id: list(n.parents) for n in nodes}
    child_map: dict[str, list[str]] = {n.unique_id: [] for n in nodes}
    for n in nodes:
        for p in n.parents:
            if p in child_map:
                child_map[p].append(n.unique_id)

    manifest_nodes: dict[str, dict] = {}
    manifest_sources: dict[str, dict] = {}
    for n in nodes:
        entry = {
            "unique_id": n.unique_id,
            "resource_type": n.resource_type,
            "name": n.unique_id.split(".")[-1],
            "package_name": "demo",
        }
        if n.resource_type == "source":
            manifest_sources[n.unique_id] = entry
        else:
            manifest_nodes[n.unique_id] = entry

    return {
        "metadata": {
            "adapter_type": "snowflake",
            "project_name": "demo",
            "dbt_version": "1.8.0",
            "generated_at": _iso(START),
        },
        "nodes": manifest_nodes,
        "sources": manifest_sources,
        "parent_map": parent_map,
        "child_map": child_map,
    }


def _build_run_results(events: list[ScheduledEvent]) -> dict:
    elapsed = max((e.completed_at for e in events), default=START) - START
    results = []
    for e in events:
        # Split into a tiny compile phase + main execute phase so the fixture
        # looks like real dbt output (replay uses outer span anyway).
        compile_end = e.started_at + timedelta(seconds=0.3)
        if compile_end > e.completed_at:
            compile_end = e.started_at  # zero-length compile for very short tests
        results.append({
            "unique_id": e.unique_id,
            "thread_id": e.thread_id,
            "status": "success",
            "execution_time": e.execution_time,
            "timing": [
                {"name": "compile",
                 "started_at": _iso(e.started_at),
                 "completed_at": _iso(compile_end)},
                {"name": "execute",
                 "started_at": _iso(compile_end),
                 "completed_at": _iso(e.completed_at)},
            ],
            "message": None,
            "failures": None,
            "adapter_response": {"rows_affected": 0},
        })
    return {
        "metadata": {
            "generated_at": _iso(START),
            "dbt_version": "1.8.0",
            "invocation_id": "demo-fixture",
        },
        "results": results,
        "elapsed_time": elapsed.total_seconds(),
        "args": {"threads": THREAD_COUNT, "target": "prod"},
    }


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    nodes = _build_topology()
    events = _simulate(nodes)

    manifest = _build_manifest(nodes)
    run_results = _build_run_results(events)

    (OUT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (OUT_DIR / "run_results.json").write_text(json.dumps(run_results, indent=2) + "\n")

    # Diagnostics
    models = [n for n in nodes if n.resource_type == "model"]
    tests = [n for n in nodes if n.resource_type == "test"]
    total_cpu = sum(e.execution_time for e in events)
    wall = (max(e.completed_at for e in events) - min(e.started_at for e in events)).total_seconds()
    print(f"wrote {OUT_DIR}/manifest.json + run_results.json")
    print(f"  models: {len(models)}  tests: {len(tests)}  events: {len(events)}")
    print(f"  threads: {THREAD_COUNT}  wall-clock: {wall:.1f}s  total CPU: {total_cpu:.1f}s")
    print(f"  theoretical parallel min wall: {total_cpu / THREAD_COUNT:.1f}s")


if __name__ == "__main__":
    main()
