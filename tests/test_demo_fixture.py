"""Lock the demo fixture's headline shape so the demo script stays predictable.

The fixture at tests/fixtures/demo_project/ is generated from
tests/fixtures/generate_demo_fixture.py. These tests guard against accidental
regenerations that would make the recorded demo commands produce confusing
numbers (e.g. headroom collapsing to $0, cost flipping sign, shared bottleneck
disappearing from top paths).
"""

from __future__ import annotations

from dbt_dag_opt.cost import compute_cost, cost_inputs_from_replay
from dbt_dag_opt.graph import build_dag
from dbt_dag_opt.longest_path import longest_paths_from_each_source
from dbt_dag_opt.models import DagArtifacts
from dbt_dag_opt.replay import build_replay


def test_demo_fixture_loads_and_has_expected_scale(
    demo_project_artifacts: DagArtifacts,
) -> None:
    """Sanity check — demo should have 19 models, 29 tests, 48 events, 4 threads."""
    report = build_replay(demo_project_artifacts)
    assert len(report.events) == 48
    assert report.thread_count == 4
    # Wall-clock in the ~7-8 min range so min-billing floor never fires.
    assert 400.0 < report.wall_clock_seconds < 600.0


def test_demo_fixture_analyze_top_path_is_critical_chain(
    demo_project_artifacts: DagArtifacts,
) -> None:
    """Top longest path should end at fct_batter_vs_pitcher and include
    int_game_events (the shared bottleneck the demo calls out)."""
    dag = build_dag(demo_project_artifacts)
    paths = longest_paths_from_each_source(dag)
    assert paths, "expected at least one path"
    top = paths[0]
    assert top.path[-1] == "model.demo.fct_batter_vs_pitcher"
    assert "model.demo.int_game_events" in top.path


def test_demo_fixture_shared_bottleneck_visible_on_top_paths(
    demo_project_artifacts: DagArtifacts,
) -> None:
    """int_game_events must be the slowest model on multiple top-5 paths —
    that's the shared-bottleneck signal the demo's analyze step relies on."""
    dag = build_dag(demo_project_artifacts)
    paths = longest_paths_from_each_source(dag)
    weights = dag.weights

    bottleneck_counts = {}
    for p in paths[:5]:
        if not p.path:
            continue
        heaviest = max(p.path, key=lambda n: weights.get(n, 0.0))
        bottleneck_counts[heaviest] = bottleneck_counts.get(heaviest, 0) + 1

    assert bottleneck_counts.get("model.demo.int_game_events", 0) >= 3


def test_demo_fixture_cost_has_meaningful_headroom_and_idle(
    demo_project_artifacts: DagArtifacts,
) -> None:
    """--warehouse-size L should produce a $1+ run cost with positive headroom
    and idle cost > 0 — the demo's three framed numbers all need to be non-zero."""
    report = build_replay(demo_project_artifacts)
    cost = compute_cost(
        cost_inputs_from_replay(
            report,
            warehouse_size="L",
            credits_per_hour=None,
            rate_per_credit_usd=2.0,
            apply_minimum_billing=True,
        )
    )
    # Wall-clock > 60s → min-billing floor shouldn't fire.
    assert cost.min_billing_applied is False
    assert cost.run_cost_usd > 1.0
    assert cost.headroom_usd > 0.0
    assert cost.idle_cost_usd > 0.0
    # At least 20% of warehouse-seconds should be idle — the demo's "idle cost"
    # framing needs a visible percentage.
    assert cost.waste_fraction > 0.20
