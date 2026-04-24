from __future__ import annotations

import json

import pytest

from dbt_dag_opt.errors import InvalidArtifactError
from dbt_dag_opt.models import DagArtifacts
from dbt_dag_opt.replay import build_replay, parse_events
from dbt_dag_opt.replay_formatters import ReplayFormat, render_replay


def test_parse_events_skips_missing_timing() -> None:
    run_results = {
        "results": [
            {"unique_id": "x.y", "thread_id": "T-1", "timing": []},
            {"unique_id": "x.y2", "thread_id": "T-1"},
            {"thread_id": "T-1", "timing": [{"started_at": "2026-01-01T00:00:00Z"}]},
        ]
    }
    assert parse_events(run_results) == []


def test_parse_events_requires_results_list() -> None:
    with pytest.raises(InvalidArtifactError):
        parse_events({"results": "not a list"})


def test_synthetic_replay_report_shape(synthetic_replay_artifacts: DagArtifacts) -> None:
    report = build_replay(synthetic_replay_artifacts)

    assert report.wall_clock_seconds == pytest.approx(40.0)
    assert report.elapsed_time_seconds == pytest.approx(40.0)
    assert report.total_cpu_seconds == pytest.approx(50.0)  # 10+10+20+10
    assert report.thread_count == 2
    assert len(report.events) == 4


def test_synthetic_thread_stats(synthetic_replay_artifacts: DagArtifacts) -> None:
    report = build_replay(synthetic_replay_artifacts)
    by_id = {ts.thread_id: ts for ts in report.thread_stats}

    # Thread-1 span = 0-40 = 40s, busy = 30s, idle = 10s (waiting on C)
    t1 = by_id["Thread-1"]
    assert t1.busy_seconds == pytest.approx(30.0)
    assert t1.idle_seconds == pytest.approx(10.0)
    assert t1.utilization == pytest.approx(0.75)
    assert t1.event_count == 3

    # Thread-2 span = 10-30 = 20s, busy = 20s, idle = 0s within span
    t2 = by_id["Thread-2"]
    assert t2.busy_seconds == pytest.approx(20.0)
    assert t2.idle_seconds == pytest.approx(0.0)
    assert t2.utilization == pytest.approx(1.0)
    assert t2.event_count == 1


def test_synthetic_critical_path_prefers_latest_blocker(
    synthetic_replay_artifacts: DagArtifacts,
) -> None:
    report = build_replay(synthetic_replay_artifacts)
    path = [step.unique_id for step in report.critical_path]

    # D has two parents (B finished at 20, C finished at 30). C is the blocker.
    assert path == ["model.demo.A", "model.demo.C", "model.demo.D"]
    assert report.critical_path_seconds == pytest.approx(40.0)

    d_step = report.critical_path[-1]
    assert d_step.waited_on == "model.demo.C"
    assert d_step.idle_before == pytest.approx(0.0)  # D started right when C finished


def test_synthetic_idle_gaps_attribute_to_blocker(
    synthetic_replay_artifacts: DagArtifacts,
) -> None:
    report = build_replay(synthetic_replay_artifacts)

    # Expect two gaps: Thread-2 startup (0-10, before C, waited on A)
    # and Thread-1 (20-30, before D, waited on C).
    by_before = {g.before_node: g for g in report.top_idle_gaps}
    assert by_before["model.demo.C"].seconds == pytest.approx(10.0)
    assert by_before["model.demo.C"].waited_on == "model.demo.A"
    assert by_before["model.demo.D"].seconds == pytest.approx(10.0)
    assert by_before["model.demo.D"].waited_on == "model.demo.C"


def test_build_replay_raises_when_no_events() -> None:
    artifacts = DagArtifacts(manifest={"parent_map": {}}, run_results={"results": []})
    with pytest.raises(InvalidArtifactError):
        build_replay(artifacts)


def test_render_replay_json_is_valid(synthetic_replay_artifacts: DagArtifacts) -> None:
    report = build_replay(synthetic_replay_artifacts)
    rendered = render_replay(report, ReplayFormat.JSON)
    payload = json.loads(rendered)
    assert payload["thread_count"] == 2
    assert payload["wall_clock_seconds"] == pytest.approx(40.0)
    assert [step["unique_id"] for step in payload["critical_path"]] == [
        "model.demo.A",
        "model.demo.C",
        "model.demo.D",
    ]


def test_render_replay_text_contains_key_sections(
    synthetic_replay_artifacts: DagArtifacts,
) -> None:
    report = build_replay(synthetic_replay_artifacts)
    rendered = render_replay(report, ReplayFormat.TEXT)
    assert "Run summary" in rendered
    assert "Thread utilization" in rendered
    assert "Observed critical path" in rendered
    assert "Top idle gaps" in rendered


def test_dbt_dugout_integration(dbt_dugout_artifacts: DagArtifacts) -> None:
    """Smoke test against the real dbt_dugout run captured at tests/fixtures/dbt_dugout/."""
    report = build_replay(dbt_dugout_artifacts)

    # observed wall-clock should be close to dbt's self-reported elapsed_time
    assert 0.4 < report.wall_clock_seconds < 2.0
    assert report.thread_count == 4
    assert len(report.events) == 57

    # critical path must start at a source (no waited_on) and end at the terminal node
    assert report.critical_path[0].waited_on is None
    for step in report.critical_path[1:]:
        assert step.waited_on is not None

    # every event must have a positive-or-zero duration
    for ev in report.events:
        assert ev.duration_seconds >= 0.0
