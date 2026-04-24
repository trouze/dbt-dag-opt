from __future__ import annotations

import pytest

from dbt_dag_opt.errors import InvalidArtifactError
from dbt_dag_opt.graph import build_dag
from dbt_dag_opt.models import DagArtifacts


def test_build_dag_identifies_sources(tiny_artifacts: DagArtifacts) -> None:
    dag = build_dag(tiny_artifacts)
    assert dag.sources == sorted(["source.demo.raw.orders", "source.demo.raw.customers"])


def test_build_dag_attaches_execution_times(tiny_artifacts: DagArtifacts) -> None:
    dag = build_dag(tiny_artifacts)
    assert dag.weights["model.demo.fact_orders"] == 20.0
    assert dag.weights["model.demo.stg_customers"] == 2.0
    # Sources present in run_results with 0.0 execution time
    assert dag.weights["source.demo.raw.orders"] == 0.0


def test_build_dag_zero_weight_for_missing_run_result() -> None:
    artifacts = DagArtifacts(
        manifest={
            "nodes": {"model.x.a": {}, "model.x.b": {}},
            "sources": {},
            "child_map": {"model.x.a": ["model.x.b"], "model.x.b": []},
            "parent_map": {"model.x.a": [], "model.x.b": ["model.x.a"]},
        },
        run_results={"results": [{"unique_id": "model.x.a", "execution_time": 4.0}]},
    )
    dag = build_dag(artifacts)
    assert dag.weights["model.x.a"] == 4.0
    assert dag.weights["model.x.b"] == 0.0


def test_build_dag_filters_non_runnable_ids() -> None:
    artifacts = DagArtifacts(
        manifest={
            "nodes": {"model.x.a": {}, "test.x.assert_positive": {}},
            "sources": {},
            "child_map": {"model.x.a": ["test.x.assert_positive"], "test.x.assert_positive": []},
            "parent_map": {"test.x.assert_positive": ["model.x.a"], "model.x.a": []},
        },
        run_results={"results": []},
    )
    dag = build_dag(artifacts)
    assert "test.x.assert_positive" not in dag.nodes
    # child edge to the filtered test should be dropped
    assert dag.child_map["model.x.a"] == []


def test_build_dag_raises_when_no_runnable_nodes() -> None:
    artifacts = DagArtifacts(
        manifest={
            "nodes": {"test.x.t": {}},
            "sources": {},
            "child_map": {"test.x.t": []},
            "parent_map": {"test.x.t": []},
        },
        run_results={"results": []},
    )
    with pytest.raises(InvalidArtifactError, match="no runnable nodes"):
        build_dag(artifacts)


def test_build_dag_tolerates_null_execution_time() -> None:
    artifacts = DagArtifacts(
        manifest={
            "nodes": {"model.x.a": {}},
            "sources": {},
            "child_map": {"model.x.a": []},
            "parent_map": {"model.x.a": []},
        },
        run_results={"results": [{"unique_id": "model.x.a", "execution_time": None}]},
    )
    dag = build_dag(artifacts)
    assert dag.weights["model.x.a"] == 0.0
