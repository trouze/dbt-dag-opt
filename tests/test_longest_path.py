from __future__ import annotations

import pytest

from dbt_dag_opt.errors import GraphError
from dbt_dag_opt.graph import build_dag
from dbt_dag_opt.longest_path import longest_paths_from_each_source
from dbt_dag_opt.models import Dag, DagArtifacts


def test_longest_path_on_tiny_fixture(tiny_artifacts: DagArtifacts) -> None:
    dag = build_dag(tiny_artifacts)
    results = {r.source: r for r in longest_paths_from_each_source(dag)}

    # raw.orders → stg_orders (5) → int_orders (10) → fact_orders (20) = 35
    orders = results["source.demo.raw.orders"]
    assert orders.distance == 35.0
    assert orders.path == [
        "source.demo.raw.orders",
        "model.demo.stg_orders",
        "model.demo.int_orders",
        "model.demo.fact_orders",
    ]

    # raw.customers → stg_customers (2) → int_orders (10) → fact_orders (20) = 32
    # beats raw.customers → stg_customers (2) → dim_customers (3) → fact_orders (20) = 25
    customers = results["source.demo.raw.customers"]
    assert customers.distance == 32.0
    assert customers.path[-1] == "model.demo.fact_orders"
    assert "model.demo.int_orders" in customers.path


def test_single_node_dag() -> None:
    artifacts = DagArtifacts(
        manifest={
            "nodes": {"model.x.a": {}},
            "sources": {},
            "child_map": {"model.x.a": []},
            "parent_map": {"model.x.a": []},
        },
        run_results={"results": [{"unique_id": "model.x.a", "execution_time": 7.5}]},
    )
    dag = build_dag(artifacts)
    [result] = longest_paths_from_each_source(dag)
    assert result.path == ["model.x.a"]
    assert result.distance == 7.5


def test_linear_chain() -> None:
    # a -> b -> c -> d with weights 1, 2, 3, 4 => longest from a = 10
    ids = ["model.x.a", "model.x.b", "model.x.c", "model.x.d"]
    weights = [1.0, 2.0, 3.0, 4.0]
    artifacts = DagArtifacts(
        manifest={
            "nodes": {i: {} for i in ids},
            "sources": {},
            "child_map": {
                ids[0]: [ids[1]],
                ids[1]: [ids[2]],
                ids[2]: [ids[3]],
                ids[3]: [],
            },
            "parent_map": {
                ids[0]: [],
                ids[1]: [ids[0]],
                ids[2]: [ids[1]],
                ids[3]: [ids[2]],
            },
        },
        run_results={
            "results": [
                {"unique_id": i, "execution_time": w} for i, w in zip(ids, weights, strict=True)
            ]
        },
    )
    dag = build_dag(artifacts)
    [result] = longest_paths_from_each_source(dag)
    assert result.path == ids
    assert result.distance == sum(weights)


def test_cycle_raises() -> None:
    # a -> b -> a (cycle). Such a manifest shouldn't exist in practice but we
    # guard against it because topo sort is the load-bearing step.
    dag = Dag(
        child_map={"model.x.a": ["model.x.b"], "model.x.b": ["model.x.a"]},
        parent_map={"model.x.a": ["model.x.b"], "model.x.b": ["model.x.a"]},
        weights={"model.x.a": 1.0, "model.x.b": 1.0},
        sources=[],
        nodes=frozenset({"model.x.a", "model.x.b"}),
    )
    with pytest.raises(GraphError, match="cycle"):
        longest_paths_from_each_source(dag)


def test_diamond_picks_heavier_branch() -> None:
    # a -> b -> d and a -> c -> d. If b=10 and c=1, longest = a->b->d
    artifacts = DagArtifacts(
        manifest={
            "nodes": {f"model.x.{n}": {} for n in "abcd"},
            "sources": {},
            "child_map": {
                "model.x.a": ["model.x.b", "model.x.c"],
                "model.x.b": ["model.x.d"],
                "model.x.c": ["model.x.d"],
                "model.x.d": [],
            },
            "parent_map": {
                "model.x.a": [],
                "model.x.b": ["model.x.a"],
                "model.x.c": ["model.x.a"],
                "model.x.d": ["model.x.b", "model.x.c"],
            },
        },
        run_results={
            "results": [
                {"unique_id": "model.x.a", "execution_time": 1.0},
                {"unique_id": "model.x.b", "execution_time": 10.0},
                {"unique_id": "model.x.c", "execution_time": 1.0},
                {"unique_id": "model.x.d", "execution_time": 5.0},
            ]
        },
    )
    dag = build_dag(artifacts)
    [result] = longest_paths_from_each_source(dag)
    assert result.path == ["model.x.a", "model.x.b", "model.x.d"]
    assert result.distance == 16.0
