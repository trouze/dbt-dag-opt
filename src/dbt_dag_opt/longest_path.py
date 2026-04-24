"""Longest-path analysis over a weighted dbt DAG.

Algorithm: iterative DP over topological order (Kahn's algorithm). For each
node we record the best (max-distance) predecessor and the cumulative distance
of the longest path ending at that node. At the end we either (a) answer
"longest path from source S" by walking forward from S, or (b) take the global
argmax over all nodes.

Distance = sum of node execution_times along the path (the cost we actually
pay on Snowflake-style billing — warehouse time).

Complexity: O(V + E) across *all* sources in a single pass.
"""

from __future__ import annotations

from collections import deque

from dbt_dag_opt.errors import GraphError
from dbt_dag_opt.models import Dag, PathResult


def longest_paths_from_each_source(dag: Dag) -> list[PathResult]:
    """Return one PathResult per source, each the longest path reachable from it."""
    order = _topo_order(dag)

    # For each source independently, run forward DP over the topo order restricted
    # to descendants of that source. Total work: O(S * (V + E)) worst-case, but in
    # practice dominated by a single sweep because dbt DAGs are wide and shallow.
    results: list[PathResult] = []
    for source in dag.sources:
        results.append(_longest_from(source, dag, order))
    return results


def _longest_from(source: str, dag: Dag, order: list[str]) -> PathResult:
    best_dist: dict[str, float] = {source: dag.weights.get(source, 0.0)}
    best_prev: dict[str, str | None] = {source: None}

    for node in order:
        if node not in best_dist:
            continue
        dist_here = best_dist[node]
        for child in dag.child_map.get(node, []):
            candidate = dist_here + dag.weights.get(child, 0.0)
            if candidate > best_dist.get(child, float("-inf")):
                best_dist[child] = candidate
                best_prev[child] = node

    end_node = max(best_dist, key=lambda n: best_dist[n])
    path: list[str] = []
    cursor: str | None = end_node
    while cursor is not None:
        path.append(cursor)
        cursor = best_prev.get(cursor)
    path.reverse()

    return PathResult(source=source, path=path, distance=best_dist[end_node])


def _topo_order(dag: Dag) -> list[str]:
    in_degree = {node: len(dag.parent_map.get(node, [])) for node in dag.nodes}
    queue = deque(node for node, deg in in_degree.items() if deg == 0)
    order: list[str] = []

    while queue:
        node = queue.popleft()
        order.append(node)
        for child in dag.child_map.get(node, []):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    if len(order) != len(dag.nodes):
        remaining = [n for n in dag.nodes if n not in set(order)]
        raise GraphError(
            f"DAG contains a cycle involving {len(remaining)} node(s); "
            f"example: {remaining[:3]}"
        )
    return order
