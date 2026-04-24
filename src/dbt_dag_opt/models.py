from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DagArtifacts:
    """Raw dbt artifacts needed for critical-path analysis.

    `manifest` and `run_results` are the parsed JSON bodies of dbt's
    `manifest.json` and `run_results.json`.
    """

    manifest: dict[str, Any]
    run_results: dict[str, Any]


@dataclass(frozen=True)
class Dag:
    """A dbt DAG with execution-time weights attached to each node.

    Edges are stored as a child_map: parent_id -> list of child_ids.
    `weights[node_id]` is the node's execution_time in seconds (0.0 if missing).
    `sources` are nodes with no parents (dbt sources or orphan models).
    """

    child_map: dict[str, list[str]]
    parent_map: dict[str, list[str]]
    weights: dict[str, float]
    sources: list[str]
    nodes: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True)
class PathResult:
    """Longest path from a given source to the leaf of maximum cumulative time."""

    source: str
    path: list[str]
    distance: float
