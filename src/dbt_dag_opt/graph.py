"""Construct a weighted dbt DAG from raw artifacts."""

from __future__ import annotations

from typing import Any

from dbt_dag_opt.errors import InvalidArtifactError
from dbt_dag_opt.models import Dag, DagArtifacts

# dbt unique_id prefixes that count as "real" graph nodes. Tests, exposures, and
# semantic models are excluded — they don't contribute to pipeline wall-clock.
_RUNNABLE_PREFIXES = ("model.", "source.", "seed.", "snapshot.")


def build_dag(
    artifacts: DagArtifacts, *, include_prefixes: tuple[str, ...] = _RUNNABLE_PREFIXES
) -> Dag:
    manifest = artifacts.manifest
    run_results = artifacts.run_results

    manifest_nodes: dict[str, Any] = manifest["nodes"]
    manifest_sources: dict[str, Any] = manifest.get("sources", {})
    raw_child_map: dict[str, list[str]] = manifest["child_map"]
    raw_parent_map: dict[str, list[str]] = manifest["parent_map"]

    all_ids: set[str] = (
        set(manifest_nodes) | set(manifest_sources) | set(raw_child_map) | set(raw_parent_map)
    )
    nodes = {nid for nid in all_ids if nid.startswith(include_prefixes)}

    if not nodes:
        raise InvalidArtifactError(
            "no runnable nodes found in manifest (expected model./source./seed./snapshot. ids)"
        )

    child_map = {
        nid: [child for child in raw_child_map.get(nid, []) if child in nodes] for nid in nodes
    }
    parent_map = {
        nid: [parent for parent in raw_parent_map.get(nid, []) if parent in nodes] for nid in nodes
    }

    weights = _build_weights(nodes, run_results.get("results", []))

    sources = sorted(nid for nid in nodes if not parent_map[nid])

    return Dag(
        child_map=child_map,
        parent_map=parent_map,
        weights=weights,
        sources=sources,
        nodes=frozenset(nodes),
    )


def _build_weights(nodes: set[str], results: list[dict[str, Any]]) -> dict[str, float]:
    by_id: dict[str, float] = {}
    for result in results:
        unique_id = result.get("unique_id")
        if not isinstance(unique_id, str):
            continue
        raw = result.get("execution_time", 0.0)
        try:
            by_id[unique_id] = float(raw) if raw is not None else 0.0
        except (TypeError, ValueError):
            by_id[unique_id] = 0.0
    return {nid: by_id.get(nid, 0.0) for nid in nodes}
