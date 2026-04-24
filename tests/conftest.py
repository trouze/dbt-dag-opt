from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from dbt_dag_opt.models import DagArtifacts

FIXTURES_DIR = Path(__file__).parent / "fixtures"
DBT_DUGOUT_DIR = FIXTURES_DIR / "dbt_dugout"
DEMO_PROJECT_DIR = FIXTURES_DIR / "demo_project"


@pytest.fixture
def tiny_manifest_path() -> Path:
    return FIXTURES_DIR / "tiny_manifest.json"


@pytest.fixture
def tiny_run_results_path() -> Path:
    return FIXTURES_DIR / "tiny_run_results.json"


@pytest.fixture
def tiny_artifacts(tiny_manifest_path: Path, tiny_run_results_path: Path) -> DagArtifacts:
    with tiny_manifest_path.open() as fh:
        manifest = json.load(fh)
    with tiny_run_results_path.open() as fh:
        run_results = json.load(fh)
    return DagArtifacts(manifest=manifest, run_results=run_results)


@pytest.fixture
def dbt_dugout_manifest_path() -> Path:
    return DBT_DUGOUT_DIR / "manifest.json"


@pytest.fixture
def dbt_dugout_run_results_path() -> Path:
    return DBT_DUGOUT_DIR / "run_results.json"


@pytest.fixture
def dbt_dugout_artifacts(
    dbt_dugout_manifest_path: Path, dbt_dugout_run_results_path: Path
) -> DagArtifacts:
    with dbt_dugout_manifest_path.open() as fh:
        manifest = json.load(fh)
    with dbt_dugout_run_results_path.open() as fh:
        run_results = json.load(fh)
    return DagArtifacts(manifest=manifest, run_results=run_results)


@pytest.fixture
def demo_project_manifest_path() -> Path:
    return DEMO_PROJECT_DIR / "manifest.json"


@pytest.fixture
def demo_project_run_results_path() -> Path:
    return DEMO_PROJECT_DIR / "run_results.json"


@pytest.fixture
def demo_project_artifacts(
    demo_project_manifest_path: Path, demo_project_run_results_path: Path
) -> DagArtifacts:
    with demo_project_manifest_path.open() as fh:
        manifest = json.load(fh)
    with demo_project_run_results_path.open() as fh:
        run_results = json.load(fh)
    return DagArtifacts(manifest=manifest, run_results=run_results)


def _phase(started: str, completed: str, name: str = "execute") -> dict[str, str]:
    return {"name": name, "started_at": started, "completed_at": completed}


@pytest.fixture
def synthetic_replay_artifacts() -> DagArtifacts:
    """A 4-node, 2-thread run with known structure.

    Graph:  A -> B -> D
            A -> C -> D

    Thread-1:  A(0-10)  B(10-20)  [idle 20-30 waiting on C]  D(30-40)
    Thread-2:  [idle 0-10 startup]  C(10-30)  [done; span ends at 30]

    Observed critical path = A -> C -> D (40s span).
    """
    t0 = "2026-04-24T00:00:00.000000+00:00"
    t10 = "2026-04-24T00:00:10.000000+00:00"
    t20 = "2026-04-24T00:00:20.000000+00:00"
    t30 = "2026-04-24T00:00:30.000000+00:00"
    t40 = "2026-04-24T00:00:40.000000+00:00"

    manifest: dict[str, Any] = {
        "nodes": {
            "model.demo.A": {},
            "model.demo.B": {},
            "model.demo.C": {},
            "model.demo.D": {},
        },
        "sources": {},
        "parent_map": {
            "model.demo.A": [],
            "model.demo.B": ["model.demo.A"],
            "model.demo.C": ["model.demo.A"],
            "model.demo.D": ["model.demo.B", "model.demo.C"],
        },
        "child_map": {
            "model.demo.A": ["model.demo.B", "model.demo.C"],
            "model.demo.B": ["model.demo.D"],
            "model.demo.C": ["model.demo.D"],
            "model.demo.D": [],
        },
    }

    run_results: dict[str, Any] = {
        "elapsed_time": 40.0,
        "results": [
            {
                "unique_id": "model.demo.A",
                "thread_id": "Thread-1",
                "status": "success",
                "execution_time": 10.0,
                "timing": [_phase(t0, t10)],
            },
            {
                "unique_id": "model.demo.B",
                "thread_id": "Thread-1",
                "status": "success",
                "execution_time": 10.0,
                "timing": [_phase(t10, t20)],
            },
            {
                "unique_id": "model.demo.C",
                "thread_id": "Thread-2",
                "status": "success",
                "execution_time": 20.0,
                "timing": [_phase(t10, t30)],
            },
            {
                "unique_id": "model.demo.D",
                "thread_id": "Thread-1",
                "status": "success",
                "execution_time": 10.0,
                "timing": [_phase(t30, t40)],
            },
        ],
    }
    return DagArtifacts(manifest=manifest, run_results=run_results)
