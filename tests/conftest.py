from __future__ import annotations

import json
from pathlib import Path

import pytest

from dbt_dag_opt.models import DagArtifacts

FIXTURES_DIR = Path(__file__).parent / "fixtures"


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
