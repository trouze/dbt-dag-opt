# Changelog

All notable changes to this project will be documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `dbt-dag-opt replay` subcommand: reconstructs the observed schedule from `run_results.json`'s `thread_id` + per-phase `timing` data, joined against `manifest.json`'s `parent_map`. Reports per-thread utilization, observed critical path (walked backwards from the last-completing node), and top idle gaps with parent-node attribution.
- Output formats for `replay`: `text` (rich terminal summary, default) and `json` (full replay report, including raw events).
- Integration fixture at `tests/fixtures/dbt_dugout/` — a real Snowflake dbt run (57 nodes, 4 threads) used to smoke-test `replay` end-to-end.
- `analyze --show-path`: render the full chain of node ids for each longest path in the table output.
- `analyze` table now includes a **Bottleneck** column naming the slowest model on each path. A bottleneck that appears across multiple rows is a shared-node optimization target.

## [0.1.0] - 2026-04-24

Initial PyPI release. Complete rewrite of the pre-release prototype.

### Added

- `dbt-dag-opt analyze` CLI (Typer) with two input modes:
  - File mode: `--manifest` and `--run-results` point at local dbt artifacts.
  - Cloud mode: `--account-id`, `--job-id`, optional `--run-id`, and `DBT_CLOUD_TOKEN` env var (or `--token`) pull artifacts from the dbt Cloud Admin API.
- Output formats: `table` (rich terminal), `json` (valid, `jq`-friendly), `jsonl`.
- `--top N` to limit results; `--output` to write to a file.
- Typed exceptions (`ArtifactLoadError`, `DbtCloudAPIError`, `InvalidArtifactError`, `GraphError`).
- Package ships with `py.typed` (PEP 561).
- CI matrix across Python 3.10 / 3.11 / 3.12.
- PyPI publishing via Trusted Publishers (OIDC) on tag push.

### Changed (vs. prototype)

- Replaced per-source recursive DFS + ProcessPoolExecutor with a single iterative DP over topological order. O(V + E) across all sources, no recursion-limit risk, no 20s per-task timeout.
- Node weights are now attached to the *target* node of each path hop (fixes a bug where parent weights were assigned to outgoing edges).
- Adjacency list replaces full-edge-list rescan on every DFS step.
- Output is valid JSON by default (prototype's `longest_paths.json` was a stream of comma-separated fragments opened in append mode — not parseable).

### Notes for PyPI Trusted Publishing

Before the first `v*` tag is pushed, configure PyPI: Project settings → Publishing → Add GitHub publisher with `trouze/dbt-dag-opt` / workflow `publish.yml` / environment `pypi`.
