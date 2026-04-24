# Changelog

All notable changes to this project will be documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `replay` cost overlay: pass `--warehouse-size` (Snowflake XS…6XL) or `--credits-per-hour` (non-Snowflake adapters) to translate wall-clock into dollars. Renders **Run cost**, **Critical-path floor**, **Headroom** (= run − floor; the prize for better parallelization), and **Idle cost** (the $ equivalent of thread-idle warehouse-seconds). Defaults to $2.00/credit (Snowflake Standard On-Demand); override with `--rate-per-credit`. Snowflake's 60-second minimum-billing floor is applied automatically; pass `--no-minimum-billing` to see raw wall-clock × rate.
- New module `dbt_dag_opt.cost` with `CostInputs`, `CostReport`, `compute_cost()`, `credits_per_hour_for()`, and `cost_inputs_from_replay()`. Designed primitive-first so a future `whatif` simulator can call `compute_cost` against simulated schedules and diff the resulting `CostReport`s.

## [0.1.0] - 2026-04-24

Initial PyPI release. Complete rewrite of the pre-release prototype.

### Added

- `dbt-dag-opt analyze` CLI (Typer) with two input modes:
  - File mode: `--manifest` and `--run-results` point at local dbt artifacts.
  - Cloud mode: `--account-id`, `--job-id`, optional `--run-id`, and `DBT_CLOUD_TOKEN` env var (or `--token`) pull artifacts from the dbt Cloud Admin API.
- `analyze --show-path` to render the full chain of node ids for each longest path in the table output.
- `analyze` table includes a **Bottleneck** column naming the slowest model on each path. A bottleneck that appears across multiple rows is a shared-node optimization target.
- `dbt-dag-opt replay` subcommand: reconstructs the observed schedule from `run_results.json`'s `thread_id` + per-phase `timing` data, joined against `manifest.json`'s `parent_map`. Reports per-thread utilization, observed critical path (walked backwards from the last-completing node), and top idle gaps with parent-node attribution.
- Output formats: `analyze` → `table` (rich terminal, default), `json` (valid, `jq`-friendly), `jsonl`. `replay` → `text` (rich terminal summary, default), `json` (full replay report including raw events).
- `--top N` to limit `analyze` results; `--top-idle-gaps N` for `replay`; `--output` to write any command to a file.
- Typed exceptions (`ArtifactLoadError`, `DbtCloudAPIError`, `InvalidArtifactError`, `GraphError`).
- Package ships with `py.typed` (PEP 561).
- Integration fixture at `tests/fixtures/dbt_dugout/` — a real Snowflake dbt run (57 nodes, 4 threads) used to smoke-test `replay` end-to-end.
- CI matrix across Python 3.10 / 3.11 / 3.12.
- PyPI publishing via Trusted Publishers (OIDC) on tag push.

### Changed (vs. prototype)

- Replaced per-source recursive DFS + ProcessPoolExecutor with a single iterative DP over topological order. O(V + E) across all sources, no recursion-limit risk, no 20s per-task timeout.
- Node weights are now attached to the *target* node of each path hop (fixes a bug where parent weights were assigned to outgoing edges).
- Adjacency list replaces full-edge-list rescan on every DFS step.
- Output is valid JSON by default (prototype's `longest_paths.json` was a stream of comma-separated fragments opened in append mode — not parseable).
