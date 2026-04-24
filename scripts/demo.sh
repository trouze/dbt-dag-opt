#!/usr/bin/env bash
# Demo script for dbt-dag-opt. Designed to be recorded (asciinema / QuickTime).
#
# Runs against a synthetic 24-model dbt project under tests/fixtures/demo_project/
# — a baseball analytics warehouse with 4 threads, ~7.5 min wall-clock, and one
# shared bottleneck (int_game_events) sitting on three of the top longest paths.
#
# Usage:
#   ./scripts/demo.sh                  # interactive: Enter advances each step
#   AUTO=1 PAUSE=2 ./scripts/demo.sh   # non-interactive: 2s sleep per step
#   AUTO=1 PAUSE=0 ./scripts/demo.sh   # rapid dry-run (CI smoke)

set -euo pipefail

AUTO="${AUTO:-0}"
PAUSE="${PAUSE:-0.5}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MANIFEST="$ROOT/tests/fixtures/demo_project/manifest.json"
RUN_RESULTS="$ROOT/tests/fixtures/demo_project/run_results.json"

bold() { printf "\033[1m%s\033[0m\n" "$*"; }
dim() { printf "\033[2m%s\033[0m\n" "$*"; }
section() {
    echo
    printf "\033[1;36m▌ %s\033[0m\n" "$*"
    echo
}
wait_step() {
    if [ "$AUTO" = "1" ]; then
        sleep "$PAUSE"
        return
    fi
    # Dim hint, then clear the line after Enter so the recording stays clean.
    printf "\033[2m  ↵ \033[0m"
    read -r _
    printf "\033[1A\033[2K"
}
run() {
    bold "\$ $*"
    wait_step
    eval "$@"
    echo
}

section "1 · Which paths through the DAG are actually slow?"
dim "analyze uses manifest + run_results to compute the critical path — the"
dim "longest cumulative chain of model execution times. That's the bound on"
dim "how fast your pipeline could possibly run."
run "uv run dbt-dag-opt analyze --manifest \"$MANIFEST\" --run-results \"$RUN_RESULTS\" --top 5"

section "2 · The Bottleneck column names the slowest model on each path"
dim "Watch for a model that appears as the bottleneck on MULTIPLE rows — that's"
dim "shared-node leverage. Optimizing it speeds up several paths at once."

section "3 · Drill into the full chain with --show-path"
run "uv run dbt-dag-opt analyze --manifest \"$MANIFEST\" --run-results \"$RUN_RESULTS\" --top 3 --show-path"

section "4 · What actually happened? (replay reconstructs the observed schedule)"
dim "replay reads thread_id + timing from run_results to reconstruct the"
dim "per-thread Gantt, identify the observed critical path, and attribute"
dim "every idle gap to the upstream model a thread was waiting on."
run "uv run dbt-dag-opt replay --manifest \"$MANIFEST\" --run-results \"$RUN_RESULTS\" --top-idle-gaps 5"

section "5 · Put a price on it: --warehouse-size translates wall-clock to dollars"
dim "Four framed numbers:"
dim "  • Run cost         — what this run billed"
dim "  • Critical-path floor — the irreducible cost of your slowest chain"
dim "  • Headroom         — run − floor; prize for better parallelization"
dim "  • Idle cost        — \$ equivalent of thread-idle warehouse-seconds"
run "uv run dbt-dag-opt replay --manifest \"$MANIFEST\" --run-results \"$RUN_RESULTS\" --warehouse-size L --top-idle-gaps 3"

section "6 · Change the warehouse, change the bill (same run, XL)"
dim "Doubling warehouse size doubles the rate. Same wall-clock, 2x cost."
run "uv run dbt-dag-opt replay --manifest \"$MANIFEST\" --run-results \"$RUN_RESULTS\" --warehouse-size XL --top-idle-gaps 0"

section "7 · Non-Snowflake adapters: pass --credits-per-hour directly"
dim "Databricks, BigQuery, Redshift — pass the cost/hour your adapter charges."
run "uv run dbt-dag-opt replay --manifest \"$MANIFEST\" --run-results \"$RUN_RESULTS\" --credits-per-hour 12 --rate-per-credit 1.5 --top-idle-gaps 0"

section "8 · Machine-readable: --format json"
dim "Everything in the text output is also in JSON — pipe to jq for dashboards,"
dim "Slack alerts, or CI annotations."
run "uv run dbt-dag-opt replay --manifest \"$MANIFEST\" --run-results \"$RUN_RESULTS\" --warehouse-size L --format json | jq '.cost'"

section "Wrap"
dim "Three takeaways from this run:"
dim "  1. int_game_events is the shared bottleneck on 3 of the top 5 paths."
dim "  2. 5% of the bill is pure parallelism headroom (small — DAG is well-shaped)."
dim "  3. 30% of warehouse-seconds are idle threads — you're overprovisioned"
dim "     on thread count for this DAG shape. Consider --threads 2 next run."
echo
bold "pip install dbt-dag-opt"
echo
