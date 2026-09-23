#!/usr/bin/env python3
"""Apply the predeclared #4819 throughput decision only.

Correctness, memory, allocation, residency, storage, and review gates remain
separate and require evidence beyond this paired timing calculation.
"""

import argparse
import json
import re
import statistics
from pathlib import Path


def load_cell(root: Path, scale: str, ordinal: int):
    depth = (0, 1, 1, 0, 0, 1, 1, 0, 0, 1)[ordinal - 1]
    path = root / f"{scale}-{ordinal:02d}-engine-{depth}-input-1"
    assert (path / "validation.json").is_file(), path
    result = json.loads((path / "result.json").read_text())
    load = result["load"]
    assert load["engine_prepare_depth"] == depth, path
    assert load["pipeline_depth"] == 1, path
    assert load["engine_prepare_path"] == "prepared", path
    assert load["engine_idle_scratch_reserve_bytes"] == 32 << 20, path
    assert 0 < load["engine_peak_reserved_bytes"] <= 1_342_177_280, path
    rss_match = re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", (path / "time.txt").read_text())
    assert rss_match, path
    return {
        "ordinal": ordinal,
        "depth": depth,
        "wall_seconds": load["wall_seconds"],
        "input_overlap_seconds": load["input_overlap_seconds"],
        "engine_overlap_seconds": load["engine_prepare_commit_overlap_seconds"],
        "producer_credit_wait_seconds": load["producer_credit_wait_seconds"],
        "producer_source_credit_wait_seconds": load["producer_source_credit_wait_seconds"],
        "producer_prepare_credit_wait_seconds": load["producer_prepare_credit_wait_seconds"],
        "producer_retry_wait_seconds": load["producer_retry_wait_seconds"],
        "engine_budget_retry_batches": load["engine_budget_retry_batches"],
        "allocated_bytes_per_row": load["allocated_bytes_per_row"],
        "allocations_per_row": load["allocations_per_row"],
        "peak_reserved_bytes": load["engine_peak_reserved_bytes"],
        "idle_scratch_reserve_bytes": load["engine_idle_scratch_reserve_bytes"],
        "peak_rss_kib": int(rss_match.group(1)),
        "durable_storage_bytes": result["storage"]["durable_storage_bytes_wal_excluded"],
    }


def summarize(root: Path, scale: str):
    cells = [load_cell(root, scale, i) for i in range(1, 11)]
    controls = [cell for cell in cells if cell["depth"] == 0]
    candidates = [cell for cell in cells if cell["depth"] == 1]
    pairs = []
    for i in range(0, 10, 2):
        first, second = cells[i : i + 2]
        a, b = (first, second) if first["depth"] == 0 else (second, first)
        pairs.append(a["wall_seconds"] - b["wall_seconds"])
    a_median = statistics.median(cell["wall_seconds"] for cell in controls)
    b_median = statistics.median(cell["wall_seconds"] for cell in candidates)
    a_spread = max(abs(cell["wall_seconds"] - a_median) for cell in controls)
    gain = a_median - b_median
    positive_pairs = sum(delta > 0 for delta in pairs)
    return {
        "scale": scale,
        "control_median_seconds": a_median,
        "candidate_median_seconds": b_median,
        "median_gain_seconds": gain,
        "maximum_control_deviation_seconds": a_spread,
        "paired_deltas_seconds": pairs,
        "positive_pairs": positive_pairs,
        "throughput_gate_pass": positive_pairs >= 4 and gain > a_spread,
        "cells": cells,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("matrix", type=Path)
    args = parser.parse_args()
    assert (args.matrix / "complete.txt").is_file(), "matrix is incomplete"
    summary = {scale: summarize(args.matrix, scale) for scale in ("1m", "10m")}
    print(json.dumps(summary, indent=2, sort_keys=True))
    raise SystemExit(0 if all(item["throughput_gate_pass"] for item in summary.values()) else 1)
