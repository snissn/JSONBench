#!/usr/bin/env bash
set -euo pipefail

# Freeze the product and harness before invoking. Every cell gets a new DB.
for name in BIN BIN_SHA256 BUILD_MANIFEST DATA_DIR FIXTURE_SHA256 ENGINE_SHA LOADER_SHA ANALYZER OUT ENGINE_PREPARE_MAX_BYTES ENGINE_IDLE_SCRATCH_RESERVE_BYTES; do
  [[ -n "$(printenv "$name" 2>/dev/null || true)" ]] || { echo "missing $name" >&2; exit 2; }
done
[[ "$ENGINE_SHA" =~ ^[0-9a-f]{40}$ && "$LOADER_SHA" =~ ^[0-9a-f]{40}$ ]] || { echo "engine and loader identities must be full commit SHAs" >&2; exit 2; }
[[ "$ENGINE_PREPARE_MAX_BYTES" =~ ^[1-9][0-9]*$ && "$ENGINE_IDLE_SCRATCH_RESERVE_BYTES" =~ ^[0-9]+$ ]] || { echo "memory gate values must be nonnegative integer bytes" >&2; exit 2; }
(( ENGINE_PREPARE_MAX_BYTES > ENGINE_IDLE_SCRATCH_RESERVE_BYTES )) || { echo "memory gate must exceed idle scratch reserve" >&2; exit 2; }
[[ ! -e "$OUT" ]] || { echo "refusing existing output: $OUT" >&2; exit 2; }
[[ -x "$BIN" && -f "$BUILD_MANIFEST" && -d "$DATA_DIR" && -f "$ANALYZER" ]] || { echo "binary, build manifest, fixture, or analyzer missing" >&2; exit 2; }
actual_binary=$(sha256sum "$BIN" | awk '{print $1}')
[[ "$actual_binary" == "$BIN_SHA256" ]] || { echo "binary hash mismatch: $actual_binary" >&2; exit 2; }
for identity in "engine=$ENGINE_SHA" "loader=$LOADER_SHA" "binary_sha256=$actual_binary"; do
  grep -Fxq "$identity" "$BUILD_MANIFEST" || { echo "build manifest missing $identity" >&2; exit 2; }
done
mkdir "$OUT"
cp "$0" "$OUT/harness.sh"
sha256sum "$OUT/harness.sh" > "$OUT/harness.sha256"
cp "$ANALYZER" "$OUT/analyze.py"
sha256sum "$OUT/analyze.py" > "$OUT/analyze.sha256"
cp "$BUILD_MANIFEST" "$OUT/build-manifest.txt"
sha256sum "$OUT/build-manifest.txt" > "$OUT/build-manifest.sha256"
go version -m "$BIN" > "$OUT/go-build-info.txt"
gomap_build=$(grep -F $'\tdep\tgithub.com/snissn/gomap\t' "$OUT/go-build-info.txt" || true)
[[ "$gomap_build" == *"-${ENGINE_SHA:0:12}"* ]] || { echo "binary does not embed the pinned engine version" >&2; exit 2; }
if grep -F -A1 $'\tdep\tgithub.com/snissn/gomap\t' "$OUT/go-build-info.txt" | grep -q $'\t=>'; then
  echo "binary contains a local gomap replacement" >&2; exit 2
fi
(
  cd "$DATA_DIR"
  find . -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum
) > "$OUT/fixture-files.sha256"
actual_fixture=$(sha256sum "$OUT/fixture-files.sha256" | awk '{print $1}')
[[ "$actual_fixture" == "$FIXTURE_SHA256" ]] || { echo "fixture hash mismatch: $actual_fixture" >&2; exit 2; }
printf 'engine=%s\nloader=%s\nbinary_sha256=%s\nfixture_sha256=%s\nengine_prepare_max_bytes=%s\nengine_idle_scratch_reserve_bytes=%s\n' "$ENGINE_SHA" "$LOADER_SHA" "$actual_binary" "$actual_fixture" "$ENGINE_PREPARE_MAX_BYTES" "$ENGINE_IDLE_SCRATCH_RESERVE_BYTES" > "$OUT/identity.txt"
{ date -u; uname -a; lscpu; df -h "$OUT" "$DATA_DIR"; uptime; } > "$OUT/host-start.txt"
printf 'GOWORK=off\nGOMAXPROCS=12\n' > "$OUT/environment.txt"

run_cell() {
  local scale="$1" ordinal="$2" depth="$3" input_depth="$4"
  local cell="$OUT/$scale-$ordinal-engine-$depth-input-$input_depth"
  mkdir "$cell"
  date -u > "$cell/started.txt"
  uptime > "$cell/uptime-before.txt"
  df -h "$cell" > "$cell/storage-before.txt"
  printf 'scale=%s ordinal=%s engine_prepare_depth=%s load_pipeline_depth=%s\n' "$scale" "$ordinal" "$depth" "$input_depth" > "$cell/parameters.txt"
  GOWORK=off GOMAXPROCS=12 /usr/bin/time -v -o "$cell/time.txt" "$BIN" run \
    -data-dir "$DATA_DIR" -format json \
    -storage-layout column-store-full-prepared \
    -query-mode one_shot_end_to_end -metadata-mode no_aggregate_metadata \
    -projection full -queries q1,q2,q3,q4,q5,qexpr \
    -batch-size 16000 -tries 1 -profile durable -data-root fast \
    -allow-errors -load-pipeline-depth "$input_depth" -engine-prepare-depth "$depth" \
    -engine-prepare-max-bytes "$ENGINE_PREPARE_MAX_BYTES" -scale "$scale" \
    -db-dir "$cell/db" -out "$cell/result.json" \
    > "$cell/stdout.txt" 2> "$cell/stderr.txt"
  python3 - "$cell/result.json" "$cell/validation.json" "$OUT/$scale-query-hashes.json" "$scale" "$ENGINE_PREPARE_MAX_BYTES" "$ENGINE_IDLE_SCRATCH_RESERVE_BYTES" <<'PY'
import json, math, pathlib, sys
result_path, validation_path, hashes_path, scale = map(pathlib.Path, sys.argv[1:5])
max_bytes, idle_bytes = map(int, sys.argv[5:7])
with result_path.open() as f:
    result = json.load(f)
load = result["load"]
expected = {"1m": (1_000_000, 0), "10m": (9_999_994, 6)}[str(scale)]
assert (load["rows"], load["input_rows"] - load["rows"]) == expected
assert load.get("skipped_invalid_json_rows", 0) == expected[1]
assert load["engine_prepare_path"] == "prepared"
assert load["engine_prepared_batches"] == load["engine_committed_batches"] > 0
assert load.get("engine_abandoned_batches", 0) == 0
assert load.get("engine_fallback_batches", 0) == 0
assert 0 < load["engine_peak_owned_bytes"] <= max_bytes
assert 0 < load["engine_peak_reserved_bytes"] <= max_bytes
assert load["engine_idle_scratch_reserve_bytes"] == idle_bytes
assert math.isfinite(load["wall_seconds"]) and load["wall_seconds"] > 0
for counter in ("input_overlap_seconds", "engine_prepare_commit_overlap_seconds", "producer_credit_wait_seconds", "producer_source_credit_wait_seconds", "producer_prepare_credit_wait_seconds", "producer_retry_wait_seconds"):
    assert math.isfinite(load[counter]) and load[counter] >= 0, counter
assert load["engine_budget_retry_batches"] >= 0
hashes = {q["name"]: q["result_hash"] for q in result["queries"]}
assert set(hashes) == {"q1", "q2", "q3", "q4", "q5", "qexpr"}
assert all(len(value) == 64 for value in hashes.values())
if hashes_path.exists():
    assert hashes == json.loads(hashes_path.read_text()), "query hash mismatch"
else:
    hashes_path.write_text(json.dumps(hashes, sort_keys=True, indent=2) + "\n")
validation = {"rows": load["rows"], "skipped": expected[1], "wall_seconds": load["wall_seconds"], "query_hashes": hashes, "engine_peak_owned_bytes": load["engine_peak_owned_bytes"], "engine_peak_reserved_bytes": load["engine_peak_reserved_bytes"], "engine_idle_scratch_reserve_bytes": load["engine_idle_scratch_reserve_bytes"], "input_overlap_seconds": load["input_overlap_seconds"], "engine_overlap_seconds": load["engine_prepare_commit_overlap_seconds"], "producer_credit_wait_seconds": load["producer_credit_wait_seconds"]}
validation_path.write_text(json.dumps(validation, sort_keys=True, indent=2) + "\n")
print(result_path, "rows", load["rows"], "wall_seconds", load["wall_seconds"], flush=True)
PY
  sha256sum "$cell/result.json" > "$cell/result.sha256"
  date -u > "$cell/finished.txt"
}

for scale in 1m 10m; do
  for spec in 01:0 02:1 03:1 04:0 05:0 06:1 07:1 08:0 09:0 10:1; do
    IFS=: read -r ordinal depth <<< "$spec"
    run_cell "$scale" "$ordinal" "$depth" 1
  done
done
run_cell 1m historical 0 0
run_cell 10m historical 0 0
(
  cd "$DATA_DIR"
  find . -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum
) > "$OUT/fixture-files-after.sha256"
cmp "$OUT/fixture-files.sha256" "$OUT/fixture-files-after.sha256"
[[ "$(sha256sum "$BIN" | awk '{print $1}')" == "$BIN_SHA256" ]] || { echo "binary changed during matrix" >&2; exit 2; }
{ date -u; df -h "$OUT"; uptime; } > "$OUT/host-finish.txt"
date -u > "$OUT/complete.txt"
