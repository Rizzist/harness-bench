#!/bin/zsh
# Self-certify both built-in mock transports before running real harnesses.
# env: AHRB_OUT_ROOT (default ~/ahrb-results), AHRB_BIN (default target/debug/ahrb)
set -u
cd "$(dirname "$0")/.." || exit 1
export CARGO_INCREMENTAL=0
ROOT="${AHRB_OUT_ROOT:-$HOME/ahrb-results}"
ROOT="${ROOT:a}/mock-cert"
STAMP=$(date -u '+%Y%m%dT%H%M%SZ') || exit 1
printf -v ID '%04x' "$RANDOM"
RUN="$ROOT/$STAMP-$ID"
echo "RUN_DIR=$RUN"
mkdir -p "$ROOT" && mkdir "$RUN" || { echo "RUN_DIR=$RUN (cannot create fresh directory)"; echo "MOCK_CERT FAIL"; exit 1; }
LOG="$RUN/mock-cert.log"
log() { print -r -- "$*" | tee -a "$LOG"; }
finish() { log "RUN_DIR=$RUN"; log "MOCK_CERT $1"; }
BIN="${AHRB_BIN:-target/debug/ahrb}"
if [[ -z "${AHRB_BIN:-}" && ! -x "$BIN" ]]; then
  cargo build --locked >> "$LOG" 2>&1
  RC=$?; log "BUILD EXIT=$RC"
  if (( RC != 0 )); then finish FAIL; exit 1; fi
fi
FAILED=0
for m in mock mock-exec; do
  OUT="$RUN/$m"
  if ! mkdir "$OUT"; then log "$m OUTPUT_FAILED"; FAILED=1; continue; fi
  "$BIN" run --manifest "adapters/$m/manifest.toml" --profile quick --output "$OUT" >> "$LOG" 2>&1
  RC=$?; log "$m EXIT=$RC"
  (( RC == 0 )) || FAILED=1
  REPORT="$OUT/report.json"
  if [[ ! -f "$REPORT" ]]; then log "$m MISSING_REPORT"; FAILED=1; continue; fi
  python3 - "$REPORT" "$m" >> "$LOG" 2>&1 <<'PY'
import collections, json, sys
r = json.load(open(sys.argv[1])); m = sys.argv[2]
tests = r['results']
classes = [t['outcome']['class'] for t in tests]
rows = [t['row'] for t in tests]
print(m, dict(collections.Counter(classes)), 'total', len(tests))
print(m, 'non-PASS:', [(t['row'], t['id'], t['outcome']['class']) for t in tests if t['outcome']['class'] != 'PASS'])
print(m, 'badge:', 'yes' if r.get('badge') else 'NONE')
complete = sorted(rows) == list(range(1, 74))
if m == 'mock':
    expected = all(t['outcome']['class'] == ('UNSUPPORTED' if t['row'] == 68 else 'PASS') for t in tests)
else:
    expected = (all(c in ('PASS', 'UNSUPPORTED') for c in classes)
                and any(t['row'] == 68 and t['outcome']['class'] == 'PASS' for t in tests)
                and bool(r.get('badge')))
ok = complete and expected
print(m, 'EXPECTATION=' + ('PASS' if ok else 'FAIL'))
sys.exit(0 if ok else 1)
PY
  RC=$?; log "$m SUMMARY_EXIT=$RC"
  (( RC == 0 )) || FAILED=1
done
grep -E '^(mock|mock-exec) (\{|non-PASS:|badge:|EXPECTATION=)' "$LOG"
if (( FAILED )); then finish FAIL; exit 1; fi
finish PASS
exit 0
