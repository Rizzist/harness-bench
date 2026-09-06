#!/bin/zsh
# AHRB self-certification on the built-in mock harnesses — run BEFORE any real harness on a new machine.
# daemon mock: expect all rows PASS except the spec-sanctioned UNSUP (row 68); mock-exec: row 68 PASS, zero FAIL/ERROR, badge emitted.
set -u
cd "$(dirname "$0")/.."
export CARGO_INCREMENTAL=0
ROOT="${AHRB_OUT_ROOT:-$HOME/ahrb-results}"; mkdir -p "$ROOT"
[ -x target/debug/ahrb ] || cargo build --locked || exit 1
for m in mock mock-exec; do
  OUT="$ROOT/cert/$m"; rm -rf "$OUT"; mkdir -p "$OUT"
  ./target/debug/ahrb run --manifest "adapters/$m/manifest.toml" --profile quick --output "$OUT" > "$ROOT/cert/$m.log" 2>&1
  echo "$m EXIT=$?"
  REPORT=$(find "$OUT" -name report.json -type f | head -1)
  python3 - "$REPORT" "$m" <<'PY'
import json,sys,collections
r=json.load(open(sys.argv[1])); m=sys.argv[2]
tests=r.get('tests') or r.get('results') or []
def cls(t):
    o=t.get('outcome') or t.get('status') or {}
    return o.get('class') if isinstance(o,dict) else o
print(m, dict(collections.Counter(cls(t) for t in tests)), 'total', len(tests))
print(m, 'non-PASS:', [(t.get('row') or t.get('id'), t.get('name'), cls(t)) for t in tests if cls(t)!='PASS'])
print(m, 'badge:', 'yes' if r.get('badge') else 'NONE')
PY
done
