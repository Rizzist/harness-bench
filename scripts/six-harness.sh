#!/bin/zsh
# AHRB — serial six-harness run of the 73-row matrix (functional + resource), quick profile.
# Run on a QUIET machine (load < 2, nothing else running). Always absolute output paths.
# usage: scripts/six-harness.sh [harness ...]   (default order below; codex LAST — the slow per-invocation long pole)
# env:   AHRB_OUT_ROOT (default ~/ahrb-results)   AHRB_PROFILE (default quick)   AHRB_MIN_FREE_MB (default 10000)
set -u
cd "$(dirname "$0")/.."
export CARGO_INCREMENTAL=0
ROOT="${AHRB_OUT_ROOT:-$HOME/ahrb-results}"; PROFILE="${AHRB_PROFILE:-quick}"; MINFREE="${AHRB_MIN_FREE_MB:-10000}"
mkdir -p "$ROOT"; LOG="$ROOT/six-harness.log"; : > "$LOG"
[ -x target/debug/ahrb ] || cargo build --locked >> "$LOG" 2>&1 || { echo "BUILD FAILED (see $LOG)"; exit 1; }
free() { df -m "$HOME" | tail -1 | awk '{print $4}'; }
load() { sysctl -n vm.loadavg 2>/dev/null | awk '{print $2}' || cat /proc/loadavg | awk '{print $1}'; }
echo "START $(date '+%F %T') disk=$(free)MB load=$(load) profile=$PROFILE" >> "$LOG"
HARNESSES=("$@"); [ ${#HARNESSES[@]} -eq 0 ] && HARNESSES=(claude-code opencode pi rick haider-agent codex)
for h in "${HARNESSES[@]}"; do
  f=$(free); if [ "$f" -lt "$MINFREE" ]; then echo "ABORT_LOWDISK before $h free=${f}MB" >> "$LOG"; break; fi
  # per-harness deadlines: per-invocation harnesses pay a fresh process per turn — codex is the slowest
  case $h in codex) DL=3600 ;; opencode) DL=1800 ;; *) DL=1500 ;; esac
  # haider: TTL=0 gives one-shot whole-tree resource accounting (default TTL lingers a daemon → client-only numbers)
  ENVPFX=""; [ "$h" = "haider-agent" ] && ENVPFX="HAIDER_RUN_DAEMON_IDLE_TTL_MS=0"
  OUT="$ROOT/matrix/$h"; rm -rf "$OUT"; mkdir -p "$OUT"
  echo "=== $h START $(date '+%T') load=$(load) disk=${f}MB deadline=${DL}s ===" >> "$LOG"
  env $ENVPFX ./target/debug/ahrb run --manifest "adapters/$h/manifest.toml" --profile "$PROFILE" --deadline "$DL" --output "$OUT" --junit >> "$LOG" 2>&1
  echo "$h EXIT=$?" >> "$LOG"
  REPORT=$(find "$OUT" -name report.json -type f 2>/dev/null | head -1)
  if [ -n "$REPORT" ]; then
    python3 - "$REPORT" "$h" >> "$LOG" 2>&1 <<'PY'
import json,sys,collections
r=json.load(open(sys.argv[1])); h=sys.argv[2]
tests=r.get('tests') or r.get('results') or []
def cls(t):
    o=t.get('outcome') or t.get('status') or {}
    return o.get('class') if isinstance(o,dict) else o
c=collections.Counter(cls(t) for t in tests)
print(f"RESULT {h}: counts={dict(c)} total={len(tests)}")
print(f"RESULT {h}: FAIL={[t.get('row') or t.get('id') for t in tests if cls(t)=='FAIL']} ERROR={[t.get('row') or t.get('id') for t in tests if cls(t)=='ERROR']}")
b=r.get('badge'); print(f"RESULT {h}: badge={'yes' if b else 'none'} {json.dumps(b)[:160] if b else ''}")
rs=r.get('resource_summary') or {}
g=lambda k:(rs.get(k) or {}).get('value') if isinstance(rs.get(k),dict) else rs.get(k)
print(f"RESULT {h}: peak_rss_mib={g('peak_rss_mib')} wall_per_turn_ms={g('wall_per_turn_ms')} beta={g('parallel_beta_mib_per_agent')}")
PY
  else echo "RESULT $h: no report.json (aborted before write)" >> "$LOG"; fi
  # reclaim this harness's bulky profiles + the ahrb-<id> temp it created (only AFTER its run is over)
  find "$OUT" -type d -name 'profile-*' -exec rm -rf {} + 2>/dev/null
  find /private/tmp /tmp -maxdepth 1 -type d -name 'ahrb-*' 2>/dev/null | xargs rm -rf 2>/dev/null
  echo "=== $h DONE $(date '+%T') disk=$(free)MB ===" >> "$LOG"
done
echo "SIX_HARNESS_DONE $(date '+%F %T') disk=$(free)MB" >> "$LOG"; grep '^RESULT' "$LOG"
