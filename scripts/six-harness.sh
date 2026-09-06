#!/bin/zsh
# Serial 73-row matrix. Run on a quiet machine; codex is the final long pole.
# usage: scripts/six-harness.sh [harness ...]
# env: AHRB_OUT_ROOT, AHRB_BIN, AHRB_PROFILE, AHRB_MIN_FREE_MB, AHRB_KEEP_PROFILES=1
# AHRB_TMP_ROOT overrides the cleanup boundary only (for private test fixtures).
set -u
cd "$(dirname "$0")/.." || exit 1
export CARGO_INCREMENTAL=0
ROOT="${AHRB_OUT_ROOT:-$HOME/ahrb-results}"
ROOT="${ROOT:a}/six-harness"
PROFILE="${AHRB_PROFILE:-quick}"; MINFREE="${AHRB_MIN_FREE_MB:-10000}"
STAMP=$(date -u '+%Y%m%dT%H%M%SZ') || exit 1
printf -v ID '%04x' "$RANDOM"
RUN="$ROOT/$STAMP-$ID"
echo "RUN_DIR=$RUN"
mkdir -p "$ROOT" && mkdir "$RUN" || { echo "RUN_DIR=$RUN (cannot create fresh directory)"; echo "SIX_HARNESS FAIL"; exit 1; }
LOG="$RUN/six-harness.log"
log() { print -r -- "$*" | tee -a "$LOG"; }
finish() { log "SIX_HARNESS_DONE $(date -u '+%FT%TZ') RUN_DIR=$RUN"; log "SIX_HARNESS $1"; }
BIN="${AHRB_BIN:-target/debug/ahrb}"
if [[ -z "${AHRB_BIN:-}" && ! -x "$BIN" ]]; then
  cargo build --locked >> "$LOG" 2>&1
  RC=$?; log "BUILD EXIT=$RC"
  if (( RC != 0 )); then finish FAIL; exit 1; fi
fi
free() { df -m "$RUN" | awk 'END {print $4}'; }
load() {
  if [[ -r /proc/loadavg ]]; then awk '{print $1}' /proc/loadavg
  else sysctl -n vm.loadavg 2>/dev/null | awk '{print $2}'; fi
}
log "START $(date -u '+%FT%TZ') disk=$(free)MB load=$(load) profile=$PROFILE"
HARNESSES=("$@"); (( ${#HARNESSES[@]} )) || HARNESSES=(claude-code opencode pi rick haider-agent codex)
FAILED=0
for h in "${HARNESSES[@]}"; do
  case $h in claude-code|opencode|pi|rick|haider-agent|codex) ;; *) log "RESULT $h: INVALID_HARNESS"; FAILED=1; continue ;; esac
  f=$(free)
  if [[ "$f" != <-> || "$MINFREE" != <-> ]]; then log "RESULT $h: DISK_CHECK_FAILED"; FAILED=1; break; fi
  if (( f < MINFREE )); then log "RESULT $h: ABORT_LOWDISK free=${f}MB"; FAILED=1; break; fi
  case $h in codex) DL=3600 ;; opencode) DL=1800 ;; *) DL=1500 ;; esac
  ENVPFX=(); [[ "$h" == haider-agent ]] && ENVPFX=(HAIDER_RUN_DAEMON_IDLE_TTL_MS=0)
  OUT="$RUN/$h"
  if ! mkdir "$OUT"; then log "RESULT $h: OUTPUT_FAILED"; FAILED=1; continue; fi
  # Inventory old roots before launch; never reclaim one even if the run touches it.
  python3 - "$OUT/cleanup-before.json" "${AHRB_TMP_ROOT:-/tmp}" >> "$LOG" 2>&1 <<'PY'
import json, pathlib, sys, time
root = pathlib.Path(sys.argv[2]).resolve(strict=True)
before = {'root': str(root), 'started': time.time(),
          'existing': sorted(p.name for p in root.iterdir() if p.name.startswith('ahrb-'))}
with open(sys.argv[1], 'x') as out:
    json.dump(before, out)
PY
  RC=$?
  if (( RC != 0 )); then log "RESULT $h: CLEANUP_SNAPSHOT_EXIT=$RC"; FAILED=1; continue; fi
  log "$h START $(date -u '+%T') load=$(load) disk=${f}MB deadline=${DL}s"
  env "${ENVPFX[@]}" "$BIN" run --manifest "adapters/$h/manifest.toml" --profile "$PROFILE" --deadline "$DL" --output "$OUT" --junit >> "$LOG" 2>&1
  RC=$?; log "RESULT $h: EXIT=$RC"
  (( RC == 0 )) || FAILED=1
  REPORT="$OUT/report.json"
  if [[ -f "$REPORT" ]]; then
    python3 - "$REPORT" "$h" >> "$LOG" 2>&1 <<'PY'
import collections, json, sys
r = json.load(open(sys.argv[1])); h = sys.argv[2]
tests = r['results']
def cls(t):
    return t['outcome']['class']
c = collections.Counter(cls(t) for t in tests)
print(f"RESULT {h}: counts={dict(c)} total={len(tests)}")
print(f"RESULT {h}: FAIL={[t['row'] for t in tests if cls(t)=='FAIL']} ERROR={[t['row'] for t in tests if cls(t)=='ERROR']}")
b = r.get('badge'); print(f"RESULT {h}: badge={'yes' if b else 'none'} {json.dumps(b)[:160] if b else ''}")
rs = r.get('resource_summary') or {}
print(f"RESULT {h}: peak_rss_mib={rs.get('peak_rss_mib')} wall_per_turn_ms={rs.get('wall_per_turn_ms')} beta={rs.get('parallel_beta_mib_per_agent')}")
PY
    RC=$?; log "RESULT $h: SUMMARY_EXIT=$RC"
    (( RC == 0 )) || FAILED=1
  else log "RESULT $h: MISSING_REPORT (aborted before write)"; FAILED=1; fi
  if [[ "${AHRB_KEEP_PROFILES:-0}" != 1 ]]; then
    # Report ownership + direct temp child + freshness. Never follow a root symlink.
    python3 - "$REPORT" "$OUT/cleanup-before.json" "$h" >> "$LOG" 2>&1 <<'PY'
import json, pathlib, shutil, sys
report, snapshot, h = sys.argv[1:]
if not pathlib.Path(report).is_file():
    print(f"RESULT {h}: CLEANUP_SKIPPED no report")
    sys.exit(0)
r = json.load(open(report)); before = json.load(open(snapshot))
raw = r.get('profile_path')
if not raw:
    print(f"RESULT {h}: CLEANUP_SKIPPED no profile_path")
    sys.exit(0)
p = pathlib.Path(raw)
safe = (p.is_absolute() and p.name.startswith('ahrb-')
        and p.parent.resolve() == pathlib.Path(before['root'])
        and p.name not in before['existing'] and not p.is_symlink())
if not safe:
    print(f"RESULT {h}: CLEANUP_REFUSED {p}")
    sys.exit(1)
if p.exists():
    stat = p.stat()
    if not p.is_dir() or getattr(stat, 'st_birthtime', stat.st_ctime) < before['started']:
        print(f"RESULT {h}: CLEANUP_REFUSED predates run or not directory: {p}")
        sys.exit(1)
    shutil.rmtree(p)
    print(f"RESULT {h}: CLEANUP_REMOVED {p}")
PY
    RC=$?; log "RESULT $h: CLEANUP_EXIT=$RC"
    (( RC == 0 )) || FAILED=1
    find "$OUT" -type d -name 'profile-*' -prune -exec rm -rf -- {} + >> "$LOG" 2>&1
    RC=$?; (( RC == 0 )) || { log "RESULT $h: OUTPUT_CLEANUP_EXIT=$RC"; FAILED=1; }
  else log "RESULT $h: KEEP_PROFILES"; fi
  log "$h DONE $(date -u '+%T') disk=$(free)MB"
done
grep '^RESULT' "$LOG"
if (( FAILED )); then finish FAIL; exit 1; fi
finish PASS
exit 0
