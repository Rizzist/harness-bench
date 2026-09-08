#!/bin/zsh
# Quiet, serial four-pillar reference. See docs/HANDOFF-macmini.md.
# Usage: scripts/reference-run.sh [--resume ABSOLUTE_RUN_DIR]
set -u
cd "$(dirname "$0")/.." || exit 2
export CARGO_INCREMENTAL=0
BIN="${AHRB_BIN:-target/debug/ahrb}"
SUPPORT="$PWD/scripts/reference-support.py"
HARNESSES=(${=AHRB_REFERENCE_HARNESSES:-claude-code opencode pi rick haider-agent codex})
if (( $# == 2 )) && [[ "$1" == --resume && "$2" == /* && -d "$2" ]]; then
  RUN="${2:A}"
  if [[ -z "${AHRB_REFERENCE_HARNESSES:-}" ]]; then
    SELECTION=$(python3 -c 'import json,sys; print(" ".join(json.load(open(sys.argv[1]))["harnesses"]))' "$RUN/provenance.json") || exit 2
    HARNESSES=(${=SELECTION})
  fi
elif (( $# == 0 )); then
  ROOT="${AHRB_OUT_ROOT:-$HOME/ahrb-results}"
  ROOT="${ROOT:a}/reference"
  STAMP=$(date -u '+%Y%m%dT%H%M%SZ') || exit 2
  printf -v ID '%04x' "$RANDOM"
  RUN="$ROOT/$STAMP-$ID"
  mkdir -p "$ROOT" && mkdir "$RUN" || { print 'REFERENCE FAIL (cannot create fresh directory)'; exit 2; }
else
  print -u2 'usage: reference-run.sh [--resume ABSOLUTE_RUN_DIR]'
  exit 2
fi
print -r -- "RUN_DIR=$RUN"
# Never steal an active or stale lock. After a crash, inspect ownership before removal.
mkdir "$RUN/.reference-lock" 2>/dev/null || { print 'REFERENCE FAIL (run locked)'; exit 2; }
print $$ > "$RUN/.reference-lock/pid"
trap 'rm -f "$RUN/.reference-lock/pid"; rmdir "$RUN/.reference-lock"' EXIT
LOG="$RUN/reference-run.log"
log() { print -r -- "$*" | tee -a "$LOG"; }
finish() { log "REFERENCE $1"; }
run_guard() {
  python3 "$SUPPORT" "$@" >> "$LOG" 2>&1
  local guard_rc=$?
  if (( guard_rc != 0 )); then
    log 'GUARD_STOP classification=FAIL; remaining scheduled steps were not run; reason in reference-run.log'
  fi
  return "$guard_rc"
}
trap 'log "RESULT interrupted: EXIT=130"; finish FAIL; exit 130' INT TERM HUP
PREFLIGHT=$(mktemp -d "$RUN/preflight-XXXXXXXX") || { finish FAIL; exit 2; }
log "START $(date -u '+%FT%TZ') profile=quick"
if [[ ! -x "$BIN" ]]; then
  log 'RESULT binary: EXIT=2 (build with cargo build --locked first)'
  finish FAIL; exit 2
fi
python3 "$SUPPORT" inventory "$PREFLIGHT" "$BIN" "${HARNESSES[@]}" >> "$LOG" 2>&1
RC=$?; log "RESULT inventory: EXIT=$RC"
if (( RC != 0 )); then finish FAIL; exit 2; fi
FAILED=0; PARTIAL=0
for h in "${HARNESSES[@]}"; do
  python3 "$SUPPORT" doctor "$PREFLIGHT" "$BIN" "$h" > "$PREFLIGHT/doctor-$h.log" 2>&1
  RC=$?; log "RESULT $h/doctor: EXIT=$RC"
  (( RC == 0 )) || FAILED=1
done
if (( FAILED )); then finish FAIL; exit 2; fi
python3 "$SUPPORT" identity "$RUN" "$PREFLIGHT" >> "$LOG" 2>&1
RC=$?; log "RESULT identity: EXIT=$RC"
ADOPT_ALLOWED=1
case $RC in 0) ;; 3) ADOPT_ALLOWED=0 ;; *) finish FAIL; exit 2 ;; esac
run_guard guard "$RUN" preflight
RC=$?; log "RESULT preflight/guard: EXIT=$RC"
if (( RC != 0 )); then finish FAIL; exit 2; fi
if [[ "${AHRB_SKIP_MOCK_CERT:-0}" == 1 ]]; then
  if [[ -z "${AHRB_SKIP_MOCK_CERT_REASON:-}" ]]; then
    log 'RESULT mock-cert: EXIT=2 (AHRB_SKIP_MOCK_CERT_REASON required)'; finish FAIL; exit 2
  fi
  log "RESULT mock-cert: SKIPPED reason=$AHRB_SKIP_MOCK_CERT_REASON"
else
  AHRB_OUT_ROOT="$PREFLIGHT" AHRB_BIN="$BIN" scripts/mock-cert.sh >> "$LOG" 2>&1
  RC=$?; log "RESULT mock-cert: EXIT=$RC"
  if (( RC != 0 )); then finish FAIL; exit 2; fi
fi
for h in "${HARNESSES[@]}"; do
  run_guard guard "$RUN" "$h"
  RC=$?; log "RESULT $h/guard: EXIT=$RC"
  if (( RC != 0 )); then FAILED=1; break; fi
  for pillar in matrix economy fidelity storage; do
    OUT="$RUN/$h/$pillar"
    ADOPT=3
    if (( ADOPT_ALLOWED )) && [[ -f "$OUT/report.json" ]]; then
      python3 "$SUPPORT" adopt "$RUN" "$h" "$pillar" >> "$LOG" 2>&1
      ADOPT=$?
    fi
    if (( ADOPT == 0 )); then
      log "RESULT $h/$pillar: SKIPPED provenance-bound report (adoption reason recorded)"
    else
      run_guard disk "$RUN" "$h:$pillar"
      RC=$?; log "RESULT $h/$pillar/disk: EXIT=$RC"
      if (( RC != 0 )); then FAILED=1; break 2; fi
      # Preserve incomplete attempts, including diagnostics and ownership receipts.
      if [[ -e "$OUT" ]]; then
        ARCHIVE=$(mktemp -d "$RUN/$h/attempt-$pillar-XXXXXXXX") || { FAILED=1; break; }
        mv "$OUT" "$ARCHIVE/output" || { FAILED=1; break; }
      fi
      # Storage requires a nonexistent output, so stage controller evidence beside it.
      mkdir -p "$RUN/$h" || { FAILED=1; break; }
      META=$(mktemp -d "$RUN/$h/step-$pillar-XXXXXXXX") || { FAILED=1; break; }
      python3 "$SUPPORT" snapshot "$META" >> "$LOG" 2>&1
      RC=$?; log "RESULT $h/$pillar/snapshot: EXIT=$RC"
      if (( RC != 0 )); then FAILED=1; continue; fi
      case $h in codex) DL=3600 ;; opencode) DL=1800 ;; *) DL=1500 ;; esac
      ARGS=(--deadline "$DL")
      # The storage runner resolves 10,509 + 3*sweep_interval_s for quick (§1).
      # Clear the generic environment override so it cannot shorten that budget.
      [[ "$pillar" == storage ]] && ARGS=()
      ENVPFX=(); [[ "$h" == haider-agent ]] && ENVPFX=(HAIDER_RUN_DAEMON_IDLE_TTL_MS=0)
      log "START $h/$pillar $(date -u '+%FT%TZ') deadline=$([[ "$pillar" == storage ]] && print storage-default || print "$DL")"
      START=$SECONDS
      env -u AHRB_DEADLINE "${ENVPFX[@]}" "$BIN" run --manifest "adapters/$h/manifest.toml" --pillar "$pillar" --profile quick "${ARGS[@]}" --output "$OUT" --junit > "$META/command.log" 2>&1
      RC=$?; log "RESULT $h/$pillar: EXIT=$RC duration_s=$(( SECONDS - START ))"
      # Keep native exit status even when the CLI exits before creating output.
      print "$RC" > "$META/exit-code.txt"
      if ! mkdir -p "$OUT" || ! mv -n "$META/"* "$OUT/" || ! rmdir "$META"; then
        log "RESULT $h/$pillar/evidence: EXIT=2 (staging evidence retained)"
        FAILED=1; continue
      fi
      if (( RC == 0 || RC == 1 )); then
        python3 "$SUPPORT" cleanup "$OUT" >> "$LOG" 2>&1
        RC=$?
      else
        # An abnormal exit cannot prove that owned children have stopped.
        log "RESULT $h/$pillar/cleanup: SKIPPED abnormal command exit; profiles retained"
        RC=0
      fi
      log "RESULT $h/$pillar/cleanup: EXIT=$RC"
      print "$RC" > "$OUT/cleanup-exit-code.txt"
      (( RC == 0 )) || FAILED=1
      python3 "$SUPPORT" seal "$RUN" "$h" "$pillar" >> "$LOG" 2>&1
      RC=$?; log "RESULT $h/$pillar/provenance: EXIT=$RC"
      (( RC == 0 )) || FAILED=1
    fi
    python3 "$SUPPORT" assess "$OUT" "$pillar" >> "$LOG" 2>&1
    RC=$?; log "RESULT $h/$pillar/assessment: EXIT=$RC"
    case $RC in 0) ;; 3) PARTIAL=1 ;; *) FAILED=1 ;; esac
  done
done
if (( FAILED )); then finish FAIL; exit 2; fi
if (( PARTIAL )); then finish PARTIAL; exit 1; fi
finish PASS
exit 0
