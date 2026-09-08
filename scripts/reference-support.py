#!/usr/bin/env python3
"""Private runner operations; Python 3.9 stdlib, no account/profile discovery."""
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time

HARNESSES = ("claude-code", "opencode", "pi", "rick", "haider-agent", "codex", "mock", "mock-exec")
CLASSES = ("PASS", "FAIL", "ERROR", "UNSUPPORTED", "ABSENT")


# Required serialized fields for the current report contracts (see the matching
# Rust summary types and docs/SPEC-v3-{economy,fidelity}.md / SPEC-v4-storage.md).
# ? permits JSON null; ~ permits omission only where Rust skips serialization.
# Arrays validate every element. Unknown schema versions cannot certify a PASS.
FIELD_TYPES = {
    'EconomySummary': {
        'uint': 'schema turn_budget model_turns total_reference_tokens tool_calls tool_results tool_result_requests last_context_size_tokens cache_control_breakpoints redundant_tokens per_turn_fixed_overhead_tokens wasted_tool_call_count retry_attempts retry_reference_tokens cache_bust_count invalidated_prefix_tokens',
        'str': 'task profile reference_token_label completion_label cost_outcome_label cache_eligible_fraction_label cache_control_breakpoints_label cache_eligibility_note redundant_tokens_label context_token_curve_label per_turn_fixed_overhead_tokens_label wasted_tool_call_count_label retry_label cache_regime cache_regime_label cache_input_discount_label effective_cost_label prefix_stability_label',
        'ReferenceTokenizerPin': 'reference_tokenizer',
        'number': 'tool_batching_factor reference_tariff_usd_per_million_tokens reference_cost_usd cache_eligible_fraction context_token_curve_slope cache_input_discount effective_reference_tokens effective_cost_usd stable_prefix_preserved_fraction',
        'EconomyCompletion': 'completion',
        'EconomyEffectsVerified': 'effects_verified',
        '?uint': 'tokens_per_completed_task',
        '[]uint': 'cache_control_breakpoints_per_request context_token_curve invalidated_prefix_tokens_per_turn',
        'bool': 'context_token_curve_last_matches_last_context_size',
    },
    'ReferenceTokenizerPin': {
        'str': 'encoding version vocabulary_sha256',
        'uint': 'vocabulary_entries',
    },
    'EconomyEffectsVerified': {
        'str': 'label workspace_receipt_before_sha256 workspace_receipt_after_sha256',
        '[]EconomyExpectedEffect': 'expected',
        '[]EconomyObservedEffect': 'observed',
        'bool': 'all_verified',
    },
    'EconomyExpectedEffect': {
        'str': 'path content_sha256 edit_call_id read_back_call_id',
    },
    'EconomyObservedEffect': {
        'str': 'path',
        '?str': 'before_content_sha256 after_content_sha256 read_back_content_sha256',
        'uint': 'edit_observations read_back_observations',
        'bool': 'edit_reported_success read_back_path_verified',
    },
    'FidelitySummary': {
        'uint': 'schema turn_budget model_turns end_turn',
        'str': 'task profile measurement_label needle_survival_fraction_label survival_curve_label retained_tool_result_fraction_label end_reason_label workspace_state_label workspace_receipt_before_sha256 workspace_receipt_after_sha256',
        '[]NeedleSurvival': 'needles',
        'number': 'needle_survival_fraction',
        '[]number': 'survival_curve retained_tool_result_fraction',
        '?uint': 'first_loss_turn declared_turn_ceiling',
        'FidelityEndReason': 'end_reason',
        'HarnessExitStatus': 'harness_exit_status',
        '?int': 'harness_exit_code',
        'bool': 'internal_cap_detected',
        'WorkspaceState': 'workspace_state',
    },
    'NeedleSurvival': {
        'str': 'id token',
        '?uint': 'planted_turn first_disappeared_turn',
        'bool': 'ever_reappeared',
    },
    'StorageSummary': {
        'uint': 'schema turn_budget repetitions completed_turns physical_requests',
        'str': 'task profile os topology comparison_scope measurement_label counter_source allocation_source declarations_sha256',
        '?number': 'write_bytes_per_turn_p50 write_bytes_per_turn_p95 write_bytes_per_turn_max logical_growth_bytes_per_turn net_growth_bytes_per_turn write_amplification_ratio fsync_calls_per_turn fdatasync_calls_per_turn fullfsync_calls_per_turn durability_calls_per_turn assumed_fsync_cost_ms estimated_durability_wall_ms_per_turn first_turn_allocated_bytes footprint_slope_bytes_per_turn compaction_before_allocated_bytes compaction_after_allocated_bytes compaction_freed_pct close_retained_bytes_per_session close_retained_after_sweep_bytes_per_session stored_unique_ratio resume_read_bytes_p50 resume_read_bytes_p95 resume_latency_p50_ms resume_latency_p95_ms',
        '?str': 'disk_class durability_class',
        '?GrowthClass': 'growth_class',
        '?uint': 'closed_sessions retention_cap_bytes delete_residue_allocated_bytes delete_residue_files uninstall_residue_allocated_bytes uninstall_residue_files stored_request_bytes unique_request_content_bytes crash_residue_allocated_bytes crash_residue_files',
        '?BoundClass': 'close_retention_class',
        '?RetentionClass': 'request_retention_class',
        '?CrashOutcome': 'crash_resume_outcome',
        '?ResumeOutcome': 'resume_outcome',
        '[]CurvePoint': 'footprint_curve',
        '[]Auxiliary': 'auxiliaries',
    },
    'CurvePoint': {
        'uint': 'turn',
        'number': 'allocated_bytes mad_bytes',
    },
    'Auxiliary': {
        'str': 'name',
        'bool': 'declared rotation_observed',
        '?uint': 'cap_bytes',
        'uint': 'peak_allocated_bytes final_allocated_bytes',
        'number': 'slope_bytes_per_turn',
        '?BoundClass': 'class',
    },
    'Fingerprint': {
        'str': 'harness harness_version manifest workflows fake_model normalizer ahrb_revision platform profile',
        'uint': 'host_memory_bytes',
    },
    'ResourceSummary': {
        'str': 'topology profile comparison_scope',
        'number': 'peak_rss_mib mean_rss_mib median_rss_mib cpu_total_s cpu_per_turn_ms wall_per_turn_ms sampler_overhead_pct',
        '~?number': 'wall_per_turn_p50_ms wall_per_turn_p95_ms wall_per_turn_max_ms wall_per_turn_mad_ms wall_per_turn_jitter_ratio time_to_first_model_request_p50_ms time_to_first_model_request_p95_ms time_to_first_model_request_max_ms memory_time_integral_mib_s_per_turn memory_time_integral_coverage_ratio memory_time_integral_max_sample_gap_ms cpu_per_turn_p50_ms cpu_per_turn_p95_ms disk_write_bytes_per_turn_p50 disk_write_bytes_per_turn_p95 disk_write_bytes_per_turn_max session_journal_growth_bytes_per_turn log_growth_bytes_per_turn disk_write_growth_slope_bytes_per_turn2 model_wait_cpu_p50_ms model_wait_wall_p50_ms model_wait_cpu_one_core_max_ratio large_tool_output_peak_rss_delta_mib latency_slope_ms_per_100_turns latency_last_first_decile_ratio session_residue_slope_mib_per_session session_residue_final_mib session_store_byte_slope_per_session session_store_file_count_slope_per_session session_store_final_residue_bytes resume_latency_p50_ms resume_latency_p95_ms resume_latency_slope_ms_per_turn fanout_max_local_rss_alpha fanout_max_local_wall_alpha fanout_global_rss_alpha fairness_latency_cv fairness_latency_max_min_ratio fairness_latency_spread_ms idle_rss_mib parallel_beta_mib_per_agent scaling_alpha',
        '~?str': 'latency_class cpu_class',
        '~?bool': 'disk_io_counter_complete unbounded_disk_growth',
        '~?uint': 'session_store_final_residue_files fanout_cliff_n_rss fanout_cliff_n_wall fanout_max_measured_n fairness_starved_agents',
    },
    'Badge': {
        'uint': 'spec_version parallel_width automation_score',
        'str': 'os topology profile resource_class latency_class cpu_class comparison_scope',
        '[]str': 'facets',
    },
}

ENUMS = {
    'EconomyCompletion': 'completed terminal-without-effect aborted stalled looped over-budget'.split(),
    'FidelityEndReason': 'reached-scripted-terminal harness-internal-ceiling ahrb-deadline crashed'.split(),
    'HarnessExitStatus': 'not-applicable running exit-code signal-or-unknown'.split(),
    'WorkspaceState': 'mutated untouched'.split(),
    'GrowthClass': 'bounded linear superlinear'.split(),
    'BoundClass': 'bounded unbounded'.split(),
    'RetentionClass': 'none deduplicated full'.split(),
    'CrashOutcome': 'preserved corrupt failed'.split(),
    'ResumeOutcome': 'preserved failed'.split(),
}


def read_json(path):
    with open(path, encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise ValueError("expected a JSON object")
    return value


def write_json(path, value):
    with open(path, "x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")


def digest(path):
    result = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def command(args, **kwargs):
    return subprocess.check_output(args, timeout=30, **kwargs).decode("utf-8", "replace").strip()


def probe_environment(home):
    env = {k: v for k, v in os.environ.items()
           if not any(s in k.upper() for s in ("KEY", "TOKEN", "SECRET", "CREDENTIAL", "AUTH"))}
    for key in ("HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", "XDG_CACHE_HOME",
                "CODEX_HOME", "CLAUDE_CONFIG_DIR", "RICK_HOME", "RICK_DATA", "HAIDER_HOME",
                "PI_CODING_AGENT_DIR"):
        env[key] = home
    return env


def doctor(out, binary, harness):
    with tempfile.TemporaryDirectory(prefix="doctor-", dir=out) as home:
        result = subprocess.run([binary, "doctor", "--manifest", "adapters/" + harness + "/manifest.toml"],
                                env=probe_environment(home), timeout=30, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, encoding="utf-8")
    print(result.stdout, end="")
    print(result.stderr, end="", file=sys.stderr)
    if result.returncode:
        return result.returncode
    report = json.loads(result.stdout)
    canonical = report.get("manifest_sha256") if isinstance(report, dict) else None
    if not isinstance(canonical, str) or not re.fullmatch('[0-9a-f]{64}', canonical) or report.get("ready") is not True:
        raise ValueError("doctor lacks a ready canonical manifest receipt")
    # Stage canonical hashes from the actual AHRB parser before identity seals
    # root provenance. Raw TOML bytes and parsed/default-expanded Manifest JSON
    # deliberately have different digests.
    provenance = read_json(out / "provenance.json")
    provenance["canonical_manifest_sha256"][harness] = canonical
    (out / "provenance.json").write_text(json.dumps(provenance, indent=2, allow_nan=False) + "\n")
    return 0


def inventory(out, binary, harnesses):
    if not harnesses or len(set(harnesses)) != len(harnesses) or any(h not in HARNESSES for h in harnesses):
        raise ValueError("invalid or duplicate reference harness selection")
    if "codex" in harnesses and harnesses[-1] != "codex":
        raise ValueError("codex must be last")
    binaries = {}
    # Probe only public version/help commands, in an empty disposable home with
    # authentication environment variables removed. Never copy account profiles.
    with tempfile.TemporaryDirectory(prefix="version-", dir=out) as home:
        env = probe_environment(home)
        for h in harnesses:
            name = {"claude-code": "claude", "haider-agent": "haider",
                    "mock": "target/debug/ahrb-mock-harness",
                    "mock-exec": "target/debug/ahrb-mock-harness"}.get(h, h)
            names = [name, "haiderd"] if h == "haider-agent" else [name]
            for executable in names:
                resolved = shutil.which(executable)
                if not resolved:
                    raise ValueError("missing binary: " + executable)
                flag = "version" if h == "rick" else "--help" if h.startswith("mock") else "--version"
                version = command([resolved, flag], env=env, stderr=subprocess.STDOUT)
                binaries[executable] = {"version": version, "sha256": digest(resolved),
                                        "probe_kind": "help" if flag == "--help" else "version"}
    revision = command(["git", "rev-parse", "HEAD"])
    diff = subprocess.check_output(["git", "diff", "HEAD", "--binary"])
    # Script hashes bind untracked implementation too, unlike git diff alone.
    scripts = {p.name: digest(p) for p in sorted(Path("scripts").glob("reference-*")) if p.is_file()}
    value = {"schema": 1, "harnesses": harnesses, "profile": "quick", "binaries": binaries,
             "ahrb_sha256": digest(binary), "ahrb_revision": revision,
             "working_diff_sha256": hashlib.sha256(diff).hexdigest(), "scripts_sha256": scripts,
             "manifest_sha256": {h: digest(Path("adapters") / h / "manifest.toml") for h in harnesses},
             "canonical_manifest_sha256": {},
             "os": platform.system() + " " + platform.release() + " " + platform.machine()}
    write_json(out / "provenance.json", value)


def identity(run, preflight):
    new = read_json(preflight / "provenance.json")
    target = run / "provenance.json"
    if target.exists():
        if read_json(target) != new:
            raise ValueError("resume provenance differs (binary, source, manifests, OS or harness selection); start a fresh run")
    else:
        write_json(target, new)
        if any(run.glob("*/*/report.json")):
            print("RESUME unbound reports: no original provenance; all existing steps will re-run")
            return 3
    return 0


def report_pillar(report):
    summaries = [p for p in ("economy", "fidelity", "storage")
                 if report.get(p + "_summary") is not None]
    if len(summaries) > 1:
        raise ValueError("conflicting pillar summaries")
    if summaries and report.get("pillar") not in (None, summaries[0]):
        raise ValueError("explicit pillar conflicts with typed summary")
    return report.get("pillar") or (summaries[0] if summaries else "matrix")


def step_identity(run, harness, pillar):
    provenance = read_json(run / "provenance.json")
    return {"schema": 1, "step": harness + "/" + pillar, "harness": harness,
            "pillar": pillar, "profile": provenance["profile"],
            "manifest_sha256": provenance["manifest_sha256"][harness],
            "report_manifest_sha256": provenance["canonical_manifest_sha256"][harness],
            "candidate_revision": provenance["ahrb_revision"],
            "provenance_sha256": digest(run / "provenance.json")}


def check_fingerprint(report, expected):
    fingerprint = report.get("fingerprint", {})
    if not isinstance(fingerprint, dict):
        raise ValueError("missing report fingerprint object")
    # These are manifest IDs, not display names or executable filenames.
    harness_id = {"mock": "ahrb-mock", "mock-exec": "ahrb-mock-exec"}.get(
        expected["harness"], expected["harness"])
    for field, value in (("harness", harness_id), ("profile", expected["profile"]),
                         ("manifest", expected["report_manifest_sha256"]),
                         ("ahrb_revision", expected["candidate_revision"])):
        if fingerprint.get(field) != value:
            raise ValueError("report fingerprint mismatch: " + field)
    if report_pillar(report) != expected["pillar"]:
        raise ValueError("report pillar mismatch")


def seal(run, harness, pillar):
    out = run / harness / pillar
    if not (out / "report.json").is_file():
        return
    receipt = step_identity(run, harness, pillar)
    if digest(Path('adapters') / harness / 'manifest.toml') != receipt['manifest_sha256']:
        raise ValueError('manifest file changed during step')
    receipt["report_sha256"] = digest(out / "report.json")
    write_json(out / "step-provenance.json", receipt)


def adopt(run, harness, pillar):
    out = run / harness / pillar
    try:
        expected = step_identity(run, harness, pillar)
        expected["report_sha256"] = digest(out / "report.json")
        receipt = read_json(out / "step-provenance.json")
        if receipt != expected:
            raise ValueError("step receipt differs from plan or report digest")
        check_fingerprint(read_json(out / "report.json"), expected)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print("RESUME " + harness + "/" + pillar + ": RE-RUN reason=" + str(error))
        return 3
    print("RESUME " + harness + "/" + pillar + ": ADOPTED same step/pillar/harness/profile/manifest/candidate; report_sha256="
          + expected["report_sha256"] + " provenance_sha256=" + expected["provenance_sha256"])
    return 0


def guard(run, harness, disk_only=False):
    threshold = float(os.environ.get("AHRB_MAX_LOAD", "2"))
    wait = int(os.environ.get("AHRB_LOAD_WAIT_SECONDS", "900"))
    poll = int(os.environ.get("AHRB_LOAD_POLL_SECONDS", "30"))
    minimum = int(os.environ.get("AHRB_MIN_FREE_MB", "40000"))
    if not math.isfinite(threshold) or threshold <= 0 or wait < 0 or poll <= 0 or minimum < 0:
        raise ValueError("invalid load/disk guard configuration")
    start = time.monotonic()
    while True:
        load = os.getloadavg()[0]
        free = shutil.disk_usage(run).free // (1024 * 1024)
        sample = {"timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                  "harness": harness, "load_1m": load, "free_mb": free,
                  "max_load": threshold, "min_free_mb": minimum}
        with open(run / "load-samples.jsonl", "a", encoding="utf-8") as stream:
            stream.write(json.dumps(sample) + "\n")
        print("GUARD " + json.dumps(sample), flush=True)
        if free < minimum:
            raise ValueError("ABORT_LOWDISK: free_mib=" + str(free) + " < min_free_mib=" + str(minimum)
                             + "; guard stop is FAIL; remaining scheduled steps were not run")
        if disk_only or load < threshold:
            return
        remaining = wait - (time.monotonic() - start)
        if remaining <= 0:
            raise ValueError("LOAD_WAIT_EXHAUSTED: guard stop is FAIL; remaining scheduled steps were not run")
        time.sleep(min(poll, remaining))


def snapshot(out):
    root = Path(os.environ.get("AHRB_TMP_ROOT", "/tmp")).resolve(strict=True)
    write_json(out / "cleanup-before.json", {"root": str(root), "started": time.time(),
               "existing": sorted(p.name for p in root.iterdir() if p.name.startswith("ahrb-"))})


def cleanup(out):
    if os.environ.get("AHRB_KEEP_PROFILES") == "1":
        print("KEEP_PROFILES")
        return
    report = out / "report.json"
    if not report.is_file():
        print("CLEANUP_SKIPPED no report")
        return
    before = read_json(out / "cleanup-before.json")
    raw = read_json(report).get("profile_path")
    if raw:
        p = Path(raw)
        safe = (p.is_absolute() and p.name.startswith("ahrb-") and not p.is_symlink()
                and p.parent.resolve() == Path(before["root"]) and p.name not in before["existing"])
        if not safe:
            raise ValueError("CLEANUP_REFUSED unsafe ownership")
        if p.exists():
            stat = p.stat()
            if not p.is_dir() or getattr(stat, "st_birthtime", stat.st_ctime) < before["started"]:
                raise ValueError("CLEANUP_REFUSED predates step")
            shutil.rmtree(p)
            print("CLEANUP_REMOVED owned temporary root")
    # Output is exclusively created by this step. Do not follow symlinks.
    for parent, dirs, _ in os.walk(out, followlinks=False):
        for name in list(dirs):
            p = Path(parent) / name
            if name.startswith("profile-") and not p.is_symlink():
                shutil.rmtree(p)
                dirs.remove(name)


def validate_type(value, kind, path, reasons):
    if kind.startswith('?'):
        if value is None:
            return
        return validate_type(value, kind[1:], path, reasons)
    if kind.startswith('[]'):
        if not isinstance(value, list):
            reasons.append(path + ': expected array')
        else:
            for i, item in enumerate(value):
                validate_type(item, kind[2:], path + '[' + str(i) + ']', reasons)
        return
    if kind in FIELD_TYPES:
        if not isinstance(value, dict):
            reasons.append(path + ': missing typed object')
            return
        for child_kind, fields in FIELD_TYPES[kind].items():
            for key in fields.split():
                if key not in value:
                    if not child_kind.startswith('~'):
                        reasons.append(path + '.' + key + ': missing field')
                else:
                    validate_type(value[key], child_kind.lstrip('~'), path + '.' + key, reasons)
        return
    if kind in ENUMS:
        valid = isinstance(value, str) and value in ENUMS[kind]
    elif kind == 'str':
        valid = isinstance(value, str) and bool(value)
    elif kind == 'bool':
        valid = isinstance(value, bool)
    elif kind in ('uint', 'int'):
        valid = type(value) is int and (-(2**31) <= value < 2**31 if kind == 'int' else 0 <= value < 2**64)
    elif kind == 'number':
        try:
            valid = type(value) in (int, float) and math.isfinite(value)
        except OverflowError:
            valid = False
    else:
        raise ValueError('unknown validator type: ' + kind)
    if not valid:
        reasons.append(path + ': invalid ' + kind)
    elif path.endswith(('sha256', '.manifest', '.workflows')) and value is not None:
        if not re.fullmatch('[0-9a-f]{64}', value):
            reasons.append(path + ': invalid SHA-256')


STORAGE_ROWS = 'write-volume durability-cost footprint-curve compaction-vs-disk close-retention delete-uninstall-residue bounded-auxiliaries request-body-retention crash-residue resume-read-cost'.split()
# Required measurements for successful observations; the conditional nulls below
# follow §4 (zero denominators, inapplicable primitives/verbs/sweep/resume).
STORAGE_REQUIRED = {
    1: 'write_bytes_per_turn_p50 write_bytes_per_turn_p95 write_bytes_per_turn_max logical_growth_bytes_per_turn net_growth_bytes_per_turn disk_class',
    2: 'durability_calls_per_turn assumed_fsync_cost_ms estimated_durability_wall_ms_per_turn durability_class',
    3: 'first_turn_allocated_bytes footprint_slope_bytes_per_turn growth_class',
    4: 'compaction_before_allocated_bytes compaction_after_allocated_bytes',
    5: 'closed_sessions close_retained_bytes_per_session retention_cap_bytes close_retention_class',
    6: '', 7: '',
    8: 'stored_request_bytes unique_request_content_bytes',
    9: 'crash_residue_allocated_bytes crash_residue_files crash_resume_outcome',
    10: 'resume_outcome',
}


def storage_completeness(report, summary, reasons):
    if summary['repetitions'] != 3:
        reasons.append('storage_summary.repetitions: expected quick repetitions=3')
    if summary['assumed_fsync_cost_ms'] != 4:
        reasons.append('storage_summary.assumed_fsync_cost_ms: expected fixed estimate=4')
    for row in report['results']:
        n = row['row']
        if row.get('id') != STORAGE_ROWS[n - 1] or row.get('pillar') != 'storage':
            reasons.append('storage row identity mismatch: S' + str(n))
        if row['outcome']['class'] not in ('PASS', 'FAIL'):
            continue
        prefix = 'S' + str(n) + ': '
        details = report.get('details')
        details = details.get(STORAGE_ROWS[n - 1], {}) if isinstance(details, dict) else {}
        trials = details.get('trials', []) if isinstance(details, dict) else []
        trials = [t for t in trials if isinstance(t, dict)] if isinstance(trials, list) else []
        def trial_value(trial, section, key):
            value = trial.get(section)
            return value.get(key) if isinstance(value, dict) else None
        for key in STORAGE_REQUIRED[n].split():
            if summary[key] is None:
                reasons.append(prefix + 'missing claimed measurement ' + key)
        if n in (1, 3, 7, 8) and summary['completed_turns'] != 300:
            reasons.append(prefix + 'shared task did not complete 300 turns')
        if n in (1, 3, 7, 8) and summary['physical_requests'] < summary['completed_turns']:
            reasons.append(prefix + 'physical request count is smaller than completed turns')
        if (n == 1 and summary['logical_growth_bytes_per_turn'] != 0 and summary['write_amplification_ratio'] is None
                and not any(trial_value(t, 'diagnostics', 'amplification_reason') == 'zero-denominator' for t in trials)):
            reasons.append(prefix + 'missing amplification with nonzero denominator')
        if n == 2 and all(summary[k] is None for k in ('fsync_calls_per_turn', 'fdatasync_calls_per_turn', 'fullfsync_calls_per_turn')):
            reasons.append(prefix + 'no applicable primitive measured')
        if n == 2:
            for primitive in ('fsync', 'fdatasync', 'fullfsync'):
                if summary[primitive + '_calls_per_turn'] is None:
                    applicability = [trial_value(t, 'diagnostics', 'primitive_applicability') for t in trials]
                    if len(applicability) != 3 or not all(isinstance(a, dict) and a.get(primitive) == 'not-applicable' for a in applicability):
                        reasons.append(prefix + 'null ' + primitive + ' lacks inapplicability receipts')
        if n == 3 and [p['turn'] for p in summary['footprint_curve']] != [0, 1, 10, 50, 100]:
            reasons.append(prefix + 'missing or duplicate footprint checkpoints')
        if (n == 4 and summary['compaction_before_allocated_bytes'] != 0 and summary['compaction_freed_pct'] is None
                and not any(trial_value(t, 'summary', 'compaction_before_allocated_bytes') == 0 for t in trials)):
            reasons.append(prefix + 'missing compaction percent with nonzero baseline')
        if n == 6 and not any(all(summary[k] is not None for k in pair) for pair in (
                ('delete_residue_allocated_bytes', 'delete_residue_files'),
                ('uninstall_residue_allocated_bytes', 'uninstall_residue_files'))):
            reasons.append(prefix + 'no complete declared operation measurement')
        if n == 5 and summary['closed_sessions'] != 60:
            reasons.append(prefix + 'quick close task did not complete 20 sessions in each of 3 repetitions')
        if n == 7 and not summary['auxiliaries']:
            reasons.append(prefix + 'missing family audit')
        if n == 7 and any(a['cap_bytes'] is None or a['class'] is None for a in summary['auxiliaries']):
            reasons.append(prefix + 'PASS family audit lacks cap/class')
        if n == 8 and summary['request_retention_class'] is None:
            classes = [trial_value(t, 'summary', 'request_retention_class') for t in trials]
            if (len(trials) != 3 or not all(t.get('measurement_complete') is True for t in trials)
                    or not all(c in ENUMS['RetentionClass'] for c in classes) or len(set(classes)) < 2):
                reasons.append(prefix + 'null retention class lacks complete differing repetition classes')
        if n == 8 and summary['unique_request_content_bytes'] != 0 and summary['stored_unique_ratio'] is None:
            reasons.append(prefix + 'missing retention ratio with nonzero denominator')
        if n == 10 and summary['resume_outcome'] == 'preserved' and any(summary[k] is None for k in (
                'resume_read_bytes_p50', 'resume_read_bytes_p95', 'resume_latency_p50_ms', 'resume_latency_p95_ms')):
            reasons.append(prefix + 'missing preserved-resume measurements')


def completeness(report, pillar):
    reasons = []
    validate_type(report.get('fingerprint'), 'Fingerprint', 'fingerprint', reasons)
    if report.get('schema') != (4 if pillar == 'storage' else 3):
        reasons.append('report.schema: unsupported or missing schema')
    if report_pillar(report) != pillar:
        reasons.append('wrong report pillar')
    rows = report.get('results')
    if not isinstance(rows, list):
        return reasons + ['results: missing array']
    for i, row in enumerate(rows):
        if not isinstance(row, dict) or not isinstance(row.get('outcome'), dict) or row['outcome'].get('class') not in CLASSES:
            return reasons + ['results[' + str(i) + ']: missing or invalid outcome']
    expected = 73 if pillar == 'matrix' else 10 if pillar == 'storage' else 0
    if any(type(row.get('row')) is not int for row in rows) or sorted(row['row'] for row in rows) != list(range(1, expected + 1)):
        reasons.append('incomplete or duplicate report rows')
    if pillar == 'matrix':
        validate_type(report.get('resource_summary'), 'ResourceSummary', 'resource_summary', reasons)
        resource = report.get('resource_summary')
        if isinstance(resource, dict) and resource.get('profile') != 'quick':
            reasons.append('resource_summary.profile: does not match quick plan')
        if 'badge' not in report:
            reasons.append('badge: missing field (null is permitted)')
        elif report['badge'] is not None:
            validate_type(report['badge'], 'Badge', 'badge', reasons)
        return reasons
    summary = report.get(pillar + '_summary')
    validate_type(summary, pillar.title() + 'Summary', pillar + '_summary', reasons)
    if reasons:
        return reasons
    schema, task, budget = {'economy': (4, 'ahrb-harness-economy-mvp-v1', 8),
                            'fidelity': (1, 'ahrb-harness-fidelity-longhorizon-v1', 24),
                            'storage': (1, 'ahrb-storage-tiny-turns-v1', 100)}[pillar]
    for key, value in (('schema', schema), ('task', task), ('profile', 'quick'), ('turn_budget', budget)):
        if summary[key] != value:
            reasons.append(pillar + '_summary.' + key + ': does not match current quick task contract')
    if pillar == 'storage':
        storage_completeness(report, summary, reasons)
    else:
        turns = summary['model_turns']
        arrays = ('context_token_curve', 'cache_control_breakpoints_per_request') if pillar == 'economy' else ('survival_curve', 'retained_tool_result_fraction')
        for key in arrays:
            if len(summary[key]) != turns:
                reasons.append(key + ': length differs from model_turns')
        if pillar == 'economy':
            if len(summary['invalidated_prefix_tokens_per_turn']) != max(0, turns - 1):
                reasons.append('invalidated_prefix_tokens_per_turn: missing transitions')
            effects = summary['effects_verified']
            if summary['completion'] == 'completed' and (not effects['all_verified'] or not effects['expected']
                    or len(effects['observed']) != len(effects['expected'])
                    or summary['tokens_per_completed_task'] != summary['total_reference_tokens']):
                reasons.append('economy completed without complete effect evidence or task token count')
            if summary['completion'] == 'completed':
                if turns < budget:
                    reasons.append('economy completed before scripted terminal turn')
                for expected, observed in zip(effects['expected'], effects['observed']):
                    if (observed['path'] != expected['path']
                            or observed['after_content_sha256'] != expected['content_sha256']
                            or observed['read_back_content_sha256'] != expected['content_sha256']
                            or observed['edit_observations'] != 1 or observed['read_back_observations'] != 1
                            or not observed['edit_reported_success'] or not observed['read_back_path_verified']):
                        reasons.append('economy completed with incomplete or conflicting observed effect')
        else:
            if summary['end_reason'] == 'reached-scripted-terminal' and turns < budget:
                reasons.append('fidelity completed before scripted terminal turn')
            if summary['end_turn'] != turns:
                reasons.append('fidelity end_turn differs from model_turns')
            if sorted(n['id'] for n in summary['needles']) != sorted(('exact-function-signature', 'absolute-fixture-path', 'applied-edit-digest', 'ordinal-marker')):
                reasons.append('fidelity needles: incomplete or duplicate planted set')
    return reasons


def assess(out, pillar):
    rc = int((out / 'exit-code.txt').read_text().strip())
    cleanup_rc = int((out / 'cleanup-exit-code.txt').read_text().strip())
    if cleanup_rc or rc not in (0, 1):
        raise ValueError('step command or cleanup failed')
    try:
        report = read_json(out / 'report.json')
        reasons = completeness(report, pillar)
        try:
            check_fingerprint(report, step_identity(out.parent.parent, out.parent.name, pillar))
        except (OSError, ValueError, KeyError, TypeError) as error:
            reasons.append(str(error))
    except (OSError, ValueError, KeyError, TypeError) as error:
        report, reasons = {}, ['incomplete report: ' + str(error)]
    summary = report.get(pillar + '_summary') or {}
    if isinstance(summary, dict) and pillar == 'fidelity' and summary.get('end_reason') == 'ahrb-deadline':
        raise ValueError('fidelity run deadline exhausted')
    rows = report.get('results')
    classes = [r['outcome'].get('class') for r in rows
               if isinstance(r, dict) and isinstance(r.get('outcome'), dict)] if isinstance(rows, list) else []
    if rc == 1 and not any(c in ('FAIL', 'ERROR') for c in classes):
        raise ValueError('nonzero command without a corresponding row finding')
    for c in CLASSES[1:]:
        if c in classes:
            reasons.append(str(classes.count(c)) + ' row(s) ' + c)
    if isinstance(summary, dict):
        if pillar == 'economy' and summary.get('completion') != 'completed':
            reasons.append('economy task not completed')
        if pillar == 'fidelity' and summary.get('end_reason') != 'reached-scripted-terminal':
            reasons.append('fidelity scripted terminal not reached')
    for reason in reasons:
        print('ASSESS_REASON ' + reason)
    print('ASSESS ' + ('PARTIAL' if reasons else 'PASS'))
    return 3 if reasons else 0


def main():
    action, *args = sys.argv[1:]
    if action == "inventory":
        inventory(Path(args[0]), args[1], args[2:])
    elif action == "identity":
        return identity(Path(args[0]), Path(args[1]))
    elif action in ("adopt", "seal"):
        return (adopt if action == "adopt" else seal)(Path(args[0]), args[1], args[2])
    elif action == "doctor":
        return doctor(Path(args[0]), args[1], args[2])
    elif action == "guard":
        guard(Path(args[0]), args[1])
    elif action == "disk":
        guard(Path(args[0]), args[1], disk_only=True)
    elif action == "snapshot":
        snapshot(Path(args[0]))
    elif action == "cleanup":
        cleanup(Path(args[0]))
    elif action == "assess":
        return assess(Path(args[0]), args[1])
    else:
        raise ValueError("unknown reference operation")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print("REFERENCE_ERROR " + str(error), file=sys.stderr)
        sys.exit(2)
