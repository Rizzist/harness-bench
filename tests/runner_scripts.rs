#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static FIXTURE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    root: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let sequence = FIXTURE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "runner-scripts-{}-{nonce}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&root).unwrap();
        let root = fs::canonicalize(root).unwrap();
        fs::create_dir(root.join("tmp")).unwrap();
        // A private Python startup fixture avoids a load-guard race on idle CI hosts.
        fs::create_dir(root.join("python")).unwrap();
        fs::write(
            root.join("python/sitecustomize.py"),
            "import os\nos.getloadavg = lambda: (1.0, 1.0, 1.0)\n",
        )
        .unwrap();
        fs::create_dir_all(root.join("out/mock-cert/earlier")).unwrap();
        fs::create_dir_all(root.join("out/six-harness/earlier")).unwrap();
        fs::write(root.join("out/mock-cert/earlier/evidence"), "keep").unwrap();
        fs::write(root.join("out/six-harness/earlier/evidence"), "keep").unwrap();
        fs::create_dir(root.join("tmp/ahrb-unrelated")).unwrap();
        fs::write(root.join("tmp/ahrb-unrelated/evidence"), "keep").unwrap();
        let stub = root.join("ahrb-stub");
        fs::write(&stub, STUB).unwrap();
        fs::set_permissions(stub, fs::Permissions::from_mode(0o755)).unwrap();
        Self { root }
    }

    fn run(&self, script: &str, mode: &str, extra: &[(&str, &str)]) -> Output {
        self.run_args(script, mode, extra, &[])
    }

    fn run_args(&self, script: &str, mode: &str, extra: &[(&str, &str)], args: &[&str]) -> Output {
        let mut command = Command::new("zsh");
        if extra.iter().any(|(key, _)| *key == "STUB_SEED") {
            command.args([
                "-c",
                "RUNNER_SCRIPT=\"$1\"; shift; RANDOM=\"$STUB_SEED\"; source \"$RUNNER_SCRIPT\"",
                "runner",
            ]);
        }
        command
            .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("scripts/{script}.sh")))
            .args(args)
            .env("AHRB_OUT_ROOT", self.root.join("out"))
            .env("AHRB_TMP_ROOT", self.root.join("tmp"))
            .env("AHRB_BIN", self.root.join("ahrb-stub"))
            .env("PYTHONPATH", self.root.join("python"))
            .env("AHRB_MIN_FREE_MB", "0")
            .env("AHRB_KEEP_PROFILES", "0")
            .env("STUB_MODE", mode)
            .env("AHRB_REFERENCE_HARNESSES", "mock mock-exec")
            .env("AHRB_MAX_LOAD", "100000")
            .env("AHRB_SKIP_MOCK_CERT", "1")
            .env(
                "AHRB_SKIP_MOCK_CERT_REASON",
                "stub orchestration regression",
            )
            .env_remove("HAIDER_RUN_DAEMON_IDLE_TTL_MS");
        for (key, value) in extra {
            command.env(key, value);
        }
        command
            .output()
            .expect("execute zsh runner with private stub")
    }

    fn run_dir(&self, output: &Output) -> PathBuf {
        let text = String::from_utf8_lossy(&output.stdout);
        let path = text
            .lines()
            .find_map(|line| line.strip_prefix("RUN_DIR="))
            .unwrap();
        let path = PathBuf::from(path);
        assert!(path.is_absolute());
        assert!(path.starts_with(self.root.join("out")));
        path
    }

    fn log(&self, script: &str, output: &Output) -> String {
        fs::read_to_string(self.run_dir(output).join(format!("{script}.log"))).unwrap()
    }

    fn assert_preserved(&self) {
        for file in [
            "tmp/ahrb-unrelated/evidence",
            "out/mock-cert/earlier/evidence",
            "out/six-harness/earlier/evidence",
        ] {
            assert_eq!(fs::read_to_string(self.root.join(file)).unwrap(), "keep");
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

const STUB: &str = r##"#!/usr/bin/env python3
import hashlib, json, os, pathlib, subprocess, sys
args = sys.argv
mode = os.environ['STUB_MODE']
if 'doctor' in args:
    manifest = pathlib.Path(args[args.index('--manifest')+1])
    print(json.dumps({'ready': mode != 'doctor', 'manifest_sha256': hashlib.sha256(b'canonical-fixture:' + manifest.read_bytes()).hexdigest()}))
    sys.exit(9 if mode == 'doctor' else 0)
out = pathlib.Path(args[args.index('--output') + 1])
h = pathlib.Path(args[args.index('--manifest') + 1]).parent.name
pillar = args[args.index('--pillar') + 1] if '--pillar' in args else 'matrix'
# Match the real storage CLI: even an empty existing output is invalid.
if pillar == 'storage' and out.exists():
    print('storage requires a fresh output', file=sys.stderr)
    sys.exit(2)
out.mkdir(parents=True, exist_ok=True)
with open(out.parent / 'calls.jsonl', 'a') as calls:
    calls.write(json.dumps({'h': h, 'args': args[1:], 'ttl': os.environ.get('HAIDER_RUN_DAEMON_IDLE_TTL_MS'), 'deadline_env': os.environ.get('AHRB_DEADLINE')}) + '\n')
if mode == 'missing':
    sys.exit(0)
if mode == 'malformed':
    (out / 'report.json').write_text('{')
    sys.exit(0)
profile = pathlib.Path(os.environ['AHRB_TMP_ROOT']) / ('ahrb-' + out.parent.name + '-' + h + '-' + pillar)
if mode in ('old', 'outside'):
    profile = pathlib.Path(os.environ['STUB_PROFILE'])
    os.utime(profile, None)
elif mode == 'symlink':
    profile.symlink_to(os.environ['STUB_PROFILE'], target_is_directory=True)
else:
    profile.mkdir()
(out / 'profile-local').mkdir()
tests = [{'row': n, 'id': str(n), 'outcome': {'class': 'UNSUPPORTED' if h == 'mock' and n == 68 else 'PASS'}} for n in range(1, 74)]
if mode == 'classification':
    tests[0]['outcome']['class'] = 'FAIL'
if mode == 'row68':
    tests[67]['outcome']['class'] = 'UNSUPPORTED'
if mode == 'incomplete':
    tests.pop()
if mode == 'duplicate':
    tests[-1] = tests[0]
fixtures = json.loads(r'''{
  "economy": {
    "schema": 4,
    "turn_budget": 8,
    "model_turns": 8,
    "total_reference_tokens": 0,
    "tool_calls": 0,
    "tool_results": 0,
    "tool_result_requests": 0,
    "last_context_size_tokens": 0,
    "cache_control_breakpoints": 0,
    "redundant_tokens": 0,
    "per_turn_fixed_overhead_tokens": 0,
    "wasted_tool_call_count": 0,
    "retry_attempts": 0,
    "retry_reference_tokens": 0,
    "cache_bust_count": 0,
    "invalidated_prefix_tokens": 0,
    "task": "ahrb-harness-economy-mvp-v1",
    "profile": "quick",
    "reference_token_label": "synthetic-fixture",
    "completion_label": "synthetic-fixture",
    "cost_outcome_label": "synthetic-fixture",
    "cache_eligible_fraction_label": "synthetic-fixture",
    "cache_control_breakpoints_label": "synthetic-fixture",
    "cache_eligibility_note": "synthetic-fixture",
    "redundant_tokens_label": "synthetic-fixture",
    "context_token_curve_label": "synthetic-fixture",
    "per_turn_fixed_overhead_tokens_label": "synthetic-fixture",
    "wasted_tool_call_count_label": "synthetic-fixture",
    "retry_label": "synthetic-fixture",
    "cache_regime": "synthetic-fixture",
    "cache_regime_label": "synthetic-fixture",
    "cache_input_discount_label": "synthetic-fixture",
    "effective_cost_label": "synthetic-fixture",
    "prefix_stability_label": "synthetic-fixture",
    "reference_tokenizer": {
      "encoding": "synthetic-fixture",
      "version": "synthetic-fixture",
      "vocabulary_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
      "vocabulary_entries": 0
    },
    "tool_batching_factor": 0.0,
    "reference_tariff_usd_per_million_tokens": 0.0,
    "reference_cost_usd": 0.0,
    "cache_eligible_fraction": 0.0,
    "context_token_curve_slope": 0.0,
    "cache_input_discount": 0.0,
    "effective_reference_tokens": 0.0,
    "effective_cost_usd": 0.0,
    "stable_prefix_preserved_fraction": 0.0,
    "completion": "completed",
    "effects_verified": {
      "label": "synthetic-fixture",
      "workspace_receipt_before_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
      "workspace_receipt_after_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
      "expected": [
        {
          "path": "synthetic-fixture",
          "content_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
          "edit_call_id": "synthetic-fixture",
          "read_back_call_id": "synthetic-fixture"
        }
      ],
      "observed": [
        {
          "path": "synthetic-fixture",
          "before_content_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
          "after_content_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
          "read_back_content_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
          "edit_observations": 1,
          "read_back_observations": 1,
          "edit_reported_success": true,
          "read_back_path_verified": true
        }
      ],
      "all_verified": true
    },
    "tokens_per_completed_task": 0,
    "cache_control_breakpoints_per_request": [
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0
    ],
    "context_token_curve": [
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0
    ],
    "invalidated_prefix_tokens_per_turn": [
      0,
      0,
      0,
      0,
      0,
      0,
      0
    ],
    "context_token_curve_last_matches_last_context_size": false
  },
  "fidelity": {
    "schema": 1,
    "turn_budget": 24,
    "model_turns": 24,
    "end_turn": 24,
    "task": "ahrb-harness-fidelity-longhorizon-v1",
    "profile": "quick",
    "measurement_label": "synthetic-fixture",
    "needle_survival_fraction_label": "synthetic-fixture",
    "survival_curve_label": "synthetic-fixture",
    "retained_tool_result_fraction_label": "synthetic-fixture",
    "end_reason_label": "synthetic-fixture",
    "workspace_state_label": "synthetic-fixture",
    "workspace_receipt_before_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
    "workspace_receipt_after_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
    "needles": [
      {
        "id": "exact-function-signature",
        "token": "synthetic-fixture",
        "planted_turn": 3,
        "first_disappeared_turn": null,
        "ever_reappeared": false
      },
      {
        "id": "absolute-fixture-path",
        "token": "synthetic-fixture",
        "planted_turn": 3,
        "first_disappeared_turn": null,
        "ever_reappeared": false
      },
      {
        "id": "applied-edit-digest",
        "token": "synthetic-fixture",
        "planted_turn": 3,
        "first_disappeared_turn": null,
        "ever_reappeared": false
      },
      {
        "id": "ordinal-marker",
        "token": "synthetic-fixture",
        "planted_turn": 3,
        "first_disappeared_turn": null,
        "ever_reappeared": false
      }
    ],
    "needle_survival_fraction": 0.0,
    "survival_curve": [
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0
    ],
    "retained_tool_result_fraction": [
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0,
      1.0
    ],
    "first_loss_turn": null,
    "declared_turn_ceiling": null,
    "end_reason": "reached-scripted-terminal",
    "harness_exit_status": "not-applicable",
    "harness_exit_code": null,
    "internal_cap_detected": false,
    "workspace_state": "mutated"
  },
  "storage": {
    "schema": 1,
    "turn_budget": 100,
    "repetitions": 3,
    "completed_turns": 300,
    "physical_requests": 330,
    "task": "ahrb-storage-tiny-turns-v1",
    "profile": "quick",
    "os": "synthetic-fixture",
    "topology": "synthetic-fixture",
    "comparison_scope": "synthetic-fixture",
    "measurement_label": "synthetic-fixture",
    "counter_source": "synthetic-fixture",
    "allocation_source": "synthetic-fixture",
    "declarations_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
    "write_bytes_per_turn_p50": 0.0,
    "write_bytes_per_turn_p95": 0.0,
    "write_bytes_per_turn_max": 0.0,
    "logical_growth_bytes_per_turn": 0.0,
    "net_growth_bytes_per_turn": 0.0,
    "write_amplification_ratio": null,
    "fsync_calls_per_turn": 0.0,
    "fdatasync_calls_per_turn": 0.0,
    "fullfsync_calls_per_turn": 0.0,
    "durability_calls_per_turn": 0.0,
    "assumed_fsync_cost_ms": 4.0,
    "estimated_durability_wall_ms_per_turn": 0.0,
    "first_turn_allocated_bytes": 0.0,
    "footprint_slope_bytes_per_turn": 0.0,
    "compaction_before_allocated_bytes": 0.0,
    "compaction_after_allocated_bytes": 0.0,
    "compaction_freed_pct": null,
    "close_retained_bytes_per_session": 0.0,
    "close_retained_after_sweep_bytes_per_session": null,
    "stored_unique_ratio": null,
    "resume_read_bytes_p50": null,
    "resume_read_bytes_p95": null,
    "resume_latency_p50_ms": null,
    "resume_latency_p95_ms": null,
    "disk_class": "64",
    "durability_class": "0",
    "growth_class": "bounded",
    "closed_sessions": 60,
    "retention_cap_bytes": 0,
    "delete_residue_allocated_bytes": 0,
    "delete_residue_files": 0,
    "uninstall_residue_allocated_bytes": null,
    "uninstall_residue_files": null,
    "stored_request_bytes": 0,
    "unique_request_content_bytes": 0,
    "crash_residue_allocated_bytes": 0,
    "crash_residue_files": 0,
    "close_retention_class": "bounded",
    "request_retention_class": "none",
    "crash_resume_outcome": "preserved",
    "resume_outcome": "failed",
    "footprint_curve": [
      {
        "turn": 0,
        "allocated_bytes": 0.0,
        "mad_bytes": 0.0
      },
      {
        "turn": 1,
        "allocated_bytes": 0.0,
        "mad_bytes": 0.0
      },
      {
        "turn": 10,
        "allocated_bytes": 0.0,
        "mad_bytes": 0.0
      },
      {
        "turn": 50,
        "allocated_bytes": 0.0,
        "mad_bytes": 0.0
      },
      {
        "turn": 100,
        "allocated_bytes": 0.0,
        "mad_bytes": 0.0
      }
    ],
    "auxiliaries": [
      {
        "name": "other",
        "declared": true,
        "rotation_observed": false,
        "cap_bytes": 0,
        "peak_allocated_bytes": 0,
        "final_allocated_bytes": 0,
        "slope_bytes_per_turn": 0.0,
        "class": "bounded"
      }
    ]
  },
  "resource": {
    "topology": "client-process-fanout",
    "profile": "quick",
    "comparison_scope": "within-topology-only",
    "peak_rss_mib": 0.0,
    "mean_rss_mib": 0.0,
    "median_rss_mib": 0.0,
    "cpu_total_s": 0.0,
    "cpu_per_turn_ms": 0.0,
    "wall_per_turn_ms": 0.0,
    "sampler_overhead_pct": 0.0
  },
  "fingerprint": {
    "harness": "synthetic-fixture",
    "harness_version": "synthetic-fixture",
    "manifest": "0000000000000000000000000000000000000000000000000000000000000000",
    "workflows": "0000000000000000000000000000000000000000000000000000000000000000",
    "fake_model": "synthetic-fixture",
    "normalizer": "synthetic-fixture",
    "ahrb_revision": "synthetic-fixture",
    "platform": "synthetic-fixture",
    "profile": "quick",
    "host_memory_bytes": 0
  },
  "badge": {
    "spec_version": 2,
    "os": "macos",
    "topology": "client-process-fanout",
    "profile": "quick",
    "parallel_width": 8,
    "resource_class": "R32",
    "latency_class": "L250",
    "cpu_class": "C10",
    "automation_score": 100,
    "facets": [
      "replay",
      "resume"
    ],
    "comparison_scope": "within-topology-only"
  }
}''')
r = {'results': tests, 'badge': None if mode == 'badge' else fixtures['badge'], 'profile_path': str(profile)}
if '--pillar' in args:
    r.update(pillar=pillar, schema=4 if pillar == 'storage' else 3,
             fingerprint=fixtures['fingerprint'], resource_summary=fixtures['resource'])
    r['fingerprint'].update(harness={'mock':'ahrb-mock','mock-exec':'ahrb-mock-exec'}.get(h,h),
        # Like the real CLI's canonical hash, this synthetic fingerprint is
        # deliberately different from the raw manifest-file SHA-256 in the plan.
        manifest=hashlib.sha256(b'canonical-fixture:' + pathlib.Path(args[args.index('--manifest')+1]).read_bytes()).hexdigest(),
        ahrb_revision=subprocess.check_output(['git','rev-parse','HEAD']).decode().strip())
    if pillar == 'storage':
        ids = 'write-volume durability-cost footprint-curve compaction-vs-disk close-retention delete-uninstall-residue bounded-auxiliaries request-body-retention crash-residue resume-read-cost'.split()
        r['results'] = [dict(t, id=ids[i], pillar='storage') for i,t in enumerate(tests[:10])]
    elif pillar != 'matrix':
        r['results'] = []
    if pillar != 'matrix':
        r['badge'] = None
        r[pillar + '_summary'] = fixtures[pillar]
    if mode == 'fidelity-deadline' and pillar == 'fidelity':
        r['fidelity_summary']['end_reason'] = 'ahrb-deadline'
    if mode == 'empty-summary' and pillar != 'matrix':
        r[pillar + '_summary'] = {}
    if mode == 'missing-measurement' and pillar == 'storage':
        r['storage_summary']['write_bytes_per_turn_p50'] = None
    if mode == 'truncated-curve' and pillar == 'economy':
        r['economy_summary']['context_token_curve'].pop()
    if mode == 'missing-tokenizer' and pillar == 'economy':
        r['economy_summary']['reference_tokenizer'] = {}
    if mode == 'missing-needle' and pillar == 'fidelity':
        r['fidelity_summary']['needles'].pop()
    if mode == 'invalid-schema' and pillar == 'storage':
        r['storage_summary']['schema'] = 99
    if mode == 'nullable-evidence' and pillar == 'storage':
        r['storage_summary']['fullfsync_calls_per_turn'] = None
        r['storage_summary']['request_retention_class'] = None
        r['details'] = {
            'durability-cost': {'trials': [{'diagnostics': {'primitive_applicability': {'fullfsync': 'not-applicable'}}} for _ in range(3)]},
            'request-body-retention': {'trials': [{'measurement_complete': True, 'summary': {'request_retention_class': c}} for c in ('none','deduplicated','full')]},
        }
(out / 'report.json').write_text(json.dumps(r))
sys.exit(7 if mode == 'exit' else 0)
"##;

#[test]
fn failures_are_recorded_and_do_not_hide_later_runs() {
    for script in ["mock-cert", "six-harness"] {
        for (mode, recorded) in [
            ("exit", "EXIT=7"),
            ("missing", "MISSING_REPORT"),
            ("malformed", "SUMMARY_EXIT=1"),
        ] {
            let f = Fixture::new();
            let output = f.run(script, mode, &[]);
            let log = f.log(script, &output);
            assert!(!output.status.success(), "{script} {mode}: {log}");
            assert!(log.contains(recorded), "{script} {mode}: {log}");
            assert!(log.contains(if script == "mock-cert" {
                "MOCK_CERT FAIL"
            } else {
                "SIX_HARNESS FAIL"
            }));
            assert!(log.contains(if script == "mock-cert" {
                "mock-exec EXIT="
            } else {
                "RESULT codex: EXIT="
            }));
            f.assert_preserved();
        }
    }
}

#[test]
fn mock_expectations_require_complete_rows_correct_classes_and_exec_badge() {
    for mode in [
        "classification",
        "row68",
        "incomplete",
        "duplicate",
        "badge",
    ] {
        let f = Fixture::new();
        let output = f.run("mock-cert", mode, &[]);
        let log = f.log("mock-cert", &output);
        assert!(!output.status.success(), "{mode}: {log}");
        assert!(log.contains("EXPECTATION=FAIL"), "{mode}: {log}");
        assert!(log.ends_with("MOCK_CERT FAIL\n"));
    }
}

#[test]
fn fresh_runs_preserve_evidence_and_reclaim_only_owned_profiles() {
    for script in ["mock-cert", "six-harness"] {
        let f = Fixture::new();
        let first = f.run(script, "pass", &[("STUB_SEED", "1234")]);
        let log = f.log(script, &first);
        assert!(first.status.success(), "{log}\n{:?}", first);
        let first_dir = f.run_dir(&first);
        let second = f.run(script, "pass", &[("STUB_SEED", "4321")]);
        assert!(second.status.success(), "{}", f.log(script, &second));
        assert_ne!(first_dir, f.run_dir(&second));
        assert_eq!(
            fs::read_to_string(first_dir.join(format!("{script}.log"))).unwrap(),
            log
        );
        f.assert_preserved();
        if script == "six-harness" {
            let calls = fs::read_to_string(first_dir.join("calls.jsonl")).unwrap();
            let calls: Vec<serde_json::Value> = calls
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            let names: Vec<_> = calls.iter().map(|c| c["h"].as_str().unwrap()).collect();
            assert_eq!(
                names,
                [
                    "claude-code",
                    "opencode",
                    "pi",
                    "rick",
                    "haider-agent",
                    "codex"
                ]
            );
            for call in calls {
                let h = call["h"].as_str().unwrap();
                let args = call["args"].as_array().unwrap();
                let deadline = args.iter().position(|a| a == "--deadline").unwrap();
                assert_eq!(
                    args[deadline + 1],
                    match h {
                        "codex" => "3600",
                        "opencode" => "1800",
                        _ => "1500",
                    }
                );
                assert!(args.iter().any(|a| a == "--junit"));
                assert_eq!(
                    call["ttl"].as_str(),
                    if h == "haider-agent" { Some("0") } else { None }
                );
                let report: serde_json::Value = serde_json::from_slice(
                    &fs::read(first_dir.join(h).join("report.json")).unwrap(),
                )
                .unwrap();
                assert!(!PathBuf::from(report["profile_path"].as_str().unwrap()).exists());
                assert!(!first_dir.join(h).join("profile-local").exists());
            }
            assert!(log.contains("CLEANUP_REMOVED"));
        }
    }
}

#[test]
fn cleanup_refuses_old_symlink_and_outside_roots_and_honors_opt_out() {
    for mode in ["old", "symlink", "outside", "keep"] {
        let f = Fixture::new();
        let path = match mode {
            "outside" => {
                let path = f.root.join("ahrb-outside");
                fs::create_dir(&path).unwrap();
                path
            }
            _ => f.root.join("tmp/ahrb-unrelated"),
        };
        let output = f.run(
            "six-harness",
            if mode == "keep" { "pass" } else { mode },
            &[
                ("STUB_PROFILE", path.to_str().unwrap()),
                ("AHRB_KEEP_PROFILES", if mode == "keep" { "1" } else { "0" }),
            ],
        );
        let log = f.log("six-harness", &output);
        assert!(path.exists());
        f.assert_preserved();
        if mode == "keep" {
            assert!(output.status.success(), "{log}");
            let out = f.run_dir(&output).join("codex");
            let report: serde_json::Value =
                serde_json::from_slice(&fs::read(out.join("report.json")).unwrap()).unwrap();
            assert!(PathBuf::from(report["profile_path"].as_str().unwrap()).is_dir());
            assert!(out.join("profile-local").is_dir());
        } else {
            assert!(!output.status.success(), "{mode}: {log}");
            assert!(log.contains("CLEANUP_REFUSED"), "{mode}: {log}");
        }
    }
}

#[test]
fn low_disk_aborts_before_launch_and_fails() {
    let f = Fixture::new();
    let output = f.run("six-harness", "pass", &[("AHRB_MIN_FREE_MB", "999999999")]);
    let log = f.log("six-harness", &output);
    assert!(!output.status.success(), "{log}");
    assert!(log.contains("ABORT_LOWDISK"));
    assert!(log.ends_with("SIX_HARNESS FAIL\n"));
    assert!(!f.run_dir(&output).join("calls.jsonl").exists());
    f.assert_preserved();
}

#[test]
fn an_existing_run_directory_is_refused_without_modifying_evidence() {
    for script in ["mock-cert", "six-harness"] {
        let f = Fixture::new();
        let bin = f.root.join("bin");
        fs::create_dir(&bin).unwrap();
        fs::write(bin.join("date"), "#!/bin/sh\necho 20260906T000000Z\n").unwrap();
        fs::set_permissions(bin.join("date"), fs::Permissions::from_mode(0o755)).unwrap();
        let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
        let env = [("STUB_SEED", "1234"), ("PATH", path.as_str())];
        let first = f.run(script, "pass", &env);
        let log = f.log(script, &first);
        assert!(first.status.success(), "{log}\n{first:?}");
        let second = f.run(script, "pass", &env);
        assert_eq!(f.run_dir(&first), f.run_dir(&second));
        assert!(!second.status.success());
        assert!(String::from_utf8_lossy(&second.stdout).contains("cannot create fresh directory"));
        assert_eq!(log, f.log(script, &second));
        f.assert_preserved();
    }
}

#[test]
fn fallback_build_failures_are_recorded_without_launching_a_run() {
    for script in ["mock-cert", "six-harness"] {
        let f = Fixture::new();
        let scripts = f.root.join("scripts");
        fs::create_dir(&scripts).unwrap();
        let copy = scripts.join(format!("{script}.sh"));
        fs::copy(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("scripts/{script}.sh")),
            &copy,
        )
        .unwrap();
        let bin = f.root.join("bin");
        fs::create_dir(&bin).unwrap();
        fs::write(bin.join("cargo"), "#!/bin/sh\nexit 9\n").unwrap();
        fs::set_permissions(bin.join("cargo"), fs::Permissions::from_mode(0o755)).unwrap();
        let output = Command::new("zsh")
            .arg(copy)
            .env_remove("AHRB_BIN")
            .env("AHRB_OUT_ROOT", f.root.join("out"))
            .env(
                "PATH",
                format!("{}:{}", bin.display(), std::env::var("PATH").unwrap()),
            )
            .output()
            .unwrap();
        let log = f.log(script, &output);
        assert!(!output.status.success(), "{log}");
        assert!(log.contains("BUILD EXIT=9"));
        assert!(log.ends_with(" FAIL\n"));
        assert!(!f.run_dir(&output).join("calls.jsonl").exists());
    }
}

#[test]
fn reference_preflight_blocks_work_on_doctor_disk_load_and_cert_failures() {
    for (mode, extra, expected) in [
        ("doctor", vec![], "RESULT mock-exec/doctor: EXIT=9"),
        (
            "pass",
            vec![("AHRB_BIN", "/nonexistent-ahrb-reference-binary")],
            "RESULT binary: EXIT=2",
        ),
        (
            "pass",
            vec![("AHRB_MIN_FREE_MB", "999999999")],
            "ABORT_LOWDISK",
        ),
        (
            "pass",
            vec![
                ("AHRB_MAX_LOAD", "0.000000001"),
                ("AHRB_LOAD_WAIT_SECONDS", "0"),
            ],
            "LOAD_WAIT_EXHAUSTED",
        ),
        (
            "classification",
            vec![("AHRB_SKIP_MOCK_CERT", "0")],
            "RESULT mock-cert: EXIT=1",
        ),
        (
            "pass",
            vec![("AHRB_SKIP_MOCK_CERT_REASON", "")],
            "AHRB_SKIP_MOCK_CERT_REASON required",
        ),
    ] {
        let f = Fixture::new();
        let output = f.run("reference-run", mode, &extra);
        let log = f.log("reference-run", &output);
        assert_eq!(output.status.code(), Some(2), "{log}\n{output:?}");
        assert!(log.contains(expected), "{log}");
        assert!(log.ends_with("REFERENCE FAIL\n"));
        assert!(!f.run_dir(&output).join("mock/matrix").exists());
        f.assert_preserved();
    }
}

#[test]
fn reference_resume_skips_reports_preserves_receipts_and_keeps_failures() {
    let f = Fixture::new();
    let first = f.run("reference-run", "pass", &[]);
    let log = f.log("reference-run", &first);
    assert_eq!(first.status.code(), Some(1), "{log}\n{first:?}");
    assert!(log.ends_with("REFERENCE PARTIAL\n")); // mock row 68 is UNSUPPORTED
    let run = f.run_dir(&first);
    let report = run.join("mock/matrix/report.json");
    let original = fs::read(&report).unwrap();
    let calls = fs::read(run.join("mock/calls.jsonl")).unwrap();
    let resume = f.run_args(
        "reference-run",
        "exit",
        &[],
        &["--resume", run.to_str().unwrap()],
    );
    let resumed_log = f.log("reference-run", &resume);
    assert_eq!(resume.status.code(), Some(1), "{resumed_log}");
    assert_eq!(
        resumed_log
            .matches("SKIPPED provenance-bound report")
            .count(),
        8
    );
    assert_eq!(fs::read(report).unwrap(), original);
    assert_eq!(fs::read(run.join("mock/calls.jsonl")).unwrap(), calls);
    fs::write(run.join("mock/matrix/exit-code.txt"), "7\n").unwrap();
    let failed = f.run_args(
        "reference-run",
        "pass",
        &[],
        &["--resume", run.to_str().unwrap()],
    );
    assert_eq!(failed.status.code(), Some(2));
    assert!(f
        .log("reference-run", &failed)
        .ends_with("REFERENCE FAIL\n"));
    fs::write(run.join("mock/matrix/exit-code.txt"), "0\n").unwrap();
    fs::write(run.join("mock/matrix/report.json"), "[]").unwrap();
    let malformed = f.run_args(
        "reference-run",
        "pass",
        &[],
        &["--resume", run.to_str().unwrap()],
    );
    assert_eq!(malformed.status.code(), Some(1));
    assert!(f
        .log("reference-run", &malformed)
        .contains("RE-RUN reason=step receipt differs"));
    f.assert_preserved();
}

#[test]
fn reference_propagates_failures_runs_later_pillars_and_scopes_cleanup() {
    for mode in [
        "exit",
        "missing",
        "malformed",
        "incomplete",
        "duplicate",
        "fidelity-deadline",
        "old",
        "symlink",
        "outside",
        "keep",
    ] {
        let f = Fixture::new();
        let outside = f.root.join("ahrb-outside");
        fs::create_dir(&outside).unwrap();
        let profile = if mode == "outside" {
            outside
        } else {
            f.root.join("tmp/ahrb-unrelated")
        };
        let output = f.run(
            "reference-run",
            if mode == "keep" { "pass" } else { mode },
            &[
                ("STUB_PROFILE", profile.to_str().unwrap()),
                ("AHRB_KEEP_PROFILES", if mode == "keep" { "1" } else { "0" }),
            ],
        );
        let log = f.log("reference-run", &output);
        assert_eq!(
            output.status.code(),
            Some(
                if ["keep", "missing", "incomplete", "duplicate"].contains(&mode) {
                    1
                } else {
                    2
                }
            ),
            "{mode}: {log}\n{output:?}"
        );
        assert!(log.contains("RESULT mock-exec/storage: EXIT="), "{log}");
        assert!(profile.exists());
        f.assert_preserved();
        if mode == "keep" {
            assert!(f
                .run_dir(&output)
                .join("mock/storage/profile-local")
                .is_dir());
        }
    }
}

#[test]
fn reference_retries_incomplete_attempts_without_erasing_diagnostics() {
    let f = Fixture::new();
    let first = f.run("reference-run", "missing", &[]);
    let run = f.run_dir(&first);
    fs::write(
        run.join("mock/matrix/diagnostic.txt"),
        "retain interrupted evidence",
    )
    .unwrap();
    let resumed = f.run_args(
        "reference-run",
        "pass",
        &[],
        &["--resume", run.to_str().unwrap()],
    );
    assert_eq!(
        resumed.status.code(),
        Some(1),
        "{}",
        f.log("reference-run", &resumed)
    );
    let archive = fs::read_dir(run.join("mock"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("attempt-matrix-")
        })
        .unwrap();
    assert_eq!(
        fs::read_to_string(archive.join("output/diagnostic.txt")).unwrap(),
        "retain interrupted evidence"
    );
    assert!(run.join("mock/matrix/report.json").is_file());
}

#[test]
fn reference_pass_and_storage_default_budget_are_explicit() {
    let f = Fixture::new();
    let output = f.run(
        "reference-run",
        "pass",
        &[
            ("AHRB_REFERENCE_HARNESSES", "mock-exec"),
            ("AHRB_DEADLINE", "0"),
        ],
    );
    let log = f.log("reference-run", &output);
    assert!(output.status.success(), "{log}\n{output:?}");
    assert!(log.ends_with("REFERENCE PASS\n"));
    let calls = fs::read_to_string(f.run_dir(&output).join("mock-exec/calls.jsonl")).unwrap();
    let calls: Vec<serde_json::Value> = calls
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(calls.len(), 4);
    assert!(calls.iter().all(|call| call["deadline_env"].is_null()));
    let args = calls[3]["args"].as_array().unwrap();
    assert!(args.iter().any(|a| a == "storage"));
    assert!(!args.iter().any(|a| a == "--deadline"));
    assert!(log.contains("deadline=storage-default"));
}

#[test]
fn reference_refuses_locked_runs_and_changed_provenance() {
    let f = Fixture::new();
    let first = f.run("reference-run", "pass", &[]);
    let run = f.run_dir(&first);
    let original_log = f.log("reference-run", &first);
    fs::create_dir(run.join(".reference-lock")).unwrap();
    let locked = f.run_args(
        "reference-run",
        "pass",
        &[],
        &["--resume", run.to_str().unwrap()],
    );
    assert_eq!(locked.status.code(), Some(2));
    assert_eq!(f.log("reference-run", &locked), original_log);
    fs::remove_dir(run.join(".reference-lock")).unwrap();
    let provenance = run.join("provenance.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&fs::read(&provenance).unwrap()).unwrap();
    value["ahrb_revision"] = "changed".into();
    fs::write(provenance, serde_json::to_vec(&value).unwrap()).unwrap();
    let changed = f.run_args(
        "reference-run",
        "pass",
        &[],
        &["--resume", run.to_str().unwrap()],
    );
    assert_eq!(changed.status.code(), Some(2));
    assert!(f
        .log("reference-run", &changed)
        .contains("resume provenance differs"));
}

#[test]
fn reference_tables_are_sanitized_and_label_missing_and_unsupported() {
    let f = Fixture::new();
    let output = f.run("reference-run", "pass", &[]);
    let run = f.run_dir(&output);
    let report = run.join("mock/storage/report.json");
    let mut value: serde_json::Value = serde_json::from_slice(&fs::read(&report).unwrap()).unwrap();
    value["storage_summary"]["measurement_label"] = "OS-accounted owned-tree physical I/O; per-file allocated footprint, not unique-volume consumption".into();
    value["storage_summary"]["counter_source"] = "/Users/private/account".into();
    value["model_requests"] = serde_json::json!([{"body": "RAW_REQUEST_SENTINEL"}]);
    value["results"][0]["outcome"] =
        serde_json::json!({"class": "UNSUPPORTED", "detail": "PRIVATE_DETAIL_SENTINEL"});
    value["results"][1]["outcome"]["class"] = "ERROR".into();
    fs::write(report, serde_json::to_vec(&value).unwrap()).unwrap();
    fs::remove_file(run.join("mock-exec/fidelity/report.json")).unwrap();
    let tables = Command::new("python3")
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/reference-tables.py"))
        .arg(&run)
        .output()
        .unwrap();
    assert!(tables.status.success(), "{tables:?}");
    let text = String::from_utf8(tables.stdout).unwrap();
    for expected in [
        "measurement_label | OS-accounted owned-tree physical I/O; per-file allocated footprint, not unique-volume consumption",
        "Automation Ready v2 · macos · client-process-fanout · quick · N8 · R32 · L250 · C10 · A100 · replay+resume",
        "D64",
        "Gbounded",
        "S1 | UNSUPPORTED",
        "S2 | ERROR",
        "fidelity — MISSING",
        "Provenance",
        "no ranking",
        "UNAVAILABLE",
    ] {
        assert!(text.contains(expected), "missing {expected}: {text}");
    }
    for forbidden in [
        "/Users/",
        "RAW_REQUEST_SENTINEL",
        "PRIVATE_DETAIL_SENTINEL",
        "profile_path",
        run.to_str().unwrap(),
    ] {
        assert!(!text.contains(forbidden), "leaked {forbidden}");
    }
}

#[test]
fn reference_defaults_to_six_harnesses_in_handoff_order_with_four_pillars() {
    let f = Fixture::new();
    let bin = f.root.join("bin");
    fs::create_dir(&bin).unwrap();
    for name in [
        "claude", "opencode", "pi", "rick", "haider", "haiderd", "codex",
    ] {
        let executable = bin.join(name);
        fs::write(&executable, "#!/bin/sh\necho fixture-version-1\n").unwrap();
        fs::set_permissions(executable, fs::Permissions::from_mode(0o755)).unwrap();
    }
    let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
    let output = f.run(
        "reference-run",
        "pass",
        &[("AHRB_REFERENCE_HARNESSES", ""), ("PATH", &path)],
    );
    let log = f.log("reference-run", &output);
    assert!(output.status.success(), "{log}\n{output:?}");
    let starts: Vec<_> = log
        .lines()
        .filter(|line| line.starts_with("START ") && line.contains("/matrix "))
        .collect();
    let order = [
        "claude-code",
        "opencode",
        "pi",
        "rick",
        "haider-agent",
        "codex",
    ];
    assert_eq!(starts.len(), order.len());
    for (line, h) in starts.iter().zip(order) {
        assert!(line.starts_with(&format!("START {h}/matrix ")));
        let calls = fs::read_to_string(f.run_dir(&output).join(h).join("calls.jsonl")).unwrap();
        let calls: Vec<serde_json::Value> = calls
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(calls.len(), 4);
        for (call, pillar) in calls
            .iter()
            .zip(["matrix", "economy", "fidelity", "storage"])
        {
            let args = call["args"].as_array().unwrap();
            let position = args.iter().position(|a| a == "--pillar").unwrap();
            assert_eq!(args[position + 1], pillar);
            assert_eq!(
                call["ttl"].as_str(),
                if h == "haider-agent" { Some("0") } else { None }
            );
            let deadline = args.iter().position(|a| a == "--deadline");
            if pillar == "storage" {
                assert!(deadline.is_none());
            } else {
                assert_eq!(
                    args[deadline.unwrap() + 1],
                    match h {
                        "codex" => "3600",
                        "opencode" => "1800",
                        _ => "1500",
                    }
                );
            }
        }
    }
}

#[test]
fn reference_requires_typed_fields_and_complete_claimed_measurements() {
    for (mode, reason) in [
        ("empty-summary", "missing field"),
        (
            "missing-measurement",
            "missing claimed measurement write_bytes_per_turn_p50",
        ),
        ("truncated-curve", "length differs from model_turns"),
        (
            "missing-tokenizer",
            "reference_tokenizer.version: missing field",
        ),
        ("missing-needle", "incomplete or duplicate planted set"),
        (
            "invalid-schema",
            "does not match current quick task contract",
        ),
    ] {
        let f = Fixture::new();
        let output = f.run(
            "reference-run",
            mode,
            &[("AHRB_REFERENCE_HARNESSES", "mock-exec")],
        );
        let log = f.log("reference-run", &output);
        assert_eq!(output.status.code(), Some(1), "{log}");
        assert!(log.contains(reason), "{log}");
        assert!(log.ends_with("REFERENCE PARTIAL\n"));
    }
    let f = Fixture::new();
    let output = f.run(
        "reference-run",
        "pass",
        &[("AHRB_REFERENCE_HARNESSES", "mock-exec")],
    );
    assert!(
        output.status.success(),
        "{}",
        f.log("reference-run", &output)
    );
    let nullable = f.run(
        "reference-run",
        "nullable-evidence",
        &[("AHRB_REFERENCE_HARNESSES", "mock-exec")],
    );
    assert!(
        nullable.status.success(),
        "{}",
        f.log("reference-run", &nullable)
    );
    let run = f.run_dir(&output);
    // Remove each required field from an otherwise complete synthetic typed report.
    // Nullable scalars remain valid when present as null in the baseline fixture.
    for pillar in ["economy", "fidelity", "storage"] {
        let out = run.join("mock-exec").join(pillar);
        let file = out.join("report.json");
        let original: serde_json::Value =
            serde_json::from_slice(&fs::read(&file).unwrap()).unwrap();
        let name = format!("{pillar}_summary");
        for field in original[&name].as_object().unwrap().keys() {
            let mut truncated = original.clone();
            truncated[&name].as_object_mut().unwrap().remove(field);
            fs::write(&file, serde_json::to_vec(&truncated).unwrap()).unwrap();
            let assessment = Command::new("python3")
                .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/reference-support.py"))
                .args(["assess", out.to_str().unwrap(), pillar])
                .output()
                .unwrap();
            assert_eq!(
                assessment.status.code(),
                Some(3),
                "{pillar}.{field}: {assessment:?}"
            );
            let text = String::from_utf8_lossy(&assessment.stdout);
            assert!(
                text.contains(&format!("{name}.{field}: missing field")),
                "{text}"
            );
        }
        fs::write(&file, serde_json::to_vec(&original).unwrap()).unwrap();
    }
}

#[test]
fn reference_reruns_each_unbound_non_matrix_pillar_and_preserves_old_report() {
    let f = Fixture::new();
    let first = f.run(
        "reference-run",
        "pass",
        &[("AHRB_REFERENCE_HARNESSES", "mock-exec")],
    );
    let source = f.run_dir(&first);
    for pillar in ["economy", "fidelity", "storage"] {
        let run = f.root.join("out").join(format!("unbound-{pillar}"));
        let old = run.join("mock-exec").join(pillar);
        fs::create_dir_all(&old).unwrap();
        for file in [
            "report.json",
            "exit-code.txt",
            "cleanup-exit-code.txt",
            "step-provenance.json",
        ] {
            fs::copy(
                source.join("mock-exec").join(pillar).join(file),
                old.join(file),
            )
            .unwrap();
        }
        let original = fs::read(old.join("report.json")).unwrap();
        let resume = f.run_args(
            "reference-run",
            "pass",
            &[("AHRB_REFERENCE_HARNESSES", "mock-exec")],
            &["--resume", run.to_str().unwrap()],
        );
        let log = f.log("reference-run", &resume);
        assert!(resume.status.success(), "{log}");
        assert!(
            log.contains("no original provenance; all existing steps will re-run"),
            "{log}"
        );
        assert!(!log.contains("SKIPPED provenance-bound report"), "{log}");
        let archive = fs::read_dir(run.join("mock-exec"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with(&format!("attempt-{pillar}-"))
            })
            .unwrap();
        assert_eq!(
            fs::read(archive.join("output/report.json")).unwrap(),
            original
        );
    }
}

#[test]
fn reference_resume_rejects_step_identity_mismatches() {
    let f = Fixture::new();
    let first = f.run(
        "reference-run",
        "pass",
        &[("AHRB_REFERENCE_HARNESSES", "mock-exec")],
    );
    let run = f.run_dir(&first);
    for field in [
        "step",
        "pillar",
        "harness",
        "profile",
        "manifest_sha256",
        "report_manifest_sha256",
        "candidate_revision",
        "provenance_sha256",
        "report_sha256",
    ] {
        let receipt = run.join("mock-exec/economy/step-provenance.json");
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&receipt).unwrap()).unwrap();
        value[field] = "wrong".into();
        fs::write(&receipt, serde_json::to_vec(&value).unwrap()).unwrap();
        let resume = f.run_args(
            "reference-run",
            "pass",
            &[("AHRB_REFERENCE_HARNESSES", "mock-exec")],
            &["--resume", run.to_str().unwrap()],
        );
        let log = f.log("reference-run", &resume);
        assert!(resume.status.success(), "{field}: {log}");
        let last = log.rsplit("profile=quick\n").next().unwrap();
        assert!(
            last.contains("RESUME mock-exec/economy: RE-RUN reason=step receipt differs"),
            "{last}"
        );
        assert_eq!(
            last.matches("ADOPTED same step/pillar/harness/profile/manifest/candidate")
                .count(),
            3,
            "{last}"
        );
        assert!(last.contains("RESULT mock-exec/economy: EXIT=0"), "{last}");
    }
}

#[test]
fn reference_guard_after_a_completed_step_is_an_explicit_fail() {
    let f = Fixture::new();
    fs::write(
        f.root.join("python/sitecustomize.py"),
        r#"import os, pathlib, shutil, sys
os.getloadavg = lambda: (1.0, 1.0, 1.0)
def disk(path):
    # Deterministically stop before the second scheduled pillar.
    completed = (pathlib.Path(path) / 'mock-exec/matrix/report.json').exists()
    return shutil._ntuple_diskusage(10**15, 0, 0 if completed else 10**15)
shutil.disk_usage = disk
"#,
    )
    .unwrap();
    let output = f.run(
        "reference-run",
        "pass",
        &[
            ("AHRB_REFERENCE_HARNESSES", "mock-exec"),
            ("AHRB_MIN_FREE_MB", "16000"),
        ],
    );
    let log = f.log("reference-run", &output);
    assert_eq!(output.status.code(), Some(2), "{log}");
    assert!(
        log.contains("RESULT mock-exec/matrix/assessment: EXIT=0"),
        "{log}"
    );
    assert!(log.contains("ABORT_LOWDISK: free_mib=0 < min_free_mib=16000; guard stop is FAIL; remaining scheduled steps were not run"), "{log}");
    assert!(!log.contains("START mock-exec/economy "), "{log}");
    assert!(log.ends_with("REFERENCE FAIL\n"));
}

#[test]
fn reference_tables_preserve_badge_none_and_withheld_states() {
    let f = Fixture::new();
    let output = f.run(
        "reference-run",
        "pass",
        &[("AHRB_REFERENCE_HARNESSES", "mock-exec")],
    );
    let run = f.run_dir(&output);
    let file = run.join("mock-exec/matrix/report.json");
    let mut report: serde_json::Value = serde_json::from_slice(&fs::read(&file).unwrap()).unwrap();
    for (badge, expected) in [
        (serde_json::Value::Null, "WITHHELD (none)"),
        ("none".into(), "none"),
        ("withheld".into(), "withheld"),
    ] {
        report["badge"] = badge;
        fs::write(&file, serde_json::to_vec(&report).unwrap()).unwrap();
        let tables = Command::new("python3")
            .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/reference-tables.py"))
            .arg(&run)
            .output()
            .unwrap();
        assert!(tables.status.success(), "{tables:?}");
        assert!(
            String::from_utf8_lossy(&tables.stdout)
                .contains(&format!("| 73 | 0 | 0 | 0 | 0 | 0 | {expected} |")),
            "{tables:?}"
        );
    }
}
