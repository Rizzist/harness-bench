#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

struct Fixture {
    root: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("runner-scripts-{}-{nonce}", std::process::id()));
        fs::create_dir(&root).unwrap();
        fs::create_dir(root.join("tmp")).unwrap();
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
            .env("AHRB_OUT_ROOT", self.root.join("out"))
            .env("AHRB_TMP_ROOT", self.root.join("tmp"))
            .env("AHRB_BIN", self.root.join("ahrb-stub"))
            .env("AHRB_MIN_FREE_MB", "0")
            .env("AHRB_KEEP_PROFILES", "0")
            .env("STUB_MODE", mode)
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
import json, os, pathlib, sys
args = sys.argv
out = pathlib.Path(args[args.index('--output') + 1])
h = pathlib.Path(args[args.index('--manifest') + 1]).parent.name
mode = os.environ['STUB_MODE']
with open(out.parent / 'calls.jsonl', 'a') as calls:
    calls.write(json.dumps({'h': h, 'args': args[1:], 'ttl': os.environ.get('HAIDER_RUN_DAEMON_IDLE_TTL_MS')}) + '\n')
if mode == 'missing':
    sys.exit(0)
if mode == 'malformed':
    (out / 'report.json').write_text('{')
    sys.exit(0)
profile = pathlib.Path(os.environ['AHRB_TMP_ROOT']) / ('ahrb-' + out.parent.name + '-' + h)
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
r = {'results': tests, 'badge': None if mode == 'badge' else {'label': 'stub'}, 'profile_path': str(profile)}
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
