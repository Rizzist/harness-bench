//! Storage lifecycle oracles and runtime scope guards, independent of adapters.
use super::{
    accounting::{FileEntry, Inventory},
    evidence::*,
    *,
};
use crate::{AhrbError, Result, evaluate::TestOutcome};
use std::path::{Component, Path, PathBuf};

/// New/replaced/content-changed regular files count even when allocation is zero.
pub fn residue<'a>(
    before: &Inventory,
    after: &'a Inventory,
    scope: &[String],
) -> Vec<&'a FileEntry> {
    let old = before
        .entries
        .iter()
        .map(|e| (&e.path, e))
        .collect::<BTreeMap<_, _>>();
    after
        .entries
        .iter()
        .filter(|e| e.kind == "regular")
        .filter(|e| {
            scope
                .iter()
                .any(|p| p.is_empty() || e.path == *p || e.path.starts_with(&format!("{p}/")))
        })
        .filter(|e| {
            old.get(&e.path).is_none_or(|b| {
                b.kind != "regular"
                    || b.device_id != e.device_id
                    || b.inode_or_file_id != e.inode_or_file_id
                    || b.sha256 != e.sha256
            })
        })
        .collect()
}

pub fn declared_outcome<'a>(outcomes: impl Iterator<Item = &'a TestOutcome>) -> TestOutcome {
    outcomes
        .max_by_key(|o| match o {
            TestOutcome::Error(_) => 5,
            TestOutcome::Fail(_) => 4,
            TestOutcome::Absent(_) => 3,
            TestOutcome::Unsupported(_) => 2,
            TestOutcome::Pass => 1,
        })
        .cloned()
        .unwrap_or_else(|| TestOutcome::Unsupported("no declared deletion operations".into()))
}

/// Check every existing ancestor without following symlinks, including absent targets.
pub fn contained_path(root: &Path, path: &Path) -> Result<PathBuf> {
    if !root.is_absolute()
        || !path.is_absolute()
        || path
            .components()
            .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
    {
        return Err(crate::AhrbError::Validation(
            "storage path is not normalized absolute".into(),
        ));
    }
    let relative = path.strip_prefix(root).map_err(|_| {
        crate::AhrbError::Validation("storage path escapes disposable profile".into())
    })?;
    // Include the supplied root and its ancestors, even for {{profile}} alone.
    // Callers creating profiles under OS temp aliases must resolve those aliases
    // before supplying the profile, rather than accepting arbitrary root links.
    for ancestor in root.ancestors() {
        if std::fs::symlink_metadata(ancestor)?
            .file_type()
            .is_symlink()
        {
            return Err(crate::AhrbError::Validation(
                "storage profile traverses a symlink".into(),
            ));
        }
    }
    let metadata = std::fs::symlink_metadata(root)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(crate::AhrbError::Validation(
            "storage profile is not a no-follow directory".into(),
        ));
    }
    let canonical_root = std::fs::canonicalize(root)?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component);
        match std::fs::symlink_metadata(&current) {
            Ok(meta) if meta.file_type().is_symlink() => {
                return Err(crate::AhrbError::Validation(
                    "storage path traverses a symlink".into(),
                ));
            }
            Ok(_) => {
                if !std::fs::canonicalize(&current)?.starts_with(&canonical_root) {
                    return Err(crate::AhrbError::Validation(
                        "storage path resolves outside disposable profile".into(),
                    ));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(relative.to_path_buf())
}

pub fn render_verb(
    name: &str,
    argv: &[String],
    variables: &BTreeMap<String, String>,
    root: &Path,
) -> Result<Vec<String>> {
    validate_verb(name, argv, name != "uninstall_cleanup")?;
    let rendered = argv
        .iter()
        .map(|a| crate::manifest::render_template(a, variables))
        .collect::<Result<Vec<_>>>()?;
    let mut resolved_template = argv.to_vec();
    let program =
        std::fs::canonicalize(&rendered[0]).unwrap_or_else(|_| PathBuf::from(&rendered[0]));
    resolved_template[0] = program.to_string_lossy().into_owned();
    validate_verb(name, &resolved_template, name != "uninstall_cleanup")?;
    for arg in rendered.iter().skip(1) {
        let value = verb_argument_value(arg);
        if value.contains(['{', '}', '$', '`', '\\', '\n', '\r']) || value.starts_with('~') {
            return Err(crate::AhrbError::Validation(
                "unsafe rendered storage argument".into(),
            ));
        }
        if value.split('/').any(|p| p == "." || p == "..") {
            return Err(crate::AhrbError::Validation(
                "storage argument contains traversal".into(),
            ));
        }
        if value.contains('/') {
            contained_path(root, Path::new(value))?;
        }
    }
    Ok(rendered)
}

pub fn resume_path(p: bool, exec: bool, control: bool) -> Option<ResumePath> {
    match (p, exec, control) {
        (true, false, _) => None,
        (true, true, false) => Some(ResumePath::ExecContinuation),
        (true, true, true) => Some(ResumePath::ExecControlContinuation),
        (false, true, false) => Some(ResumePath::DaemonExecContinuation),
        (false, true, true) => Some(ResumePath::DaemonExecControlContinuation),
        (false, false, _) => Some(ResumePath::DaemonReattach),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn declared_only_precedence_is_not_general_precedence() {
        let missing = TestOutcome::Unsupported("missing".into());
        let fail = TestOutcome::Fail("residue".into());
        let absent = TestOutcome::Absent("locator".into());
        let error = TestOutcome::Error("command".into());
        assert_eq!(
            declared_outcome([&fail, &absent, &missing].into_iter()),
            fail
        );
        assert_eq!(declared_outcome([&fail, &error].into_iter()), error);
        assert_eq!(
            declared_outcome([&TestOutcome::Pass].into_iter()),
            TestOutcome::Pass
        );
        assert!(matches!(
            declared_outcome([].into_iter()),
            TestOutcome::Unsupported(_)
        ));
    }
    #[test]
    fn residue_counts_empty_replaced_and_changed_files_but_not_unchanged_baseline() {
        let file = FileEntry {
            path: "store/a".into(),
            kind: "regular".into(),
            device_id: 1,
            inode_or_file_id: 1,
            allocated_bytes: 0,
            apparent_bytes: 0,
            sha256: Some("empty".into()),
            family: "store".into(),
        };
        let mut before = Inventory::default();
        before.entries.push(file.clone());
        let mut after = before.clone();
        assert!(residue(&before, &after, &["store".into()]).is_empty());
        after.entries[0].inode_or_file_id = 2;
        assert_eq!(residue(&before, &after, &["store".into()]).len(), 1);
        after.entries[0] = file.clone();
        after.entries[0].sha256 = Some("changed".into());
        assert_eq!(residue(&before, &after, &["store".into()]).len(), 1);
        after.entries[0].path = "elsewhere/a".into();
        assert!(residue(&before, &after, &["store".into()]).is_empty());
        assert_eq!(residue(&before, &after, &[String::new()]).len(), 1);
    }
    #[test]
    fn resume_branches_are_exclusive_and_topology_driven() {
        for p in [false, true] {
            for e in [false, true] {
                for c in [false, true] {
                    let path = resume_path(p, e, c);
                    assert_eq!(path.is_none(), p && !e);
                    if !p && !e {
                        assert_eq!(path, Some(ResumePath::DaemonReattach));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod scope_tests {
    use super::*;
    #[test]
    fn rendered_paths_reject_template_injection_and_symlink_ancestors() {
        let root = std::env::temp_dir().join(format!(
            "ahrb-scope-{}-{}",
            std::process::id(),
            crate::fake_model::monotonic_timestamp_ns()
        ));
        std::fs::create_dir(&root).unwrap();
        let root = std::fs::canonicalize(root).unwrap();
        let argv = [
            "harness",
            "delete",
            "--profile",
            "{{profile}}",
            "--session-id",
            "{{session_id}}",
        ]
        .map(str::to_owned);
        let mut variables = BTreeMap::from([
            ("profile".into(), root.to_string_lossy().into_owned()),
            ("session_id".into(), "session".into()),
        ]);
        render_verb("session_delete", &argv, &variables, &root).unwrap();
        for id in ["../../outside", "/outside", "${HOME}", "{{credential}}"] {
            variables.insert("session_id".into(), id.into());
            assert!(render_verb("session_delete", &argv, &variables, &root).is_err());
        }
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(std::env::temp_dir(), root.join("escape")).unwrap();
            assert!(contained_path(&root, &root.join("escape/new-file")).is_err());
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

/// Validate the exclusive S10 timing table and complete per-identity read bracket.
/// Counter brackets enclose origins/provider receipt; metadata and snapshots do
/// not get relabelled as harness reads or continuation launch timestamps.
pub fn validate_resume_bracket(d: &ResumeDiagnostics) -> Result<()> {
    let fail = |s: &str| crate::AhrbError::Protocol(format!("storage resume bracket: {s}"));
    let e = d.transport_kind == crate::manifest::TransportKind::Exec;
    let p = d.per_invocation_topology;
    let c = e && d.resume_control_declared;
    let expected = resume_path(p, e, d.resume_control_declared);
    if expected.is_none() || d.resume_path != expected {
        return Err(fail("unsupported or inconsistent path"));
    }
    let reattach = d
        .reattach_start_ns
        .ok_or_else(|| fail("missing reattach boundary"))?;
    if d.continuation_start_ns.is_some() != e
        || d.control_start_ns.is_some() != c
        || d.control_end_ns.is_some() != c
    {
        return Err(fail(
            "missing applicable or invented inapplicable launch/exit boundary",
        ));
    }
    let origin = if !p {
        Some(reattach)
    } else if c {
        d.control_start_ns
    } else {
        d.continuation_start_ns
    };
    let headline = if p {
        d.continuation_start_ns
    } else {
        Some(reattach)
    };
    if d.read_start_ns != origin || d.resume_start_ns != headline {
        return Err(fail("incorrect topology/control origin"));
    }
    let counter_start = d
        .counter_start_ns
        .ok_or_else(|| fail("missing initial counter boundary"))?;
    let counter_end = d
        .counter_end_ns
        .ok_or_else(|| fail("missing final counter boundary"))?;
    let first_request = d
        .first_request_ns
        .ok_or_else(|| fail("missing provider receipt"))?;
    let origin = origin.unwrap();
    let headline = headline.unwrap();
    if !(counter_start <= origin
        && origin <= headline
        && headline <= first_request
        && first_request <= counter_end
        && reattach <= headline)
    {
        return Err(fail("counter interval does not enclose resume"));
    }
    if let (Some(start), Some(end), Some(continuation)) = (
        d.control_start_ns,
        d.control_end_ns,
        d.continuation_start_ns,
    ) {
        if start < reattach || end < start || continuation < end {
            return Err(fail("control and continuation are not ordered"));
        }
        if !d
            .identities
            .iter()
            .any(|i| i.retirement_method == "RetiredAfterFinalSample")
        {
            return Err(fail("missing control retirement"));
        }
    }
    if d.start_skew_ns != Some(origin - counter_start)
        || d.end_skew_ns != Some(counter_end - first_request)
    {
        return Err(fail("incorrect counter skew"));
    }
    if d.identities.is_empty()
        || d.identities
            .iter()
            .any(|i| !i.complete || i.first_bytes.is_none() || i.last_bytes.is_none())
    {
        return Err(fail("incomplete owned identities"));
    }
    let first = d
        .identities
        .iter()
        .try_fold(0u64, |v, i| v.checked_add(i.first_bytes?));
    let last = d
        .identities
        .iter()
        .try_fold(0u64, |v, i| v.checked_add(i.last_bytes?));
    if first != d.first_read_bytes || last != d.last_read_bytes || first.is_none() || last < first {
        return Err(fail("identity read counters do not reconcile"));
    }
    Ok(())
}
pub fn compaction_summary(before: u64, after: u64) -> CompactionSummary {
    CompactionSummary {
        compaction_before_allocated_bytes: Some(before as f64),
        compaction_after_allocated_bytes: Some(after as f64),
        compaction_freed_pct: (before != 0)
            .then(|| 100.0 * (before as i128 - after as i128) as f64 / before as f64),
    }
}

pub fn evaluate_close(
    config: &StorageConfig,
    expected_sessions: u64,
    diagnostics: &CloseDiagnostics,
) -> Result<(CloseSummary, TestOutcome)> {
    let bad = |reason: &str| AhrbError::Protocol(format!("S5 incomplete close evidence: {reason}"));
    if expected_sessions == 0 || diagnostics.store_baseline_allocated_bytes.is_none() {
        return Err(bad("missing warm store baseline or session budget"));
    }
    if diagnostics.close_receipts.len() as u64 != expected_sessions {
        return Err(bad("missing public close receipt"));
    }
    let mut identities = std::collections::BTreeSet::new();
    for receipt in &diagnostics.close_receipts {
        let hash = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
        if receipt.exit_code != 0
            || !hash(&receipt.receipt_sha256)
            || !hash(&receipt.session_id_hash)
            || !identities.insert(&receipt.session_id_hash)
        {
            return Err(bad("invalid/duplicate close receipt"));
        }
    }
    let immediate = diagnostics
        .checkpoints
        .iter()
        .filter(|c| c.phase == ClosePhase::Immediate)
        .collect::<Vec<_>>();
    let expected = (0..=expected_sessions)
        .filter(|n| n % 10 == 0 || *n == expected_sessions)
        .collect::<Vec<_>>();
    if immediate
        .iter()
        .map(|c| c.closed_sessions)
        .collect::<Vec<_>>()
        != expected
        || immediate
            .first()
            .is_none_or(|c| c.retained_allocated_bytes != 0)
        || diagnostics
            .checkpoints
            .iter()
            .any(|c| !c.elapsed_s.is_finite() || c.elapsed_s < 0.0)
    {
        return Err(bad("missing, unordered or invalid checkpoint"));
    }
    let post = diagnostics
        .checkpoints
        .iter()
        .filter(|c| c.phase == ClosePhase::PostSweep)
        .collect::<Vec<_>>();
    match config.sweep_interval_s {
        Some(interval)
            if post.len() != 1
                || post[0].closed_sessions != expected_sessions
                || post[0].elapsed_s < interval as f64 =>
        {
            return Err(bad("missing/early post-sweep checkpoint"));
        }
        None if !post.is_empty() => return Err(bad("undeclared post-sweep measurement")),
        _ => {}
    }
    let class = config.retention_cap_bytes.map(|cap| {
        let bounded = if config.sweep_interval_s.is_some() {
            post[0].retained_allocated_bytes <= cap
        } else {
            immediate.iter().all(|c| c.retained_allocated_bytes <= cap)
        };
        if bounded {
            BoundClass::Bounded
        } else {
            BoundClass::Unbounded
        }
    });
    let summary = CloseSummary {
        closed_sessions: Some(expected_sessions),
        close_retained_bytes_per_session: immediate
            .last()
            .map(|c| c.retained_allocated_bytes as f64 / expected_sessions as f64),
        close_retained_after_sweep_bytes_per_session: post
            .first()
            .map(|c| c.retained_allocated_bytes as f64 / expected_sessions as f64),
        retention_cap_bytes: config.retention_cap_bytes,
        close_retention_class: class,
    };
    let outcome = if class.is_some() {
        TestOutcome::Pass
    } else {
        TestOutcome::Unsupported("no declared retention cap; byte observations retained".into())
    };
    Ok((summary, outcome))
}

/// General storage precedence; informational classes do not affect outcomes.
pub fn aggregate_outcome(outcomes: impl IntoIterator<Item = TestOutcome>) -> TestOutcome {
    let rank = |o: &TestOutcome| match o {
        TestOutcome::Error(_) => 5,
        TestOutcome::Absent(_) => 4,
        TestOutcome::Unsupported(_) => 3,
        TestOutcome::Fail(_) => 2,
        TestOutcome::Pass => 1,
    };
    outcomes
        .into_iter()
        .max_by_key(rank)
        .unwrap_or_else(|| TestOutcome::Error("missing repetition evidence".into()))
}
