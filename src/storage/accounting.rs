//! Exhaustive, descriptor-relative no-follow allocation inventories.

use super::*;
use std::fs::{File, Metadata, OpenOptions};
use std::io::Read;
#[cfg(unix)]
use std::os::{
    fd::{AsRawFd, FromRawFd},
    unix::fs::{MetadataExt, OpenOptionsExt},
};
use std::path::Path;
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FileEntry {
    pub path: String,
    pub kind: String,
    pub device_id: u64,
    pub inode_or_file_id: u64,
    pub allocated_bytes: u64,
    pub apparent_bytes: u64,
    pub sha256: Option<String>,
    pub family: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Inventory {
    pub entries: Vec<FileEntry>,
    /// Failed transient capture attempts, retained on the accepted boundary.
    pub capture_retries: Vec<Vec<String>>,
    // Stability metadata is deliberately not added to the exact JSONL schema.
    stamps: BTreeMap<String, (i64, i64, i64, i64)>,
}

impl Inventory {
    pub fn allocated_bytes(&self) -> u64 {
        self.entries
            .iter()
            .filter(|e| e.kind == "regular")
            .map(|e| e.allocated_bytes)
            .sum()
    }
    pub fn apparent_bytes(&self) -> u64 {
        self.entries
            .iter()
            .filter(|e| e.kind == "regular")
            .map(|e| e.apparent_bytes)
            .sum()
    }
    pub fn regular_files(&self) -> u64 {
        self.entries.iter().filter(|e| e.kind == "regular").count() as u64
    }
    pub fn families(&self, config: &StorageConfig) -> BTreeMap<String, u64> {
        let mut totals = BTreeMap::from([("other".into(), 0)]);
        for name in config.areas.iter().flat_map(|a| a.keys()) {
            totals.insert(name.clone(), 0);
        }
        for entry in self.entries.iter().filter(|e| e.kind == "regular") {
            *totals.entry(entry.family.clone()).or_default() += entry.allocated_bytes;
        }
        totals
    }
    pub fn growth_since(&self, before: &Self) -> u64 {
        let before = before
            .entries
            .iter()
            .map(|e| (e.path.as_str(), e))
            .collect::<BTreeMap<_, _>>();
        self.entries
            .iter()
            .map(|after| file_growth(before.get(after.path.as_str()).copied(), Some(after)))
            .sum()
    }
}

/// New paths, replaced identities, deletion and same-identity truncation reset growth.
pub fn file_growth(before: Option<&FileEntry>, after: Option<&FileEntry>) -> u64 {
    match (before, after) {
        (Some(b), Some(a))
            if a.kind == "regular"
                && b.kind == "regular"
                && a.device_id == b.device_id
                && a.inode_or_file_id == b.inode_or_file_id
                && a.apparent_bytes >= b.apparent_bytes =>
        {
            a.allocated_bytes.saturating_sub(b.allocated_bytes)
        }
        _ => 0,
    }
}

fn changed_paths(a: &Inventory, b: &Inventory) -> BTreeSet<String> {
    let entries = |i: &Inventory| {
        i.entries
            .iter()
            .map(|e| (e.path.clone(), e.clone()))
            .collect::<BTreeMap<_, _>>()
    };
    let a_entries = entries(a);
    let b_entries = entries(b);
    a_entries
        .keys()
        .chain(b_entries.keys())
        .chain(a.stamps.keys())
        .chain(b.stamps.keys())
        .filter(|path| {
            a_entries.get(*path) != b_entries.get(*path)
                || a.stamps.get(*path) != b.stamps.get(*path)
        })
        .cloned()
        .collect()
}

#[derive(Debug)]
struct TransientCaptureChange(Vec<String>);
impl std::fmt::Display for TransientCaptureChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "storage transient changed during capture at {:?}",
            self.0
        )
    }
}
impl std::error::Error for TransientCaptureChange {}

fn capture_changed(path: &str, config: &StorageConfig) -> AhrbError {
    if config
        .family(path)
        .is_ok_and(|family| family == "transient")
    {
        std::io::Error::other(TransientCaptureChange(vec![path.into()])).into()
    } else {
        AhrbError::Protocol(format!("storage changed during capture at {path}"))
    }
}

fn capture_io(error: std::io::Error, path: &str, config: &StorageConfig) -> AhrbError {
    if error.kind() == std::io::ErrorKind::NotFound {
        capture_changed(path, config)
    } else {
        error.into()
    }
}

fn transient_retry(error: &AhrbError) -> Option<Vec<String>> {
    match error {
        AhrbError::Io(error) => error
            .get_ref()?
            .downcast_ref::<TransientCaptureChange>()
            .map(|e| e.0.clone()),
        _ => None,
    }
}

#[cfg(unix)]
fn signature(m: &Metadata) -> (u64, u64, u64, u64, u32, i64, i64, i64, i64) {
    (
        m.dev(),
        m.ino(),
        m.size(),
        m.blocks(),
        m.mode(),
        m.mtime(),
        m.mtime_nsec(),
        m.ctime(),
        m.ctime_nsec(),
    )
}

#[cfg(unix)]
fn open_child(parent: &File, name: &std::ffi::CStr, directory: bool) -> Result<File> {
    let flags = libc::O_RDONLY
        | libc::O_NOFOLLOW
        | libc::O_CLOEXEC
        | libc::O_NONBLOCK
        | if directory { libc::O_DIRECTORY } else { 0 };
    // SAFETY: parent is a live directory descriptor; name is NUL terminated.
    let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: successful openat transfers one newly owned descriptor.
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(unix)]
fn names(directory: &File) -> Result<Vec<std::ffi::CString>> {
    // Open '.' instead of dup: an independent open-file description avoids sharing
    // the directory cursor with later stability probes.
    let dot = std::ffi::CString::new(".").map_err(|e| AhrbError::Protocol(e.to_string()))?;
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            dot.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: fdopendir owns fd on success, closed below on every path.
    let stream = unsafe { libc::fdopendir(fd) };
    if stream.is_null() {
        unsafe { libc::close(fd) };
        return Err(std::io::Error::last_os_error().into());
    }
    let result = (|| {
        let mut names = Vec::new();
        loop {
            #[cfg(target_os = "macos")]
            unsafe {
                *libc::__error() = 0;
            }
            #[cfg(target_os = "linux")]
            unsafe {
                *libc::__errno_location() = 0;
            }
            let entry = unsafe { libc::readdir(stream) };
            if entry.is_null() {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() != Some(0) {
                    return Err(error.into());
                }
                break;
            }
            let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) };
            if name.to_bytes() != b"." && name.to_bytes() != b".." {
                names.push(name.to_owned());
            }
        }
        names.sort();
        Ok(names)
    })();
    unsafe { libc::closedir(stream) };
    result
}

#[cfg(unix)]
fn walk(
    directory: &File,
    prefix: &str,
    device: u64,
    config: &StorageConfig,
    digest: bool,
    seen: &mut BTreeSet<(u64, u64)>,
    inventory: &mut Inventory,
) -> Result<()> {
    let before = directory.metadata()?;
    inventory.stamps.insert(
        prefix.into(),
        (
            before.mtime(),
            before.mtime_nsec(),
            before.ctime(),
            before.ctime_nsec(),
        ),
    );
    for name in names(directory)? {
        let text = name
            .to_str()
            .map_err(|_| AhrbError::Protocol("storage non-UTF8 path".into()))?;
        let path = if prefix.is_empty() {
            text.into()
        } else {
            format!("{prefix}/{text}")
        };
        // fstatat is lstat relative to the held parent, so a parent swap cannot
        // redirect either the open or the final identity check outside the root.
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        if unsafe {
            libc::fstatat(
                directory.as_raw_fd(),
                name.as_ptr(),
                stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } != 0
        {
            return Err(capture_io(std::io::Error::last_os_error(), &path, config));
        }
        let stat = unsafe { stat.assume_init() };
        let kind = stat.st_mode & libc::S_IFMT;
        if kind == libc::S_IFLNK {
            return Err(AhrbError::Protocol(format!(
                "storage symlink/escape at {path}"
            )));
        }
        let identity = (stat.st_dev as u64, stat.st_ino);
        if identity.0 != device || !seen.insert(identity) {
            return Err(AhrbError::Protocol(format!(
                "storage device escape or repeated identity at {path}"
            )));
        }
        if kind != libc::S_IFREG && kind != libc::S_IFDIR {
            inventory.entries.push(FileEntry {
                path,
                kind: "special".into(),
                device_id: identity.0,
                inode_or_file_id: identity.1,
                allocated_bytes: 0,
                apparent_bytes: stat.st_size as u64,
                sha256: None,
                family: "other".into(),
            });
            continue;
        }
        let mut file =
            open_child(directory, &name, kind == libc::S_IFDIR).map_err(|error| match error {
                AhrbError::Io(error) => capture_io(error, &path, config),
                other => other,
            })?;
        let initial = file.metadata()?;
        inventory.stamps.insert(
            path.clone(),
            (
                initial.mtime(),
                initial.mtime_nsec(),
                initial.ctime(),
                initial.ctime_nsec(),
            ),
        );
        let initial_identity = (initial.dev(), initial.ino());
        if initial.dev() != device
            || (kind == libc::S_IFREG && !initial.is_file())
            || (kind == libc::S_IFDIR && !initial.is_dir())
            || (initial_identity != identity && seen.contains(&initial_identity))
        {
            return Err(AhrbError::Protocol(format!(
                "storage kind/device escape during capture at {path}"
            )));
        }
        if (initial.dev(), initial.ino()) != identity
            || initial.size() != stat.st_size as u64
            || initial.blocks() != stat.st_blocks as u64
        {
            return Err(capture_changed(&path, config));
        }
        let sha256 = if kind == libc::S_IFREG && digest {
            let mut hash = Sha256::new();
            let mut buf = [0_u8; 65536];
            loop {
                let n = file.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                hash.update(&buf[..n]);
            }
            Some(format!("{:x}", hash.finalize()))
        } else {
            None
        };
        let family = config.family(&path)?;
        inventory.entries.push(FileEntry {
            path: path.clone(),
            kind: if kind == libc::S_IFREG {
                "regular"
            } else {
                "directory"
            }
            .into(),
            device_id: identity.0,
            inode_or_file_id: identity.1,
            allocated_bytes: initial
                .blocks()
                .checked_mul(512)
                .ok_or_else(|| AhrbError::Protocol("storage allocation overflow".into()))?,
            apparent_bytes: initial.size(),
            sha256,
            family,
        });
        if kind == libc::S_IFDIR {
            walk(&file, &path, device, config, digest, seen, inventory)?;
        }
        let final_metadata = file.metadata()?;
        let mut final_stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        if unsafe {
            libc::fstatat(
                directory.as_raw_fd(),
                name.as_ptr(),
                final_stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } != 0
        {
            return Err(capture_io(std::io::Error::last_os_error(), &path, config));
        }
        let final_stat = unsafe { final_stat.assume_init() };
        let final_identity = (final_stat.st_dev as u64, final_stat.st_ino);
        if final_stat.st_mode & libc::S_IFMT != kind
            || final_identity.0 != device
            || (final_identity != identity && seen.contains(&final_identity))
        {
            return Err(AhrbError::Protocol(format!(
                "storage kind/device escape or repeated identity during capture at {path}"
            )));
        }
        if signature(&initial) != signature(&final_metadata)
            || (final_stat.st_dev as u64, final_stat.st_ino) != identity
            || final_stat.st_size as u64 != initial.size()
            || final_stat.st_blocks as u64 != initial.blocks()
        {
            return Err(capture_changed(&path, config));
        }
    }
    if signature(&before) != signature(&directory.metadata()?) {
        return Err(capture_changed(prefix, config));
    }
    Ok(())
}

/// The workspace must already be a validated child of this root; a single
/// exhaustive root walk collapses that nested union without deduplicating files.
#[cfg(unix)]
pub fn inventory(root: &Path, config: &StorageConfig, digest: bool) -> Result<Inventory> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(root)?;
    let initial = file.metadata()?;
    let mut seen = BTreeSet::from([(initial.dev(), initial.ino())]);
    let mut out = Inventory::default();
    walk(
        &file,
        "",
        initial.dev(),
        config,
        digest,
        &mut seen,
        &mut out,
    )?;
    if signature(&initial) != signature(&std::fs::symlink_metadata(root)?) {
        return Err(AhrbError::Protocol(
            "storage root changed during capture".into(),
        ));
    }
    out.entries.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

#[cfg(not(unix))]
pub fn inventory(_root: &Path, _config: &StorageConfig, _digest: bool) -> Result<Inventory> {
    Err(AhrbError::Unsupported(
        "storage stat-st_blocks-512 unavailable on this platform".into(),
    ))
}

pub struct SettledInventory {
    pub inventory: Inventory,
    pub settle_ms: f64,
    pub sync_start_ns: u64,
    pub sync_end_ns: u64,
}

/// No stimulus is submitted during this minimum 2s settle and 100ms probe.
pub async fn settle(
    root: &Path,
    config: &StorageConfig,
    boundary_ns: u64,
) -> Result<SettledInventory> {
    let elapsed = crate::fake_model::monotonic_timestamp_ns().saturating_sub(boundary_ns);
    let remaining = 10_000_000_000_u64.checked_sub(elapsed).ok_or_else(|| {
        AhrbError::Protocol("storage boundary already exceeded maximum settle".into())
    })?;
    let limit = tokio::time::Instant::now() + Duration::from_nanos(remaining);
    tokio::time::sleep(Duration::from_nanos(
        2_000_000_000_u64.saturating_sub(elapsed),
    ))
    .await;
    let sync_start_ns = crate::fake_model::monotonic_timestamp_ns();
    let status = tokio::time::timeout_at(
        limit,
        tokio::process::Command::new("/bin/sync")
            .kill_on_drop(true)
            .status(),
    )
    .await
    .map_err(|_| AhrbError::Protocol("storage sync exceeded maximum settle".into()))??;
    let sync_end_ns = crate::fake_model::monotonic_timestamp_ns();
    if !status.success() {
        return Err(AhrbError::Protocol("storage sync failed".into()));
    }
    let inventory = settle_capture(limit, || capture_attempt(root, config)).await?;
    Ok(SettledInventory {
        inventory,
        settle_ms: crate::fake_model::monotonic_timestamp_ns().saturating_sub(boundary_ns) as f64
            / 1e6,
        sync_start_ns,
        sync_end_ns,
    })
}

async fn capture_attempt(root: &Path, config: &StorageConfig) -> Result<Option<Inventory>> {
    let before = inventory(root, config, false)?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let after = inventory(root, config, false)?;
    if before != after {
        return Ok(None);
    }
    let captured = inventory(root, config, true)?;
    let mut metadata = captured.clone();
    for entry in &mut metadata.entries {
        entry.sha256 = None;
    }
    if metadata != after {
        let changed = changed_paths(&metadata, &after);
        if changed.iter().any(|path| {
            config
                .family(path)
                .ok()
                .is_none_or(|family| family != "transient")
        }) {
            return Err(AhrbError::Protocol(format!(
                "storage undeclared paths changed between stability probe and digest: {changed:?}"
            )));
        }
        return Err(
            std::io::Error::other(TransientCaptureChange(changed.into_iter().collect())).into(),
        );
    }
    Ok(Some(captured))
}

async fn settle_capture<F, Fut>(limit: tokio::time::Instant, mut attempt: F) -> Result<Inventory>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<Inventory>>>,
{
    let mut retries = Vec::new();
    while tokio::time::Instant::now() < limit {
        match attempt().await {
            Ok(Some(mut captured)) if tokio::time::Instant::now() <= limit => {
                captured.capture_retries = retries;
                return Ok(captured);
            }
            Err(error) => {
                let Some(paths) = transient_retry(&error) else {
                    return Err(if retries.is_empty() {
                        error
                    } else {
                        AhrbError::Protocol(format!(
                            "storage capture failed: {error}; transient capture attempts={retries:?}"
                        ))
                    });
                };
                retries.push(paths);
                if retries.len() >= 3 {
                    return Err(AhrbError::Protocol(format!(
                        "storage transient re-inventory exhausted after 3 capture attempts: {retries:?}"
                    )));
                }
            }
            _ => {}
        }
    }
    Err(AhrbError::Protocol(format!(
        "storage failed to settle within 10000 ms; transient capture attempts={retries:?}"
    )))
}

/// Deterministic retry injection followed by an actual fatal filesystem capture.
#[cfg(all(test, unix))]
pub(crate) async fn transient_then_fatal_fixture(transient_attempts: usize) -> AhrbError {
    static NEXT_FIXTURE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "ahrb-transient-fatal-{}-{}",
        std::process::id(),
        NEXT_FIXTURE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(root.join("state")).unwrap();
    std::os::unix::fs::symlink("missing", root.join("state/link")).unwrap();
    let config = StorageConfig {
        areas: Some(BTreeMap::from([(
            "transient".into(),
            vec!["cache/**".into()],
        )])),
        ..Default::default()
    };
    let mut attempts = 0;
    let result = settle_capture(
        tokio::time::Instant::now() + Duration::from_secs(10),
        || {
            attempts += 1;
            std::future::ready(if attempts <= transient_attempts {
                Err(capture_changed(&format!("cache/pack-{attempts}"), &config))
            } else {
                inventory(&root, &config, true).map(Some)
            })
        },
    )
    .await;
    std::fs::remove_dir_all(root).unwrap();
    assert_eq!(attempts, transient_attempts + 1);
    result.unwrap_err()
}

#[cfg(test)]
mod transient_tests {
    use super::*;
    #[test]
    fn retry_policy_requires_declared_transient_and_a_capture_change() {
        let config = StorageConfig {
            areas: Some(BTreeMap::from([(
                "transient".into(),
                vec!["cache/**".into()],
            )])),
            ..Default::default()
        };
        assert_eq!(
            transient_retry(&capture_changed("cache/pack", &config)),
            Some(vec!["cache/pack".into()])
        );
        for path in ["", "state/journal", "cache-sibling/pack"] {
            assert!(transient_retry(&capture_changed(path, &config)).is_none());
        }
        assert!(
            transient_retry(&capture_changed("cache/pack", &StorageConfig::default())).is_none()
        );
        assert!(
            transient_retry(&capture_io(
                std::io::Error::from(std::io::ErrorKind::PermissionDenied),
                "cache/pack",
                &config
            ))
            .is_none()
        );
        assert!(
            transient_retry(&AhrbError::Protocol(
                "storage symlink/escape at cache/pack".into()
            ))
            .is_none()
        );
    }
}

#[cfg(test)]
mod capture_change_tests {
    use super::*;
    #[test]
    fn comparison_includes_every_path_and_timestamp_only_changes() {
        let mut before = Inventory::default();
        before.stamps.insert("cache/pack".into(), (1, 0, 1, 0));
        before.stamps.insert("state/journal".into(), (1, 0, 1, 0));
        let mut after = before.clone();
        after.stamps.insert("cache/pack".into(), (2, 0, 2, 0));
        assert_eq!(
            changed_paths(&before, &after),
            BTreeSet::from(["cache/pack".into()])
        );
        after.stamps.insert("state/journal".into(), (2, 0, 2, 0));
        assert_eq!(
            changed_paths(&before, &after),
            BTreeSet::from(["cache/pack".into(), "state/journal".into()])
        );
    }
}

#[cfg(test)]
mod retry_boundary_tests {
    use super::*;
    #[cfg(unix)]
    #[tokio::test]
    async fn transient_then_fatal_capture_preserves_every_failed_path() {
        for transient_attempts in 1..=2 {
            let error = transient_then_fatal_fixture(transient_attempts).await;
            let reason = error.to_string();
            for attempt in 1..=transient_attempts {
                assert!(
                    reason.contains(&format!("cache/pack-{attempt}")),
                    "{reason}"
                );
            }
            assert!(reason.contains("state/link"), "{reason}");
            assert!(reason.contains("symlink/escape"), "{reason}");
            assert!(transient_retry(&error).is_none());
        }
    }

    #[tokio::test]
    async fn bounded_reinventory_preserves_receipts_and_requires_complete_final_capture() {
        let config = StorageConfig {
            areas: Some(BTreeMap::from([(
                "transient".into(),
                vec!["cache/**".into()],
            )])),
            ..Default::default()
        };
        let mut attempts = 0;
        let captured = settle_capture(tokio::time::Instant::now() + Duration::from_secs(1), || {
            attempts += 1;
            std::future::ready(if attempts < 3 {
                Err(capture_changed("cache/pack", &config))
            } else {
                Ok(Some(Inventory::default()))
            })
        })
        .await
        .unwrap();
        assert_eq!(attempts, 3);
        assert_eq!(
            captured.capture_retries,
            vec![vec!["cache/pack".to_string()]; 2]
        );
        attempts = 0;
        let error = settle_capture(tokio::time::Instant::now() + Duration::from_secs(1), || {
            attempts += 1;
            std::future::ready(Err(capture_changed("cache/pack", &config)))
        })
        .await
        .unwrap_err();
        assert_eq!(attempts, 3);
        assert!(error.to_string().contains("exhausted"));
        attempts = 0;
        let error = settle_capture(tokio::time::Instant::now() - Duration::from_secs(1), || {
            attempts += 1;
            std::future::ready(Ok(Some(Inventory::default())))
        })
        .await
        .unwrap_err();
        assert_eq!(attempts, 0);
        assert!(error.to_string().contains("10000 ms"));
        attempts = 0;
        let error = settle_capture(tokio::time::Instant::now() + Duration::from_secs(1), || {
            attempts += 1;
            std::future::ready(Err(capture_changed("state/journal", &config)))
        })
        .await
        .unwrap_err();
        assert_eq!(attempts, 1);
        assert!(error.to_string().contains("state/journal"));
    }
}

/// Read bytes from the exact audited regular-file identity using held no-follow
/// parent descriptors. Recheck metadata and content digest before accepting them.
#[cfg(unix)]
pub fn read_verified(root: &Path, entry: &FileEntry) -> Result<Vec<u8>> {
    if entry.kind != "regular" {
        return Err(AhrbError::Protocol("S8 nonregular content read".into()));
    }
    let mut parent = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(root)?;
    let parts = entry.path.split('/').collect::<Vec<_>>();
    for (index, part) in parts.iter().enumerate() {
        if part.is_empty() || *part == "." || *part == ".." {
            return Err(AhrbError::Protocol("S8 invalid relative path".into()));
        }
        let name = std::ffi::CString::new(*part).map_err(|e| AhrbError::Protocol(e.to_string()))?;
        let mut file = open_child(&parent, &name, index + 1 < parts.len())?;
        if index + 1 < parts.len() {
            parent = file;
            continue;
        }
        let before = file.metadata()?;
        if !before.is_file()
            || (
                before.dev(),
                before.ino(),
                before.size(),
                before.blocks() * 512,
            ) != (
                entry.device_id,
                entry.inode_or_file_id,
                entry.apparent_bytes,
                entry.allocated_bytes,
            )
        {
            return Err(AhrbError::Protocol(
                "S8 changed file identity/size/blocks".into(),
            ));
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let reopened = open_child(&parent, &name, false)?;
        if signature(&before) != signature(&file.metadata()?)
            || signature(&before) != signature(&reopened.metadata()?)
            || entry.sha256.as_deref() != Some(format!("{:x}", Sha256::digest(&bytes)).as_str())
        {
            return Err(AhrbError::Protocol("S8 changed content during scan".into()));
        }
        return Ok(bytes);
    }
    Err(AhrbError::Protocol("S8 empty path".into()))
}
#[cfg(not(unix))]
pub fn read_verified(_root: &Path, _entry: &FileEntry) -> Result<Vec<u8>> {
    Err(AhrbError::Unsupported(
        "S8 no-follow content audit unavailable".into(),
    ))
}
