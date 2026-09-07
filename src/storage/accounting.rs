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
            return Err(std::io::Error::last_os_error().into());
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
        let mut file = open_child(directory, &name, kind == libc::S_IFDIR)?;
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
        if (initial.dev(), initial.ino()) != identity
            || initial.size() != stat.st_size as u64
            || initial.blocks() != stat.st_blocks as u64
        {
            return Err(AhrbError::Protocol(format!(
                "storage identity/size/blocks changed at {path}"
            )));
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
            return Err(std::io::Error::last_os_error().into());
        }
        let final_stat = unsafe { final_stat.assume_init() };
        if signature(&initial) != signature(&final_metadata)
            || (final_stat.st_dev as u64, final_stat.st_ino) != identity
            || final_stat.st_size as u64 != initial.size()
            || final_stat.st_blocks as u64 != initial.blocks()
        {
            return Err(AhrbError::Protocol(format!(
                "storage changed during capture at {path}"
            )));
        }
    }
    if signature(&before) != signature(&directory.metadata()?) {
        return Err(AhrbError::Protocol(
            "storage directory changed during capture".into(),
        ));
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
    loop {
        let before = inventory(root, config, false)?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let after = inventory(root, config, false)?;
        if before == after {
            let captured = inventory(root, config, true)?;
            let mut metadata = captured.clone();
            for entry in &mut metadata.entries {
                entry.sha256 = None;
            }
            if metadata != after {
                return Err(AhrbError::Protocol(
                    "storage changed between stability probe and digest".into(),
                ));
            }
            if tokio::time::Instant::now() > limit {
                break;
            }
            return Ok(SettledInventory {
                inventory: captured,
                settle_ms: crate::fake_model::monotonic_timestamp_ns().saturating_sub(boundary_ns)
                    as f64
                    / 1e6,
                sync_start_ns,
                sync_end_ns,
            });
        }
        if tokio::time::Instant::now() >= limit {
            break;
        }
    }
    Err(AhrbError::Protocol(
        "storage failed to settle within 10000 ms".into(),
    ))
}
