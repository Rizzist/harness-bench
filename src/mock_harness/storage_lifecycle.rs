//! Public mock close policy and real allocated writes for storage lifecycle fixtures.
use super::*;

// Finish automatic expiry before storage's mandatory two-second quiet window
// ends. Longer per-session timers can expire during a later checkpoint digest.
// Keep this policy equal to the mock manifests' declared sweep interval.
const SWEEP_SECONDS: u64 = 1;

fn retention_mode() -> Result<String> {
    let mode =
        std::env::var("AHRB_MOCK_STORAGE_CLOSE_RETENTION").unwrap_or_else(|_| "capped".into());
    if !matches!(mode.as_str(), "capped" | "grow") {
        return Err(AhrbError::Usage(
            "AHRB_MOCK_STORAGE_CLOSE_RETENTION must be capped/grow".into(),
        ));
    }
    Ok(mode)
}

#[allow(clippy::zombie_processes)] // The short automatic worker outlives this public close command.
pub(super) fn close(args: &[String]) -> Result<i32> {
    let (config, id, _) = parse_session_control(args, false)?;
    validate_session_id(&id)?;
    let store = config.state_dir.join("sessions").join(&id);
    let meta: SessionMeta = serde_json::from_slice(&fs::read(store.join("meta.json"))?)?;
    if meta.id != id {
        return Err(AhrbError::Protocol(
            "close session metadata identity mismatch".into(),
        ));
    }
    let journal = DurableJournal::open(store.join("journal.jsonl"))?;
    if !journal
        .all()?
        .iter()
        .any(|e| e.event == EventVocab::TerminalSuccess)
    {
        return Err(AhrbError::Protocol(
            "close requires a completed session".into(),
        ));
    }
    let mode = retention_mode()?;
    let sweep = std::env::var("AHRB_MOCK_STORAGE_SWEEP").unwrap_or_else(|_| "on".into());
    if !matches!(sweep.as_str(), "on" | "off") {
        return Err(AhrbError::Usage(
            "AHRB_MOCK_STORAGE_SWEEP must be on/off".into(),
        ));
    }
    // Close never removes a journal, metadata or session directory. It seals the
    // session, retaining a byte cache which the documented automatic policy expires.
    write_replace_synced(
        &store.join("closed.json"),
        &serde_json::to_vec(&json!({"closed":true,"session_id":id}))?,
    )?;
    let bytes = if mode == "capped" { 262_144 } else { 524_288 };
    write_replace_synced(&store.join("closed-cache.bin"), &vec![b'C'; bytes])?;
    if sweep == "on" {
        // An ordinary child in this public command's owned process group: the
        // runner's existing retirement registry retains it until it exits. Never
        // daemonize or detach it from that group. stdout/stderr do not hold the CLI open.
        std::process::Command::new(std::env::current_exe()?)
            .args([
                "storage-auto-sweep",
                "--state-dir",
                &config.state_dir.to_string_lossy(),
                "--session-id",
                &id,
            ])
            .env("AHRB_MOCK_STORAGE_CLOSE_RETENTION", &mode)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
    }
    println!(
        "{}",
        json!({"closed":true,"deleted":false,"session_id":id,"sweep_interval_s":SWEEP_SECONDS})
    );
    Ok(0)
}

pub(super) async fn sweep(args: &[String]) -> Result<i32> {
    let (config, id, _) = parse_session_control(args, false)?;
    validate_session_id(&id)?;
    tokio::time::sleep(Duration::from_secs(SWEEP_SECONDS)).await;
    let file = OpenOptions::new().write(true).open(
        config
            .state_dir
            .join("sessions")
            .join(id)
            .join("closed-cache.bin"),
    )?;
    file.set_len(if retention_mode()? == "capped" {
        0
    } else {
        262_144
    })?;
    file.sync_all()?;
    Ok(0)
}

pub(super) fn compaction(config: &MockConfig, id: &str, after: bool) -> Result<()> {
    // Opt-in disk stimulus only; the existing matrix remains byte-for-byte default.
    let Ok(mode) = std::env::var("AHRB_MOCK_STORAGE_COMPACTION_DISK") else {
        return Ok(());
    };
    if !matches!(mode.as_str(), "reclaim" | "append") {
        return Err(AhrbError::Usage(
            "AHRB_MOCK_STORAGE_COMPACTION_DISK must be reclaim/append".into(),
        ));
    }
    let path = config
        .state_dir
        .join("sessions")
        .join(id)
        .join("compaction-cache.bin");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)?;
    if !after {
        file.write_all(&vec![b'B'; 1_048_576])?;
    } else if mode == "reclaim" {
        file.set_len(0)?;
    } else {
        use std::io::{Seek, SeekFrom};
        file.seek(SeekFrom::End(0))?;
        file.write_all(&vec![b'A'; 65_536])?;
    }
    file.sync_all()?;
    Ok(())
}
