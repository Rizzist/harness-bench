//! Optional out-of-band storage receipts for a declared resume control process.
use super::*;
use crate::process::{ProcessDiskObservation, TreeDiskTracker};
use crate::storage::evidence::IdentityReceipt;

#[derive(Clone, Debug, Serialize)]
pub struct StorageControlReceipt {
    pub launch_ns: u64,
    pub exit_ns: u64,
    pub exit_code: Option<i32>,
    pub json: Value,
    pub identities: Vec<IdentityReceipt>,
}

pub(super) async fn collect(
    command: &mut Command,
    timeout: Duration,
) -> Result<(std::process::Output, StorageControlReceipt)> {
    let launch_ns = crate::fake_model::monotonic_timestamp_ns();
    let mut child = command.spawn()?;
    crate::process::register_child(&child)?;
    let pid = child
        .id()
        .ok_or_else(|| AhrbError::Protocol("control launch PID absent".into()))?;
    let mut stdout = BufReader::new(
        child
            .stdout
            .take()
            .ok_or_else(|| AhrbError::Protocol("control stdout absent".into()))?,
    );
    let stderr = child.stderr.take();
    let mut sampler = driver_platform_sampler();
    let mut tracker = TreeDiskTracker::default();
    let mut first = BTreeMap::new();
    let collect = async {
        let mut bytes = Vec::new();
        let mut line = Vec::new();
        let json = loop {
            let tree = sampler.discover(&[pid])?;
            if !tree.members.keys().any(|id| id.pid == pid) {
                return Err(AhrbError::Protocol(
                    "missing control launch identity".into(),
                ));
            }
            let mut reads = ProcessDiskObservation {
                expected_identities: tree.members.keys().copied().collect(),
                ..Default::default()
            };
            for id in &reads.expected_identities {
                if let Some(value) = sampler.disk_read_counter_for_identity(*id)? {
                    reads.write_bytes_by_identity.insert(*id, value);
                    first.entry(*id).or_insert(0);
                }
            }
            tracker.observe(&reads)?;
            tokio::select! {
                count=stdout.read_until(b'\n',&mut line)=>{
                    if count?==0 {return Err(AhrbError::Protocol("control exited without a pre-reap JSON terminal receipt".into()));}
                    bytes.extend_from_slice(&line);
                    if let Ok(value)=serde_json::from_slice::<Value>(&line) {break value;}
                    line.clear();
                }
                _=tokio::time::sleep(Duration::from_millis(1))=>{}
            }
        };
        // A JSON result is the public control completion. Never replace this
        // post-result sample with a last-live poll if the process was too fast.
        let final_tree = sampler.discover(&[pid])?;
        let mut final_observation = ProcessDiskObservation {
            expected_identities: final_tree.members.keys().copied().collect(),
            ..Default::default()
        };
        for id in &final_observation.expected_identities {
            if let Some(value) = sampler.disk_read_counter_for_identity(*id)? {
                final_observation.write_bytes_by_identity.insert(*id, value);
                first.entry(*id).or_insert(0);
            }
        }
        tracker.observe(&final_observation)?;
        let snapshot = tracker.snapshot();
        for r in &snapshot.identities {
            tracker.note_structured_terminal(r.identity)?;
            let value = sampler
                .disk_read_counter_for_identity(r.identity)?
                .ok_or_else(|| {
                    AhrbError::Protocol("missing control terminal-before-reap read receipt".into())
                })?;
            tracker.record_final_sample_before_reap(r.identity, value)?;
            tracker.retire_after_final_sample(r.identity)?;
        }
        let final_reads = tracker.snapshot();
        if !final_reads.counter_complete {
            return Err(AhrbError::Protocol("incomplete control read tree".into()));
        }
        let mut tail = Vec::new();
        stdout.read_to_end(&mut tail).await?;
        if !tail.iter().all(u8::is_ascii_whitespace) {
            return Err(AhrbError::Protocol(
                "control output continued after the sampled JSON receipt".into(),
            ));
        }
        bytes.extend_from_slice(&tail);
        let status = child.wait().await?;
        let exit_ns = crate::fake_model::monotonic_timestamp_ns();
        crate::process::retire_process(pid)?;
        let identities = final_reads
            .identities
            .into_iter()
            .map(|r| IdentityReceipt {
                pid: r.identity.pid,
                start_time: r.identity.start_time,
                source: read_source().into(),
                first_bytes: first.get(&r.identity).copied(),
                last_bytes: r.write_bytes,
                retirement_method: format!("{:?}", r.status),
                complete: true,
            })
            .collect();
        Ok::<_, AhrbError>((
            status,
            bytes,
            StorageControlReceipt {
                launch_ns,
                exit_ns,
                exit_code: status.code(),
                json,
                identities,
            },
        ))
    };
    let ((status, stdout, receipt), stderr) = tokio::time::timeout(timeout, async {
        tokio::try_join!(collect, async {
            read_owned_pipe(stderr).await.map_err(AhrbError::from)
        })
    })
    .await
    .map_err(|_| AhrbError::Timeout("storage resume control".into()))??;
    Ok((
        std::process::Output {
            status,
            stdout,
            stderr,
        },
        receipt,
    ))
}

pub fn read_source() -> &'static str {
    if cfg!(target_os = "macos") {
        "macos-ri_diskio_bytesread"
    } else if cfg!(target_os = "linux") {
        "linux-proc-pid-io-read_bytes"
    } else {
        "unavailable"
    }
}
