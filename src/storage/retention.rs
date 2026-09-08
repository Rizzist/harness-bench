//! S8 exact byte observations, independent of allocated growth and filenames.
use super::{
    accounting::{self, Inventory},
    evidence::*,
    *,
};
use crate::{evaluate::TestOutcome, fake_model::ModelRequestRecord};
use std::path::Path;

pub fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[derive(Clone)]
pub struct Needle {
    pub bytes: Vec<u8>,
    pub representation: &'static str,
    pub request: Option<String>,
    pub block: Option<String>,
}

/// The checked union counts overlapping representations once, separate copies again.
pub fn union_length(ranges: &[(u64, u64)]) -> Result<u64> {
    let mut ranges = ranges.to_vec();
    ranges.sort_unstable();
    let (mut total, mut end) = (0_u64, 0_u64);
    for (start, length) in ranges {
        let next = start
            .checked_add(length)
            .ok_or_else(|| AhrbError::Protocol("S8 range overflow".into()))?;
        total = total
            .checked_add(next.saturating_sub(end.max(start)))
            .ok_or_else(|| AhrbError::Protocol("S8 union overflow".into()))?;
        end = end.max(next);
    }
    Ok(total)
}

pub fn classify(
    stored: u64,
    unique: u64,
    all_bodies: bool,
    all_blocks: bool,
    limited: bool,
) -> (RetentionSummary, Coverage, Option<String>) {
    let (class, coverage, reason) = if all_bodies {
        (
            Some(RetentionClass::Full),
            if limited {
                Coverage::RepresentationLimited
            } else {
                Coverage::Complete
            },
            None,
        )
    } else if limited {
        (
            None,
            Coverage::RepresentationLimited,
            Some(
                "representation-limited: non-text bytes without a verified lossless decoder".into(),
            ),
        )
    } else if all_blocks && unique > 0 && stored <= unique {
        (Some(RetentionClass::Deduplicated), Coverage::Complete, None)
    } else if stored == 0 {
        (Some(RetentionClass::None), Coverage::Complete, None)
    } else {
        (
            None,
            Coverage::Partial,
            Some("partial-retention-unclassified".into()),
        )
    };
    (
        RetentionSummary {
            request_retention_class: class,
            stored_request_bytes: Some(stored),
            unique_request_content_bytes: Some(unique),
            stored_unique_ratio: (unique > 0).then(|| stored as f64 / unique as f64),
        },
        coverage,
        reason,
    )
}

pub fn baseline_bytes(root: &Path, inventory: &Inventory) -> Result<BTreeMap<String, Vec<u8>>> {
    inventory
        .entries
        .iter()
        .filter(|e| e.kind == "regular")
        .map(|e| Ok((e.path.clone(), accounting::read_verified(root, e)?)))
        .collect()
}

fn unchanged_ranges(before: &[u8], after: &[u8]) -> Vec<(u64, u64)> {
    let mut ranges = Vec::new();
    let mut start = None;
    for i in 0..before.len().min(after.len()) {
        if before[i] == after[i] {
            start.get_or_insert(i);
        } else if let Some(s) = start.take() {
            ranges.push((s as u64, (i - s) as u64));
        }
    }
    if let Some(s) = start {
        ranges.push((s as u64, (before.len().min(after.len()) - s) as u64));
    }
    ranges
}

pub fn scan_file(
    repetition: u32,
    entry: &accounting::FileEntry,
    bytes: &[u8],
    excluded: &[(u64, u64)],
    needles: &[Needle],
) -> Vec<BodyMatch> {
    let mut matches = Vec::new();
    let first_bytes = needles
        .iter()
        .filter_map(|n| n.bytes.first().copied())
        .collect::<BTreeSet<_>>();
    let mut positions = BTreeMap::<u8, Vec<usize>>::new();
    for (offset, byte) in bytes.iter().enumerate() {
        if first_bytes.contains(byte) {
            positions.entry(*byte).or_default().push(offset);
        }
    }
    for needle in needles.iter().filter(|n| !n.bytes.is_empty()) {
        for offset in positions
            .get(&needle.bytes[0])
            .into_iter()
            .flatten()
            .copied()
        {
            let Some(window) = bytes.get(offset..offset.saturating_add(needle.bytes.len())) else {
                continue;
            };
            if window != needle.bytes {
                continue;
            }
            let start = offset as u64;
            let length = needle.bytes.len() as u64;
            let baseline = excluded
                .iter()
                .any(|(s, n)| start < s + n && start + length > *s);
            let receipt = BodyMatch {
                repetition,
                representation: needle.representation.into(),
                request_sha256: needle.request.clone(),
                block_sha256: needle.block.clone(),
                path: entry.path.clone(),
                device_id: entry.device_id,
                inode_or_file_id: entry.inode_or_file_id,
                offset_bytes: start,
                length_bytes: length,
                excluded_baseline: baseline,
            };
            matches.push(receipt.clone());
            if baseline {
                // Keep the complete excluded match as a receipt, and count only
                // its nonbaseline subranges. Fragments cannot prove full coverage.
                let mut cuts = vec![start, start + length];
                for (s, n) in excluded {
                    cuts.push((*s).clamp(start, start + length));
                    cuts.push((s + n).clamp(start, start + length));
                }
                cuts.sort_unstable();
                cuts.dedup();
                for pair in cuts.windows(2) {
                    if !excluded
                        .iter()
                        .any(|(s, n)| pair[0] >= *s && pair[0] < s + n)
                    {
                        let mut fragment = receipt.clone();
                        fragment.offset_bytes = pair[0];
                        fragment.length_bytes = pair[1] - pair[0];
                        fragment.excluded_baseline = false;
                        fragment.representation.push_str("-fragment");
                        matches.push(fragment);
                    }
                }
            }
        }
    }
    matches
}

#[allow(clippy::too_many_arguments)]
pub fn collect(
    repetition: u32,
    root: &Path,
    config: &StorageConfig,
    baseline: &Inventory,
    baseline_content: &BTreeMap<String, Vec<u8>>,
    final_inventory: &Inventory,
    fixture_paths: &BTreeSet<String>,
    records: &[ModelRequestRecord],
    output: &Path,
) -> Result<(
    Trial<RetentionSummary, RetentionDiagnostics>,
    Vec<BodyMatch>,
)> {
    let mapping = std::fs::read_to_string(output.join("storage-request-bodies.jsonl"))?;
    let receipts = mapping
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let receipts = receipts
        .iter()
        .filter(|r| r["repetition"].as_u64() == Some(repetition as u64))
        .collect::<Vec<_>>();
    if records.is_empty() || receipts.len() != records.len() {
        return Err(AhrbError::Protocol(
            "S8 capture-error: missing physical request body capture".into(),
        ));
    }
    let mut needles = Vec::new();
    let mut blocks = BTreeMap::new();
    let mut requests = BTreeSet::new();
    let mut blobs = Vec::new();
    for record in records {
        let matches = receipts
            .iter()
            .filter(|r| r["received_ns"].as_u64() == Some(record.received_ns))
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            return Err(AhrbError::Protocol(
                "S8 capture-error: ambiguous/missing request mapping".into(),
            ));
        }
        let receipt = matches[0];
        let hash = receipt["raw_sha256"]
            .as_str()
            .ok_or_else(|| AhrbError::Protocol("S8 capture-error: missing hash".into()))?;
        if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(AhrbError::Protocol(
                "S8 capture-error: invalid digest".into(),
            ));
        }
        let path = format!("request-bodies/{hash}.bin");
        let raw = std::fs::read(output.join(&path))?;
        if digest(&raw) != hash || raw.len() as u64 != record.body_bytes {
            return Err(AhrbError::Protocol(
                "S8 capture-error: truncated/changed request body".into(),
            ));
        }
        let canonical = serde_json::to_vec(&record.request.canonical)?;
        let canonical_hash = digest(&canonical);
        requests.insert(hash.to_owned());
        needles.push(Needle {
            bytes: raw,
            representation: "raw-body",
            request: Some(hash.into()),
            block: None,
        });
        needles.push(Needle {
            bytes: canonical,
            representation: "canonical-body",
            request: Some(hash.into()),
            block: None,
        });
        for block in crate::economy::message_blocks(record) {
            let bytes = serde_json::to_vec(&block)?;
            blocks.insert(digest(&bytes), bytes);
        }
        blobs.push(BodyBlob {
            semantic_ordinal: record.semantic_ordinal,
            attempt: record.attempt,
            role: record.role.clone(),
            raw_sha256: hash.into(),
            canonical_sha256: canonical_hash,
            path,
        });
    }
    let unique = blocks
        .values()
        .try_fold(0_u64, |n, b| n.checked_add(b.len() as u64))
        .ok_or_else(|| AhrbError::Protocol("S8 unique byte overflow".into()))?;
    for (hash, bytes) in &blocks {
        needles.push(Needle {
            bytes: bytes.clone(),
            representation: "canonical-block",
            request: None,
            block: Some(hash.clone()),
        });
    }
    // Deduplicate raw needles while retaining the link of each distinct request to
    // its canonical representation (raw encodings can differ).
    let mut seen = BTreeSet::new();
    needles.retain(|n| {
        seen.insert((
            n.representation,
            n.request.clone(),
            n.block.clone(),
            digest(&n.bytes),
        ))
    });
    let mut matches = Vec::new();
    let mut exclusions = Vec::new();
    let mut limited = false;
    let audit = (|| -> Result<()> {
        for entry in final_inventory
            .entries
            .iter()
            .filter(|e| e.kind == "regular")
        {
            let bytes = accounting::read_verified(root, entry)?;
            let old = baseline.entries.iter().find(|b| {
                b.path == entry.path
                    && b.device_id == entry.device_id
                    && b.inode_or_file_id == entry.inode_or_file_id
            });
            let excluded = if fixture_paths.contains(&entry.path) {
                vec![(0, bytes.len() as u64)]
            } else if old.is_some() {
                baseline_content
                    .get(&entry.path)
                    .map(|b| unchanged_ranges(b, &bytes))
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            for (s, n) in &excluded {
                if *n > 0 {
                    exclusions.push(BaselineExclusion {
                        path: entry.path.clone(),
                        offset_bytes: *s,
                        length_bytes: *n,
                        sha256: digest(&bytes[*s as usize..(s + n) as usize]),
                    });
                }
            }
            // Audit supported text representations; any changed binary data makes
            // coverage explicitly limited. Unchanged baseline files are excluded.
            if union_length(&excluded)? < bytes.len() as u64
                && (std::str::from_utf8(&bytes).is_err() || bytes.contains(&0))
            {
                limited = true;
            }
            matches.extend(scan_file(repetition, entry, &bytes, &excluded, &needles));
        }
        if &accounting::inventory(root, config, true)? != final_inventory {
            return Err(AhrbError::Protocol(
                "S8 changed root during content scan".into(),
            ));
        }
        Ok(())
    })();
    if let Err(error) = audit {
        let reason = format!("S8 incomplete content audit: {error}");
        return Ok((
            Trial {
                repetition,
                outcome: TestOutcome::Error(reason.clone()),
                measurement_complete: false,
                reason: Some(reason),
                summary: RetentionSummary::default(),
                diagnostics: RetentionDiagnostics {
                    coverage: Coverage::CaptureError,
                    body_blobs: blobs,
                    baseline_exclusions: exclusions,
                    match_refs: Vec::new(),
                },
                evidence_refs: Vec::new(),
            },
            matches,
        ));
    }
    let mut ranges = BTreeMap::<(u64, u64), Vec<(u64, u64)>>::new();
    let mut found_requests = BTreeSet::new();
    let mut found_blocks = BTreeSet::new();
    for m in matches.iter().filter(|m| !m.excluded_baseline) {
        ranges
            .entry((m.device_id, m.inode_or_file_id))
            .or_default()
            .push((m.offset_bytes, m.length_bytes));
        if let Some(h) = &m.request_sha256
            && !m.representation.ends_with("-fragment")
        {
            found_requests.insert(h.clone());
        }
        if let Some(h) = &m.block_sha256
            && !m.representation.ends_with("-fragment")
        {
            found_blocks.insert(h.clone());
        }
    }
    let stored = ranges
        .values()
        .try_fold(0_u64, |total, r| total.checked_add(union_length(r).ok()?))
        .ok_or_else(|| AhrbError::Protocol("S8 stored byte overflow".into()))?;
    let (summary, coverage, reason) = classify(
        stored,
        unique,
        requests.is_subset(&found_requests),
        !blocks.is_empty() && blocks.keys().all(|b| found_blocks.contains(b)),
        limited,
    );
    Ok((
        Trial {
            repetition,
            outcome: reason
                .clone()
                .map_or(TestOutcome::Pass, TestOutcome::Unsupported),
            measurement_complete: true,
            reason,
            summary,
            diagnostics: RetentionDiagnostics {
                coverage,
                body_blobs: blobs,
                baseline_exclusions: exclusions,
                match_refs: Vec::new(),
            },
            evidence_refs: Vec::new(),
        },
        matches,
    ))
}

pub fn aggregate(
    trials: &[Trial<RetentionSummary, RetentionDiagnostics>],
) -> Result<RetentionSummary> {
    if trials.is_empty() || trials.iter().any(|t| !t.measurement_complete) {
        return Ok(RetentionSummary::default());
    }
    let sum = |f: fn(&RetentionSummary) -> Option<u64>| {
        trials
            .iter()
            .try_fold(0_u64, |n, t| n.checked_add(f(&t.summary)?))
    };
    let stored = sum(|s| s.stored_request_bytes)
        .ok_or_else(|| AhrbError::Protocol("S8 missing/overflow stored total".into()))?;
    let unique = sum(|s| s.unique_request_content_bytes)
        .ok_or_else(|| AhrbError::Protocol("S8 missing/overflow unique total".into()))?;
    let class = trials[0].summary.request_retention_class;
    Ok(RetentionSummary {
        request_retention_class: class.filter(|c| {
            trials
                .iter()
                .all(|t| t.summary.request_retention_class == Some(*c))
        }),
        stored_request_bytes: Some(stored),
        unique_request_content_bytes: Some(unique),
        stored_unique_ratio: (unique > 0).then(|| stored as f64 / unique as f64),
    })
}
