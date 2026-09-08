//! S7 consumes the shared, exhaustive S3 checkpoints; rotation never implies a cap.
use super::{accounting::Inventory, evidence::*, *};
use crate::evaluate::TestOutcome;

pub fn evaluate(
    repetition: u32,
    n: u32,
    snapshots: &[(u32, Inventory)],
    config: &StorageConfig,
) -> Result<Trial<AuxiliarySummary, AuxiliaryDiagnostics>> {
    if snapshots.iter().map(|(t, _)| *t).collect::<Vec<_>>() != checkpoints(n) {
        return Err(AhrbError::Protocol("S7 missing checkpoints".into()));
    }
    let diagnostics = checkpoint_diagnostics(snapshots, config);
    let families = snapshots[0].1.families(config);
    let mut auxiliaries = Vec::new();
    for name in families.keys() {
        let family = diagnostics
            .checkpoints
            .iter()
            .filter(|p| p.family == *name)
            .collect::<Vec<_>>();
        let points = family
            .iter()
            .map(|p| Checkpoint {
                turn: p.turn,
                allocated_bytes: p.allocated_bytes,
            })
            .collect::<Vec<_>>();
        let rotation = family.iter().any(|p| p.identity_replacements > 0);
        let cap = config
            .auxiliary_cap_bytes
            .as_ref()
            .and_then(|c| c.get(name))
            .copied();
        let peak = points.iter().map(|p| p.allocated_bytes).max().unwrap_or(0);
        let (_, slope, _) = evaluate_curve(&points, n)?;
        auxiliaries.push(Auxiliary {
            name: name.clone(),
            declared: config.areas.as_ref().is_some_and(|a| a.contains_key(name)),
            cap_bytes: cap,
            peak_allocated_bytes: peak,
            final_allocated_bytes: points.last().unwrap().allocated_bytes,
            slope_bytes_per_turn: slope,
            rotation_observed: rotation,
            class: cap.map(|c| {
                if peak <= c {
                    BoundClass::Bounded
                } else {
                    BoundClass::Unbounded
                }
            }),
        });
    }
    let reason = auxiliaries.iter().any(|a| a.cap_bytes.is_none()).then(|| {
        "unsupported cap assessment: one or more families have no declared cap".to_owned()
    });
    Ok(Trial {
        repetition,
        outcome: reason
            .clone()
            .map_or(TestOutcome::Pass, TestOutcome::Unsupported),
        measurement_complete: true,
        reason,
        summary: AuxiliarySummary { auxiliaries },
        diagnostics,
        evidence_refs: Vec::new(),
    })
}

pub fn aggregate(trials: &[Trial<AuxiliarySummary, AuxiliaryDiagnostics>]) -> Vec<Auxiliary> {
    let Some(first) = trials.first() else {
        return Vec::new();
    };
    first
        .summary
        .auxiliaries
        .iter()
        .map(|a| {
            let family = trials
                .iter()
                .flat_map(|t| &t.summary.auxiliaries)
                .filter(|f| f.name == a.name)
                .collect::<Vec<_>>();
            let mut out = a.clone();
            out.peak_allocated_bytes = family
                .iter()
                .map(|f| f.peak_allocated_bytes)
                .max()
                .unwrap_or(0);
            out.final_allocated_bytes = family
                .iter()
                .map(|f| f.final_allocated_bytes)
                .max()
                .unwrap_or(0);
            out.slope_bytes_per_turn =
                median(family.iter().map(|f| f.slope_bytes_per_turn).collect()).unwrap_or(0.0);
            out.rotation_observed = family.iter().any(|f| f.rotation_observed);
            out.class = out.cap_bytes.map(|cap| {
                if out.peak_allocated_bytes <= cap {
                    BoundClass::Bounded
                } else {
                    BoundClass::Unbounded
                }
            });
            out
        })
        .collect()
}

/// The same measured identity comparison is retained for interrupted horizons.
pub fn checkpoint_diagnostics(
    snapshots: &[(u32, Inventory)],
    config: &StorageConfig,
) -> AuxiliaryDiagnostics {
    let mut diagnostics = AuxiliaryDiagnostics::default();
    let mut previous = BTreeMap::new();
    for (turn, inventory) in snapshots {
        let current = inventory
            .entries
            .iter()
            .filter(|e| e.kind == "regular")
            .map(|e| (e.path.clone(), (e.device_id, e.inode_or_file_id)))
            .collect::<BTreeMap<_, _>>();
        for (family, bytes) in inventory.families(config) {
            let replacements = inventory
                .entries
                .iter()
                .filter(|e| {
                    e.kind == "regular"
                        && e.family == family
                        && previous
                            .get(&e.path)
                            .is_some_and(|id| *id != (e.device_id, e.inode_or_file_id))
                })
                .count() as u64;
            diagnostics.checkpoints.push(FamilyCheckpoint {
                turn: *turn,
                family,
                allocated_bytes: bytes,
                identity_replacements: replacements,
            });
        }
        previous = current;
    }
    diagnostics
}
