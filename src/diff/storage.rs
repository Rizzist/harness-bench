//! Storage comparison keeps every numeric delta behind the full declared scope.
use super::*;
use crate::storage::evidence::StorageSummary;
use serde_json::{Value, json};

pub(super) fn same_scope(left: Option<&StorageSummary>, right: Option<&StorageSummary>) -> bool {
    let Some((a, b)) = left.zip(right) else {
        return false;
    };
    a.schema == 1
        && b.schema == 1
        && a.declarations_sha256.len() == 64
        && a.declarations_sha256.bytes().all(|b| b.is_ascii_hexdigit())
        && [
            (&a.task, &b.task),
            (&a.profile, &b.profile),
            (&a.os, &b.os),
            (&a.topology, &b.topology),
            (&a.allocation_source, &b.allocation_source),
            (&a.counter_source, &b.counter_source),
            (&a.declarations_sha256, &b.declarations_sha256),
            (&a.comparison_scope, &b.comparison_scope),
        ]
        .iter()
        .all(|(a, b)| known_scope_value(a) && *a != "unavailable" && a == b)
}

pub(super) fn compare(
    left: Option<&StorageSummary>,
    right: Option<&StorageSummary>,
) -> Result<Option<Value>> {
    if left.is_none() && right.is_none() {
        return Ok(None);
    }
    let comparable = same_scope(left, right);
    let status = if left.is_none() || right.is_none() {
        "unavailable"
    } else if comparable {
        "comparable"
    } else {
        "not-comparable-storage-scope"
    };
    let a = serde_json::to_value(left)?;
    let b = serde_json::to_value(right)?;
    let keys = a
        .as_object()
        .into_iter()
        .flat_map(|o| o.keys())
        .chain(b.as_object().into_iter().flat_map(|o| o.keys()))
        .collect::<BTreeSet<_>>();
    let mut values = serde_json::Map::new();
    for key in keys {
        let value = match key.as_str() {
            "footprint_curve" => join(&a[key], &b[key], "turn", comparable)?,
            "auxiliaries" => join(&a[key], &b[key], "name", comparable)?,
            _ => delta(&a[key], &b[key], comparable),
        };
        values.insert(key.clone(), value);
    }
    Ok(Some(json!({"comparison_scope":status, "values":values})))
}

fn delta(a: &Value, b: &Value, comparable: bool) -> Value {
    let mut value = json!({"before":a,"after":b});
    let change = if a.is_null() || b.is_null() {
        "unavailable"
    } else if !comparable {
        "not-comparable-storage-scope"
    } else if a == b {
        "unchanged"
    } else {
        "changed"
    };
    value["change"] = json!(change);
    if comparable && let Some((a, b)) = a.as_f64().zip(b.as_f64()) {
        value["delta"] = json!(b - a);
    }
    value
}

fn join(a: &Value, b: &Value, key: &str, comparable: bool) -> Result<Value> {
    let records = |v: &Value| -> Result<BTreeMap<String, Value>> {
        let mut out = BTreeMap::new();
        for value in v.as_array().into_iter().flatten() {
            let id = value.get(key).ok_or_else(|| {
                AhrbError::Protocol(format!("storage comparison record lacks {key}"))
            })?;
            if out.insert(id.to_string(), value.clone()).is_some() {
                return Err(AhrbError::Protocol(format!(
                    "storage comparison has duplicate {key}={id}"
                )));
            }
        }
        Ok(out)
    };
    let a = records(a)?;
    let b = records(b)?;
    let mut joined = a
        .keys()
        .chain(b.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|id| {
            let before = a.get(id).unwrap_or(&Value::Null);
            let after = b.get(id).unwrap_or(&Value::Null);
            let fields = before
                .as_object()
                .into_iter()
                .flat_map(|o| o.keys())
                .chain(after.as_object().into_iter().flat_map(|o| o.keys()))
                .collect::<BTreeSet<_>>();
            let values = fields
                .into_iter()
                .filter(|field| field.as_str() != key)
                .map(|field| {
                    (
                        field.clone(),
                        delta(&before[field], &after[field], comparable),
                    )
                })
                .collect::<serde_json::Map<_, _>>();
            json!({key:before.get(key).or_else(||after.get(key)),"values":values})
        })
        .collect::<Vec<_>>();
    if key == "turn" {
        joined.sort_by_key(|record| record[key].as_u64());
    }
    Ok(Value::Array(joined))
}

pub(super) fn row_change(
    id: &str,
    before: Option<&TestResult>,
    after: Option<&TestResult>,
) -> (&'static str, bool) {
    let change = match (before, after) {
        (None, _) => "added",
        (_, None) => "removed",
        (Some(a), Some(b)) if outcome_label(&a.outcome) == outcome_label(&b.outcome) => "unchanged",
        _ => "changed",
    };
    let regression = matches!(id, "footprint-curve" | "delete-uninstall-residue")
        && before.is_some_and(|r| matches!(r.outcome, TestOutcome::Pass))
        && after.is_some_and(|r| matches!(r.outcome, TestOutcome::Fail(_)));
    if regression {
        ("regression", true)
    } else {
        (change, false)
    }
}

pub(super) fn path_entry(path: &std::path::Path) -> Result<IndexEntry> {
    let path = if path.is_dir() {
        path.join("report.json")
    } else {
        path.to_path_buf()
    };
    let path = std::fs::canonicalize(path)?;
    let report: crate::report::Report = serde_json::from_slice(&std::fs::read(&path)?)?;
    let storage = report.storage_summary.as_ref();
    Ok(serde_json::from_value(json!({
        "schema":3,"pillar":crate::results::report_pillar(&report),"storage_summary":storage,"badge_label":report.badge.as_ref().map(crate::report::ReportBadge::label),
        "run_key":path.to_string_lossy(),"completed_at":"unavailable",
        "harness":report.fingerprint.harness,"harness_version":report.fingerprint.harness_version,
        "report_path":path,"report_schema":report.schema,"spec_version":report.spec_version,
        "profile":report.fingerprint.profile,
        "os":storage.map(|s| s.os.as_str()).or_else(||report.badge.as_ref().map(crate::report::ReportBadge::os)).unwrap_or_else(||report.fingerprint.platform.split(['-',' ']).next().unwrap_or("unknown")),
        "topology":storage.map(|s| s.topology.as_str()).unwrap_or(&report.resource_summary.topology),
        "resource_summary":report.resource_summary,
        "outcome_counts":crate::results::outcome_counts(&report.results),
        "metrics":report.metrics,"manifest_sha256":report.fingerprint.manifest,
        "workflow_sha256":report.fingerprint.workflows,"ahrb_revision":report.fingerprint.ahrb_revision
    }))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn summary() -> StorageSummary {
        StorageSummary {
            schema: 1,
            task: crate::storage::TASK.into(),
            profile: "quick".into(),
            os: "macos".into(),
            topology: "per-invocation".into(),
            comparison_scope: "within-topology-only".into(),
            allocation_source: "stat-st_blocks-512".into(),
            counter_source: "macos-ri_diskio_byteswritten".into(),
            declarations_sha256: "a".repeat(64),
            write_bytes_per_turn_p50: Some(4096.0),
            ..Default::default()
        }
    }
    #[test]
    fn storage_deltas_refuse_every_mismatched_or_missing_scope() -> Result<()> {
        let a = summary();
        let mut b = a.clone();
        b.write_bytes_per_turn_p50 = Some(8192.0);
        let d = compare(Some(&a), Some(&b))?.unwrap();
        assert_eq!(d["values"]["write_bytes_per_turn_p50"]["delta"], 4096.0);
        for field in [
            "schema",
            "task",
            "profile",
            "os",
            "topology",
            "allocation_source",
            "counter_source",
            "declarations_sha256",
            "comparison_scope",
        ] {
            let mut raw = serde_json::to_value(&b)?;
            raw[field] = if field == "schema" {
                json!(2)
            } else {
                json!("unknown")
            };
            let changed: StorageSummary = serde_json::from_value(raw)?;
            let d = compare(Some(&a), Some(&changed))?.unwrap();
            assert_eq!(
                d["comparison_scope"], "not-comparable-storage-scope",
                "{field}"
            );
            assert!(
                d["values"]["write_bytes_per_turn_p50"]
                    .get("delta")
                    .is_none(),
                "{field}"
            );
        }
        let d = compare(None, Some(&a))?.unwrap();
        assert_eq!(d["comparison_scope"], "unavailable");
        assert!(
            d["values"]["write_bytes_per_turn_p50"]
                .get("delta")
                .is_none()
        );
        assert!(compare(None, None)?.is_none());
        Ok(())
    }
    #[test]
    fn curves_and_families_join_exact_keys_without_zero_filling() {
        let a = json!([{"turn":1,"allocated_bytes":100},{"turn":10,"allocated_bytes":200}]);
        let b = json!([{"turn":10,"allocated_bytes":250},{"turn":50,"allocated_bytes":500},{"turn":100,"allocated_bytes":1000}]);
        let d = join(&a, &b, "turn", true).unwrap();
        let records = d.as_array().unwrap();
        assert_eq!(
            records
                .iter()
                .map(|r| r["turn"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![1, 10, 50, 100]
        );
        let matched = records.iter().find(|r| r["turn"] == 10).unwrap();
        assert_eq!(matched["values"]["allocated_bytes"]["delta"], 50.0);
        let absent = records.iter().find(|r| r["turn"] == 100).unwrap();
        assert_eq!(absent["values"]["allocated_bytes"]["change"], "unavailable");
        assert!(absent["values"]["allocated_bytes"].get("delta").is_none());
        let d = join(
            &json!([{"name":"logs","peak_allocated_bytes":40}]),
            &json!([{"name":"logs","peak_allocated_bytes":20}]),
            "name",
            true,
        )
        .unwrap();
        assert_eq!(d[0]["values"]["peak_allocated_bytes"]["delta"], -20.0);
        assert!(join(&json!([{"turn":1},{"turn":1}]), &json!([]), "turn", true).is_err());
    }
}
