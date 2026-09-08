#!/usr/bin/env python3
"""Emit an allowlisted, path-free Markdown view of a reference run (Python 3.9)."""
import argparse
import collections
import html
import json
import math
from pathlib import Path
import re
import sys

HARNESSES = ("claude-code", "opencode", "pi", "rick", "haider-agent", "codex", "mock", "mock-exec")
CLASSES = ("PASS", "FAIL", "ERROR", "UNSUPPORTED", "ABSENT")
RESOURCE = "peak_rss_mib cpu_per_turn_ms wall_per_turn_ms wall_per_turn_p95_ms parallel_beta_mib_per_agent disk_write_bytes_per_turn_p50 disk_write_bytes_per_turn_p95".split()
ECONOMY = "schema task profile turn_budget model_turns total_reference_tokens tool_calls tool_results tool_result_requests tool_batching_factor last_context_size_tokens completion tokens_per_completed_task reference_tariff_usd_per_million_tokens reference_cost_usd cache_eligible_fraction cache_control_breakpoints cache_control_breakpoints_per_request redundant_tokens context_token_curve context_token_curve_slope context_token_curve_last_matches_last_context_size per_turn_fixed_overhead_tokens wasted_tool_call_count retry_attempts retry_reference_tokens cache_regime cache_input_discount effective_reference_tokens effective_cost_usd stable_prefix_preserved_fraction cache_bust_count invalidated_prefix_tokens invalidated_prefix_tokens_per_turn".split()
FIDELITY = "schema task profile turn_budget model_turns needle_survival_fraction survival_curve first_loss_turn retained_tool_result_fraction end_reason end_turn harness_exit_status harness_exit_code internal_cap_detected declared_turn_ceiling workspace_state workspace_receipt_before_sha256 workspace_receipt_after_sha256".split()
STORAGE = "schema task profile os topology comparison_scope turn_budget repetitions completed_turns physical_requests measurement_label counter_source allocation_source declarations_sha256 write_bytes_per_turn_p50 write_bytes_per_turn_p95 write_bytes_per_turn_max logical_growth_bytes_per_turn net_growth_bytes_per_turn write_amplification_ratio disk_class fsync_calls_per_turn fdatasync_calls_per_turn fullfsync_calls_per_turn durability_calls_per_turn assumed_fsync_cost_ms estimated_durability_wall_ms_per_turn durability_class first_turn_allocated_bytes footprint_slope_bytes_per_turn growth_class compaction_before_allocated_bytes compaction_after_allocated_bytes compaction_freed_pct closed_sessions close_retained_bytes_per_session close_retained_after_sweep_bytes_per_session retention_cap_bytes close_retention_class delete_residue_allocated_bytes delete_residue_files uninstall_residue_allocated_bytes uninstall_residue_files request_retention_class stored_request_bytes unique_request_content_bytes stored_unique_ratio crash_residue_allocated_bytes crash_residue_files crash_resume_outcome resume_read_bytes_p50 resume_read_bytes_p95 resume_latency_p50_ms resume_latency_p95_ms resume_outcome".split()


def read(path):
    try:
        with open(path, encoding="utf-8") as stream:
            value = json.load(stream)
        return (value, "PRESENT") if isinstance(value, dict) else ({}, "ERROR: invalid object")
    except FileNotFoundError:
        return {}, "MISSING"
    except (OSError, ValueError):
        return {}, "ERROR: unreadable JSON"


class BadgeText(str):
    """A badge assembled exclusively from individually sanitized scalar fields."""


def cell(value):
    """No arbitrary nested objects, paths, request bodies or detail strings escape."""
    if isinstance(value, BadgeText):
        return str(value)
    if value is None:
        return "UNAVAILABLE (null or missing)"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (int, float)):
        return str(value) if math.isfinite(value) else "ERROR: nonfinite"
    if isinstance(value, list):
        return ", ".join(cell(v) for v in value) if value else "UNAVAILABLE (empty)"
    if not isinstance(value, str) or not value:
        return "UNAVAILABLE"
    # This exact normative label contains I/O, but cannot contain a private path.
    if value == 'OS-accounted owned-tree physical I/O; per-file allocated footprint, not unique-volume consumption':
        return value
    # Values here are scalar identifiers/classes/version labels, never prose evidence.
    if len(value) > 200 or not re.fullmatch(r"[\w .:+()=,;@%\-·]+", value, re.ASCII):
        return "REDACTED (non-summary text)"
    return html.escape(value).replace("|", "&#124;")


def table(headers, rows):
    print("| " + " | ".join(headers) + " |")
    print("| " + " | ".join("---" for _ in headers) + " |")
    for row in rows:
        print("| " + " | ".join(cell(v) for v in row) + " |")
    print()


def summary(report, name, fields):
    value = report.get(name)
    if not isinstance(value, dict):
        value = {}
    table(["Field", "Value"], [(key, value.get(key)) for key in fields])
    return value


def row_class(row):
    outcome = row.get("outcome") if isinstance(row, dict) else None
    c = outcome.get("class") if isinstance(outcome, dict) else None
    return c if c in CLASSES else "ERROR"


def rows(report):
    value = report.get("results")
    return value if isinstance(value, list) else []


def matrix_badge(report, state):
    if state != "PRESENT":
        return state
    if "badge" not in report:
        return "MISSING"
    badge = report["badge"]
    if badge is None:
        return "WITHHELD (none)"
    if isinstance(badge, str):
        return badge  # Preserve explicit withheld/none states; cell sanitizes it.
    if not isinstance(badge, dict):
        return "ERROR: invalid badge"
    # Match src/evaluate.rs::badge_label; matrix Badge has no `label` field.
    fields = {k: cell(badge.get(k)) for k in (
        "os", "topology", "profile", "parallel_width", "resource_class",
        "latency_class", "cpu_class", "automation_score")}
    facets = badge.get("facets")
    facets = "+".join(cell(f) for f in facets) if isinstance(facets, list) else "MISSING"
    if badge.get("spec_version") == 1:
        parts = ["Automation Ready v1", fields["os"], fields["topology"],
                 "N" + fields["parallel_width"], fields["resource_class"], facets]
    elif badge.get("spec_version") == 2:
        parts = ["Automation Ready v2", fields["os"], fields["topology"], fields["profile"],
                 "N" + fields["parallel_width"], fields["resource_class"], fields["latency_class"],
                 fields["cpu_class"], "A" + fields["automation_score"]]
        if facets:
            parts.append(facets)
    else:
        return "ERROR: unsupported badge schema"
    return BadgeText(" · ".join(parts))


def render(run):
    provenance, provenance_status = read(run / "provenance.json")
    selected = provenance.get("harnesses", list(HARNESSES[:6]))
    if not isinstance(selected, list) or not selected or any(h not in HARNESSES for h in selected):
        raise ValueError("invalid harness inventory")
    print("# Reference measurements\n")
    verdict, certification = "MISSING (unfinished)", "MISSING"
    try:
        with open(run / "reference-run.log", encoding="utf-8") as stream:
            for line in stream:
                if line.startswith("START ") and "profile=quick" in line:
                    verdict, certification = "MISSING (unfinished)", "MISSING"
                match = re.fullmatch(r"REFERENCE (PASS|PARTIAL|FAIL)\n?", line)
                if match:
                    verdict = match.group(1)
                if line.startswith("RESULT mock-cert: SKIPPED"):
                    certification = "SKIPPED (not certified by this invocation)"
                match = re.fullmatch(r"RESULT mock-cert: EXIT=(\d+)\n?", line)
                if match:
                    certification = "PASS" if match.group(1) == "0" else "FAIL"
    except OSError:
        pass
    table(["Reference verdict", "Mock self-certification"], [[verdict, certification]])
    print("Quick profile. Descriptive tables only; no ranking or cross-topology deltas. "
          "Compare only identical OS, topology, profile, task and measurement pins; unknown scope is not comparable. "
          "Storage additionally requires equal declaration hashes. Missing values are not zero.\n")
    print("Economy uses reference tokens (o200k_base-style) and a fixed comparison tariff, not provider usage or a bill. "
          "Cache eligibility and effective cost are prefix upper-bound proxies with the stated input discount, not observed cache hits. "
          "Fidelity measures exact byte retention and scripted effects, not model competence. "
          "Storage physical writes and allocated footprint are separate; durability wall is an estimate at 4 ms per call. "
          "Informational PASS means measurement completed, including adverse classes.\n")
    for h in selected:
        print("## " + h + "\n")
        for pillar in ("matrix", "economy", "fidelity", "storage"):
            report, state = read(run / h / pillar / "report.json")
            print("### " + pillar + " — " + state + "\n")
            receipts = []
            for name in ("exit-code.txt", "cleanup-exit-code.txt"):
                try:
                    receipts.append(int((run / h / pillar / name).read_text().strip()))
                except (OSError, ValueError):
                    receipts.append("MISSING or ERROR")
            table(["Command exit", "Cleanup exit"], [receipts])
            resource = report.get("resource_summary") or {}
            fingerprint = report.get("fingerprint") or {}
            storage = report.get("storage_summary") or {}
            table(["OS", "Topology", "Profile", "Task", "Canonical manifest SHA-256", "Workflow SHA-256"], [[
                storage.get("os", fingerprint.get("platform")), storage.get("topology", resource.get("topology")),
                fingerprint.get("profile"), (report.get(pillar + "_summary") or {}).get("task", "matrix" if pillar == "matrix" else None),
                fingerprint.get("manifest"), fingerprint.get("workflows")]])
            if pillar == "matrix":
                counts = collections.Counter(row_class(row) for row in rows(report))
                table(list(CLASSES) + ["Missing rows", "Badge"], [[
                    *[counts[c] if state == "PRESENT" else state for c in CLASSES],
                    max(0, 73 - len({row.get("row") for row in rows(report) if isinstance(row, dict)})),
                    matrix_badge(report, state)]])
                summary(report, "resource_summary", RESOURCE)
            elif pillar == "economy":
                value = summary(report, "economy_summary", ECONOMY)
                pins = value.get("reference_tokenizer") or {}
                effects = value.get("effects_verified") or {}
                table(["Tokenizer version", "Vocabulary SHA-256", "Effects verified"],
                      [[pins.get("version"), pins.get("vocabulary_sha256"), effects.get("all_verified")]])
            elif pillar == "fidelity":
                value = summary(report, "fidelity_summary", FIDELITY)
                table(["Needle ID", "Planted turn", "First disappeared turn", "Ever reappeared"],
                      [[n.get(k) for k in ("id", "planted_turn", "first_disappeared_turn", "ever_reappeared")]
                       for n in value.get("needles", []) if isinstance(n, dict)] or [[None] * 4])
            else:
                value = summary(report, "storage_summary", STORAGE)
                table(["D class", "G class"], [["D" + str(value["disk_class"]) if value.get("disk_class") else None,
                      "G" + str(value["growth_class"]) if value.get("growth_class") else None]])
                indexed = collections.defaultdict(list)
                for row in rows(report):
                    if isinstance(row, dict):
                        indexed[row.get("row")].append(row)
                table(["Row", "Outcome"], [["S" + str(n), row_class(indexed[n][0]) if len(indexed[n]) == 1
                      else "ERROR: duplicate row" if indexed[n] else "MISSING"] for n in range(1, 11)])
                for key, fields in [("footprint_curve", ["turn", "allocated_bytes", "mad_bytes"]),
                                    ("auxiliaries", ["name", "declared", "cap_bytes", "peak_allocated_bytes", "final_allocated_bytes", "slope_bytes_per_turn", "rotation_observed", "class"])]:
                    print(key + "\n")
                    table(fields, [[entry.get(k) for k in fields] for entry in value.get(key, []) if isinstance(entry, dict)]
                          or [[None] * len(fields)])
    print("## Provenance — " + provenance_status + "\n")
    table(["Field", "Value"], [(key, provenance.get(key)) for key in
          ("ahrb_revision", "ahrb_sha256", "working_diff_sha256", "os", "profile")])
    table(["Binary", "Version", "SHA-256"], [[Path(name).name,
          "UNAVAILABLE (help-only binary)" if item.get("probe_kind") == "help" else str(item.get("version", "")).splitlines()[0] if item.get("version") else None,
          item.get("sha256")]
          for name, item in provenance.get("binaries", {}).items() if isinstance(item, dict)])
    table(["Manifest", "File SHA-256", "Canonical SHA-256"], [[h,
          provenance.get("manifest_sha256", {}).get(h),
          provenance.get("canonical_manifest_sha256", {}).get(h)] for h in selected])
    table(["Runner script", "SHA-256"], [[Path(k).name, v] for k, v in provenance.get("scripts_sha256", {}).items()])
    samples = []
    try:
        with open(run / "load-samples.jsonl", encoding="utf-8") as stream:
            for line in stream:
                try:
                    item = json.loads(line)
                    samples.append([item.get(k) for k in ("timestamp", "harness", "load_1m", "max_load", "free_mb", "min_free_mb")])
                except (ValueError, AttributeError):
                    samples.append(["ERROR: invalid sample"] * 6)
    except OSError:
        samples.append(["MISSING"] * 6)
    table(["UTC", "Before harness", "Load 1m", "Load threshold", "Free MiB", "Minimum MiB"], samples)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path)
    args = parser.parse_args()
    if not args.run.is_dir():
        parser.error("reference run directory does not exist")
    try:
        render(args.run)
    except (TypeError, ValueError, AttributeError):
        print("ERROR: invalid reference summary structure", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
