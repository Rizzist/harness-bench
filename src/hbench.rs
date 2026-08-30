//! Harness-name shorthand for complete AHRB runs.

use crate::cli::{Profile, RunOptions};
use crate::{AhrbError, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Parsed `hbench <name>` invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Options {
    /// User-facing harness shorthand.
    pub name: String,
    /// Complete-suite output directory.
    pub output: PathBuf,
    /// Resource measurement profile.
    pub profile: Profile,
    /// Whether to write JUnit XML.
    pub junit: bool,
}

/// Parse the thin shorthand without accepting row filters.
pub fn parse(args: &[String]) -> Result<Options> {
    let name = args
        .first()
        .ok_or_else(|| AhrbError::Usage(usage().to_owned()))?
        .clone();
    let adapter = adapter_directory(&name)?;
    let mut values = BTreeMap::new();
    let mut junit = false;
    let mut index = 1;
    while index < args.len() {
        let flag = args[index].as_str();
        if flag == "--junit" {
            if junit {
                return Err(AhrbError::Usage("duplicate --junit".to_owned()));
            }
            junit = true;
            index += 1;
            continue;
        }
        if !matches!(flag, "--output" | "--profile") {
            return Err(AhrbError::Usage(format!(
                "unknown hbench flag {flag:?}; {}",
                usage()
            )));
        }
        let value = args
            .get(index + 1)
            .filter(|value| !value.starts_with("--"))
            .ok_or_else(|| AhrbError::Usage(format!("{flag} requires a value")))?;
        if values
            .insert(flag.trim_start_matches("--").to_owned(), value.clone())
            .is_some()
        {
            return Err(AhrbError::Usage(format!("duplicate {flag}")));
        }
        index += 2;
    }
    let profile = match values.get("profile").map(String::as_str).unwrap_or("quick") {
        "quick" => Profile::Quick,
        "cert" => Profile::Cert,
        other => {
            return Err(AhrbError::Usage(format!(
                "--profile must be quick or cert, not {other:?}"
            )));
        }
    };
    let output = values
        .get("output")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("ahrb-output").join(adapter));
    Ok(Options {
        name,
        output,
        profile,
        junit,
    })
}

/// Resolve, availability-check, and execute the complete matrix.
pub async fn execute(options: Options) -> Result<i32> {
    let manifest = resolve_bundled_manifest(&options.name).map_err(|error| {
        unavailable_error(
            &options.name,
            &format!("adapter could not be resolved: {error}"),
        )
    })?;
    let doctor = crate::manifest::doctor(&manifest).map_err(|error| {
        unavailable_error(
            &options.name,
            &format!("availability probe could not run: {error}"),
        )
    })?;
    if !doctor.ready {
        let detail = if doctor.diagnostics.is_empty() {
            "availability probe was not ready".to_owned()
        } else {
            doctor.diagnostics.join("; ")
        };
        return Err(unavailable_error(&options.name, &detail));
    }
    crate::runner::run(RunOptions {
        manifest,
        output: options.output,
        profile: options.profile,
        tests: Vec::new(),
        junit: options.junit,
    })
    .await
}

/// One-line command synopsis.
pub fn usage() -> &'static str {
    "hbench <codex|claude-code|opencode|pi|rick|haider> [--output DIR] [--profile quick|cert] [--junit]"
}

fn unavailable_error(name: &str, detail: &str) -> AhrbError {
    AhrbError::Validation(format!(
        "harness {name} not installed / adapter unvalidated: {detail}"
    ))
}

fn adapter_directory(name: &str) -> Result<&'static str> {
    match name {
        "codex" => Ok("codex"),
        "claude-code" => Ok("claude-code"),
        "opencode" => Ok("opencode"),
        "pi" => Ok("pi"),
        "rick" => Ok("rick"),
        "haider" | "haider-agent" => Ok("haider-agent"),
        _ => Err(unavailable_error(name, "no bundled adapter has that name")),
    }
}

fn resolve_bundled_manifest(name: &str) -> Result<PathBuf> {
    let adapter = adapter_directory(name)?;
    let mut roots = Vec::new();
    if let Ok(current) = std::env::current_dir() {
        roots.push(current);
    }
    if let Ok(executable) = std::env::current_exe() {
        roots.extend(executable.ancestors().map(Path::to_path_buf));
    }
    roots.push(PathBuf::from(env!("CARGO_MANIFEST_DIR")));
    roots.sort();
    roots.dedup();
    for root in roots {
        let candidate = root.join("adapters").join(adapter).join("manifest.toml");
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(AhrbError::Validation(format!(
        "bundled adapter adapters/{adapter}/manifest.toml was not found"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_suite_passthrough_and_alias() -> Result<()> {
        let args = [
            "haider",
            "--output",
            "results/haider",
            "--profile",
            "cert",
            "--junit",
        ]
        .map(str::to_owned);
        let parsed = parse(&args)?;
        assert_eq!(parsed.name, "haider");
        assert_eq!(parsed.output, PathBuf::from("results/haider"));
        assert_eq!(parsed.profile, Profile::Cert);
        assert!(parsed.junit);
        assert_eq!(adapter_directory(&parsed.name)?, "haider-agent");
        Ok(())
    }

    #[test]
    fn default_is_a_complete_quick_run_output() -> Result<()> {
        let parsed = parse(&["codex".to_owned()])?;
        assert_eq!(parsed.output, PathBuf::from("ahrb-output/codex"));
        assert_eq!(parsed.profile, Profile::Quick);
        assert!(!parsed.junit);
        Ok(())
    }

    #[test]
    fn unknown_name_is_a_clear_non_panicking_error() {
        let error = parse(&["unknown".to_owned()]).expect_err("unknown adapter must fail");
        assert!(
            error
                .to_string()
                .contains("harness unknown not installed / adapter unvalidated")
        );
    }
}
