//! Minimal deterministic command-line interface.

use crate::{AhrbError, Result};
use std::path::PathBuf;

/// Parsed top-level command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Command {
    /// Validate a harness manifest and environment.
    Doctor {
        /// Adapter manifest path.
        manifest: PathBuf,
    },
    /// Execute benchmark scenarios.
    Run(RunOptions),
    /// Re-render an existing JSON report.
    Report {
        /// Existing `report.json` path.
        input: PathBuf,
    },
    /// Print the versioned test matrix.
    ListTests,
}

/// Measurement profile selected for a run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Profile {
    /// Three repetitions and shortened stability windows.
    Quick,
    /// Seven repetitions and full certification windows.
    Cert,
}

/// Fully parsed options for `ahrb run`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunOptions {
    /// Adapter manifest path.
    pub manifest: PathBuf,
    /// Output bundle directory.
    pub output: PathBuf,
    /// Measurement profile.
    pub profile: Profile,
    /// Selected row numbers; empty means the complete matrix.
    pub tests: Vec<u8>,
    /// Emit JUnit XML.
    pub junit: bool,
}

/// Parse arguments without environment-dependent defaults.
pub fn parse(args: &[String]) -> Result<Command> {
    match args.first().map(String::as_str) {
        Some("list-tests") => Ok(Command::ListTests),
        Some("doctor") => {
            let values = parse_flags(&args[1..], &["manifest"], &[])?;
            Ok(Command::Doctor {
                manifest: PathBuf::from(required(&values, "manifest")?),
            })
        }
        Some("run") => {
            let values = parse_flags(
                &args[1..],
                &["manifest", "output", "profile", "tests"],
                &["junit"],
            )?;
            let profile = match values.get("profile").map(String::as_str).unwrap_or("quick") {
                "quick" => Profile::Quick,
                "cert" => Profile::Cert,
                other => {
                    return Err(AhrbError::Usage(format!(
                        "--profile must be quick or cert, not {other:?}"
                    )));
                }
            };
            let tests = values
                .get("tests")
                .map(|text| parse_test_rows(text))
                .transpose()?
                .unwrap_or_default();
            Ok(Command::Run(RunOptions {
                manifest: PathBuf::from(required(&values, "manifest")?),
                output: PathBuf::from(required(&values, "output")?),
                profile,
                tests,
                junit: values.contains_key("junit"),
            }))
        }
        Some("report") => {
            let values = parse_flags(&args[1..], &["input"], &[])?;
            Ok(Command::Report {
                input: PathBuf::from(required(&values, "input")?),
            })
        }
        Some(other) => Err(AhrbError::Usage(format!("unknown subcommand {other:?}"))),
        None => Err(AhrbError::Usage(
            "expected doctor, run, report, or list-tests".to_owned(),
        )),
    }
}

/// Execute a parsed top-level command.
pub async fn execute(command: Command) -> Result<i32> {
    match command {
        Command::ListTests => {
            for test in crate::scenarios::all() {
                println!("{}\t{}\t{}", test.row, test.id, test.name);
            }
            Ok(0)
        }
        Command::Doctor { manifest } => {
            let result = crate::manifest::doctor(&manifest)?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            Ok(if result.ready { 0 } else { 1 })
        }
        Command::Run(options) => crate::runner::run(options).await,
        Command::Report { input } => {
            let bytes = std::fs::read(input)?;
            let report: crate::report::Report = serde_json::from_slice(&bytes)?;
            print!("{}", crate::report::render_markdown(&report));
            Ok(0)
        }
    }
}

fn parse_flags(
    args: &[String],
    value_flags: &[&str],
    switches: &[&str],
) -> Result<std::collections::BTreeMap<String, String>> {
    let mut values = std::collections::BTreeMap::new();
    let mut index = 0;
    while index < args.len() {
        let raw = &args[index];
        let name = raw
            .strip_prefix("--")
            .ok_or_else(|| AhrbError::Usage(format!("expected a --flag, found {raw:?}")))?;
        if switches.contains(&name) {
            if values.insert(name.to_owned(), "true".to_owned()).is_some() {
                return Err(AhrbError::Usage(format!("duplicate --{name}")));
            }
            index += 1;
            continue;
        }
        if !value_flags.contains(&name) {
            return Err(AhrbError::Usage(format!("unknown flag --{name}")));
        }
        let value = args
            .get(index + 1)
            .ok_or_else(|| AhrbError::Usage(format!("--{name} requires a value")))?;
        if value.starts_with("--") {
            return Err(AhrbError::Usage(format!("--{name} requires a value")));
        }
        if values.insert(name.to_owned(), value.clone()).is_some() {
            return Err(AhrbError::Usage(format!("duplicate --{name}")));
        }
        index += 2;
    }
    Ok(values)
}

fn required<'a>(
    values: &'a std::collections::BTreeMap<String, String>,
    name: &str,
) -> Result<&'a str> {
    values
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| AhrbError::Usage(format!("--{name} is required")))
}

fn parse_test_rows(text: &str) -> Result<Vec<u8>> {
    let mut rows = Vec::new();
    for part in text.split(',') {
        let row: u8 = part
            .parse()
            .map_err(|_| AhrbError::Usage(format!("invalid test row {part:?}")))?;
        if !(1..=41).contains(&row) {
            return Err(AhrbError::Usage(format!(
                "test row {row} is outside 1..=41"
            )));
        }
        rows.push(row);
    }
    rows.sort_unstable();
    rows.dedup();
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_selected_run() -> Result<()> {
        let args = [
            "run",
            "--manifest",
            "mock.toml",
            "--output",
            "out",
            "--tests",
            "40,1,1",
            "--junit",
        ]
        .map(str::to_owned);
        let command = parse(&args)?;
        match command {
            Command::Run(options) => {
                assert_eq!(options.tests, vec![1, 40]);
                assert!(options.junit);
                Ok(())
            }
            other => Err(AhrbError::Protocol(format!(
                "unexpected parsed command: {other:?}"
            ))),
        }
    }
}
