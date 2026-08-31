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
    /// Print durable result history.
    Results {
        /// Optional harness ID filter.
        harness: Option<String>,
        /// Include every historical entry instead of only the latest.
        all: bool,
    },
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
    /// Run-level wall-clock budget in seconds. `None` selects the profile default.
    pub deadline_secs: Option<u64>,
    /// Do not persist a copy under the repository `results/` directory.
    pub no_save: bool,
    /// Availability-probe version already captured by a shorthand caller.
    pub harness_version: Option<String>,
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
                &["manifest", "output", "profile", "tests", "deadline"],
                &["junit", "no-save"],
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
                output: values.get("output").map(PathBuf::from).unwrap_or_default(),
                profile,
                tests,
                junit: values.contains_key("junit"),
                deadline_secs: values
                    .get("deadline")
                    .map(|value| parse_seconds("deadline", value))
                    .transpose()?,
                no_save: values.contains_key("no-save"),
                harness_version: None,
            }))
        }
        Some("report") => {
            let values = parse_flags(&args[1..], &["input"], &[])?;
            Ok(Command::Report {
                input: PathBuf::from(required(&values, "input")?),
            })
        }
        Some("results") => {
            let (harness, all) = parse_results_args(&args[1..])?;
            Ok(Command::Results { harness, all })
        }
        Some(other) => Err(AhrbError::Usage(format!("unknown subcommand {other:?}"))),
        None => Err(AhrbError::Usage(
            "expected doctor, run, report, results, or list-tests".to_owned(),
        )),
    }
}

fn parse_seconds(name: &str, value: &str) -> Result<u64> {
    value.parse::<u64>().map_err(|_| {
        AhrbError::Usage(format!(
            "--{name} must be a non-negative integer number of seconds"
        ))
    })
}

/// Resolve an explicit deadline, `AHRB_DEADLINE`, or the profile default.
pub fn deadline_secs(options: &RunOptions) -> Result<u64> {
    if let Some(seconds) = options.deadline_secs {
        return Ok(seconds);
    }
    if let Some(value) = std::env::var_os("AHRB_DEADLINE") {
        let value = value.to_string_lossy();
        return value.parse::<u64>().map_err(|_| {
            AhrbError::Usage(
                "AHRB_DEADLINE must be a non-negative integer number of seconds".to_owned(),
            )
        });
    }
    Ok(match options.profile {
        Profile::Quick => 15 * 60,
        Profile::Cert => 30 * 60,
    })
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
        Command::Results { harness, all } => {
            crate::results::print_history(harness.as_deref(), all)?;
            Ok(0)
        }
    }
}

/// Parse `[HARNESS] [--all]` for both `ahrb results` and `hbench results`.
pub(crate) fn parse_results_args(args: &[String]) -> Result<(Option<String>, bool)> {
    let mut harness = None;
    let mut all = false;
    for argument in args {
        if argument == "--all" {
            if all {
                return Err(AhrbError::Usage("duplicate --all".to_owned()));
            }
            all = true;
        } else if argument.starts_with("--") {
            return Err(AhrbError::Usage(format!(
                "unknown results flag {argument:?}"
            )));
        } else if harness.replace(argument.clone()).is_some() {
            return Err(AhrbError::Usage(
                "results accepts at most one harness ID".to_owned(),
            ));
        }
    }
    Ok((harness, all))
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

/// Parse a comma-separated selection of matrix rows, including inclusive ranges.
pub(crate) fn parse_test_rows(text: &str) -> Result<Vec<u8>> {
    let mut rows = Vec::new();
    for raw_part in text.split(',') {
        let part = raw_part.trim();
        if part.is_empty() {
            return Err(AhrbError::Usage(
                "test row selection contains an empty item".to_owned(),
            ));
        }
        if let Some((start, end)) = part.split_once('-') {
            let start = parse_test_row(start.trim())?;
            let end = parse_test_row(end.trim())?;
            if start > end {
                return Err(AhrbError::Usage(format!(
                    "test row range {part:?} is descending"
                )));
            }
            for row in start..=end {
                ensure_implemented_row(row)?;
                rows.push(row);
            }
        } else {
            rows.push(parse_test_row(part)?);
        }
    }
    rows.sort_unstable();
    rows.dedup();
    Ok(rows)
}

fn parse_test_row(part: &str) -> Result<u8> {
    let row: u8 = part
        .parse()
        .map_err(|_| AhrbError::Usage(format!("invalid test row {part:?}")))?;
    let maximum = crate::scenarios::all()
        .last()
        .map_or(0, |definition| definition.row);
    if !(1..=maximum).contains(&row) {
        return Err(AhrbError::Usage(format!(
            "test row {row} is outside 1..={maximum}"
        )));
    }
    ensure_implemented_row(row)?;
    Ok(row)
}

fn ensure_implemented_row(row: u8) -> Result<()> {
    if crate::scenarios::all()
        .iter()
        .any(|definition| definition.row == row)
    {
        Ok(())
    } else {
        Err(AhrbError::Usage(format!(
            "test row {row} is not implemented in this staged matrix"
        )))
    }
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
                assert_eq!(options.deadline_secs, None);
                assert!(!options.no_save);
                Ok(())
            }
            other => Err(AhrbError::Protocol(format!(
                "unexpected parsed command: {other:?}"
            ))),
        }
    }

    #[test]
    fn parses_explicit_deadline() -> Result<()> {
        let args = [
            "run",
            "--manifest",
            "mock.toml",
            "--output",
            "out",
            "--deadline",
            "45",
        ]
        .map(str::to_owned);
        let Command::Run(options) = parse(&args)? else {
            return Err(AhrbError::Protocol("run command was not parsed".to_owned()));
        };
        assert_eq!(options.deadline_secs, Some(45));
        Ok(())
    }

    #[test]
    fn parses_deduplicated_inclusive_test_ranges() -> Result<()> {
        assert_eq!(
            parse_test_rows("3,1-3,30-32,31")?,
            vec![1, 2, 3, 30, 31, 32]
        );
        assert!(parse_test_rows("4-2").is_err());
        assert!(parse_test_rows("1,").is_err());
        assert_eq!(parse_test_rows("40-45")?, vec![40, 41, 42, 43, 44, 45]);
        assert_eq!(parse_test_rows("44-46")?, vec![44, 45, 46]);
        assert!(parse_test_rows("47").is_err());
        assert_eq!(parse_test_rows("63-64")?, vec![63, 64]);
        Ok(())
    }

    #[test]
    fn parses_results_history_filter() -> Result<()> {
        let args = ["results", "mock", "--all"].map(str::to_owned);
        assert_eq!(
            parse(&args)?,
            Command::Results {
                harness: Some("mock".to_owned()),
                all: true,
            }
        );
        Ok(())
    }
}
