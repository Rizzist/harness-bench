//! Small deterministic fixture executable invoked through real harness tools.

use ahrb::{AhrbError, Result};
use std::path::{Component, Path, PathBuf};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match run(&args) {
        Ok(code) => code,
        Err(error) => {
            eprintln!("ahrb-fixture: {error}");
            2
        }
    };
    std::process::exit(code);
}

fn run(args: &[String]) -> Result<i32> {
    match args.first().map(String::as_str) {
        Some("write") => {
            let path = fixture_path(required(args, "--path")?)?;
            let content = required(args, "--content")?;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&path, content.as_bytes())?;
            println!("wrote {} bytes to {}", content.len(), path.display());
            Ok(0)
        }
        Some("read") => {
            let path = fixture_path(required(args, "--path")?)?;
            print!("{}", std::fs::read_to_string(path)?);
            Ok(0)
        }
        Some("fail") => {
            let message = required(args, "--message")?;
            eprintln!("{message}");
            Ok(1)
        }
        Some(other) => Err(AhrbError::Usage(format!(
            "unknown fixture command {other:?}"
        ))),
        None => Err(AhrbError::Usage(
            "expected write, read, or fail fixture command".to_owned(),
        )),
    }
}

fn required<'a>(args: &'a [String], flag: &str) -> Result<&'a str> {
    let index = args
        .iter()
        .position(|argument| argument == flag)
        .ok_or_else(|| AhrbError::Usage(format!("{flag} is required")))?;
    args.get(index + 1)
        .map(String::as_str)
        .ok_or_else(|| AhrbError::Usage(format!("{flag} needs a value")))
}

fn fixture_path(value: &str) -> Result<PathBuf> {
    let relative = Path::new(value);
    if relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(AhrbError::Validation(format!(
            "fixture path {value:?} must be a plain relative path"
        )));
    }
    Ok(std::env::current_dir()?.join(relative))
}
