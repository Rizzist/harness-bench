//! Small deterministic fixture executable invoked through real harness tools.

use ahrb::{AhrbError, Result};
#[cfg(unix)]
use std::os::unix::process::CommandExt as _;
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
        Some("process-tree") => {
            let child_pid = required(args, "--child-pid")?;
            let grandchild_pid = required(args, "--grandchild-pid")?;
            let executable = std::env::current_exe()?;
            let _child = std::process::Command::new(executable)
                .arg("process-tree-child")
                .arg("--child-pid")
                .arg(child_pid)
                .arg("--grandchild-pid")
                .arg(grandchild_pid)
                .spawn()?;
            park_forever()
        }
        Some("process-tree-launcher") => {
            let child_pid = required(args, "--child-pid")?;
            let grandchild_pid = required(args, "--grandchild-pid")?;
            let executable = std::env::current_exe()?;
            let _child = std::process::Command::new(executable)
                .arg("process-tree")
                .arg("--child-pid")
                .arg(child_pid)
                .arg("--grandchild-pid")
                .arg(grandchild_pid)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()?;
            Ok(0)
        }
        Some("process-tree-child") => {
            let child_pid = fixture_path(required(args, "--child-pid")?)?;
            let grandchild_pid = required(args, "--grandchild-pid")?;
            std::fs::write(child_pid, std::process::id().to_string())?;
            let executable = std::env::current_exe()?;
            let _grandchild = std::process::Command::new(executable)
                .arg("process-tree-grandchild")
                .arg("--grandchild-pid")
                .arg(grandchild_pid)
                .spawn()?;
            park_forever()
        }
        Some("process-tree-grandchild") => {
            let grandchild_pid = fixture_path(required(args, "--grandchild-pid")?)?;
            std::fs::write(grandchild_pid, std::process::id().to_string())?;
            park_forever()
        }
        #[cfg(unix)]
        Some("detached-signal-owner") => {
            let pid_file = fixture_path(required(args, "--pid-file")?)?;
            let signal = required(args, "--signal")?;
            let executable = std::env::current_exe()?;
            let executable_name = executable
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| {
                    AhrbError::Protocol("fixture executable has no UTF-8 basename".to_owned())
                })?
                .to_owned();
            let marker = format!("abort-handoff-{}", std::process::id());
            ahrb::process::install_cleanup_handlers();
            ahrb::process::register_detached_match(ahrb::manifest::ProcessMatch {
                executable_name,
                environment: std::collections::BTreeMap::from([(
                    "AHRB_DETACHED_ABORT_MARKER".to_owned(),
                    marker.clone(),
                )]),
            })?;
            let child = std::process::Command::new(executable)
                .arg("detached-abort-child")
                .arg("--pid-file")
                .arg(&pid_file)
                .env("AHRB_DETACHED_ABORT_MARKER", marker)
                .process_group(0)
                .spawn()?;
            std::fs::write(&pid_file, child.id().to_string())?;
            let signal = match signal {
                "abort" => libc::SIGABRT,
                "term" => libc::SIGTERM,
                other => {
                    return Err(AhrbError::Usage(format!(
                        "unsupported detached fixture signal {other:?}"
                    )));
                }
            };
            // SAFETY: raising the requested signal in this dedicated fixture
            // validates the installed cleanup handler.
            unsafe { libc::raise(signal) };
            park_forever()
        }
        #[cfg(unix)]
        Some("detached-abort-child") => park_forever(),
        Some(other) => Err(AhrbError::Usage(format!(
            "unknown fixture command {other:?}"
        ))),
        None => Err(AhrbError::Usage(
            "expected write, read, or fail fixture command".to_owned(),
        )),
    }
}

fn park_forever() -> Result<i32> {
    loop {
        std::thread::park_timeout(std::time::Duration::from_secs(60));
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
