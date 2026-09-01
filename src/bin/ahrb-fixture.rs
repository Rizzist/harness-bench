//! Small deterministic fixture executable invoked through real harness tools.

use ahrb::{AhrbError, Result};
use std::io::Write as _;
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
        Some("emit") => {
            let bytes = required(args, "--bytes")?
                .parse::<u64>()
                .map_err(|_| AhrbError::Usage("--bytes must be an unsigned integer".to_owned()))?;
            emit_deterministic(bytes)?;
            Ok(0)
        }
        Some("egress-probe") => {
            use std::net::{SocketAddr, TcpStream};
            use std::time::Duration;
            let address = required(args, "--address")?
                .parse::<SocketAddr>()
                .map_err(|_| AhrbError::Usage("--address must be host:port".to_owned()))?;
            let timeout_ms = required(args, "--timeout-ms")?
                .parse::<u64>()
                .map_err(|_| AhrbError::Usage("--timeout-ms must be an integer".to_owned()))?;
            match TcpStream::connect_timeout(&address, Duration::from_millis(timeout_ms)) {
                Err(error)
                    if matches!(error.raw_os_error(), Some(libc::EPERM) | Some(libc::EACCES)) =>
                {
                    println!(
                        "{}",
                        serde_json::json!({
                            "blocked":true,
                            "errno":error.raw_os_error(),
                            "address":address.to_string()
                        })
                    );
                    Ok(0)
                }
                Err(error) => {
                    println!(
                        "{}",
                        serde_json::json!({
                            "blocked":false,
                            "errno":error.raw_os_error(),
                            "error":error.to_string(),
                            "address":address.to_string()
                        })
                    );
                    Ok(3)
                }
                Ok(stream) => {
                    drop(stream);
                    println!(
                        "{}",
                        serde_json::json!({"blocked":false,"connected":true,"address":address.to_string()})
                    );
                    Ok(4)
                }
            }
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
        Some(other) => Err(AhrbError::Usage(format!(
            "unknown fixture command {other:?}"
        ))),
        None => Err(AhrbError::Usage(
            "expected write, read, fail, emit, egress-probe, or process-tree fixture command"
                .to_owned(),
        )),
    }
}

fn emit_deterministic(bytes: u64) -> Result<()> {
    const CHUNK_BYTES: usize = 16 * 1024;
    let mut chunk = [0_u8; CHUNK_BYTES];
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    let mut remaining = bytes;
    let mut stream_offset = 0_u64;
    while remaining > 0 {
        let count = usize::try_from(remaining.min(CHUNK_BYTES as u64)).map_err(|_| {
            AhrbError::Protocol("large-output chunk length does not fit usize".to_owned())
        })?;
        for (index, byte) in chunk[..count].iter_mut().enumerate() {
            let index = u64::try_from(index).map_err(|_| {
                AhrbError::Protocol("large-output alphabet index does not fit u64".to_owned())
            })?;
            let offset = u8::try_from(stream_offset.saturating_add(index) % 26).map_err(|_| {
                AhrbError::Protocol("large-output alphabet offset does not fit u8".to_owned())
            })?;
            *byte = b'a'.saturating_add(offset);
        }
        output.write_all(&chunk[..count])?;
        let written = u64::try_from(count).map_err(|_| {
            AhrbError::Protocol("large-output chunk length does not fit u64".to_owned())
        })?;
        remaining = remaining.saturating_sub(written);
        stream_offset = stream_offset.saturating_add(written);
    }
    output.flush()?;
    Ok(())
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
