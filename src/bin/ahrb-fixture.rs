//! Small deterministic fixture executable invoked through real harness tools.

use ahrb::{AhrbError, Result};
use sha2::{Digest as _, Sha256};
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
            let declared_path = required(args, "--path")?;
            let path = fixture_path(declared_path)?;
            let content = required(args, "--content")?;
            let result = (|| -> std::io::Result<()> {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(&path, content.as_bytes())
            })();
            match result {
                Ok(()) => {
                    println!(
                        "{}",
                        serde_json::json!({
                            "schema":"ahrb.fixture.write.v1",
                            "ok":true,
                            "path":declared_path,
                            "write_errno":serde_json::Value::Null,
                            "bytes":content.len(),
                        })
                    );
                    Ok(0)
                }
                Err(error) => {
                    println!(
                        "{}",
                        serde_json::json!({
                            "schema":"ahrb.fixture.write.v1",
                            "ok":false,
                            "path":declared_path,
                            "write_errno":error.raw_os_error(),
                            "error":error.to_string(),
                        })
                    );
                    Ok(1)
                }
            }
        }
        Some("write-row3-dependency") => {
            let path = fixture_path(required(args, "--path")?)?;
            let content = ahrb::row3::fresh_dependency_value()?;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&path, content.as_bytes())?;
            println!("{content}");
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
                            "kind":format!("{:?}", error.kind()),
                            "error":error.to_string(),
                            "address":address.to_string()
                        })
                    );
                    Ok(3)
                }
                Ok(stream) => {
                    // The source address is the per-probe token row 62's
                    // guard matches against its sentinel's recorded peer.
                    let local = stream.local_addr().ok().map(|local| local.to_string());
                    drop(stream);
                    println!(
                        "{}",
                        serde_json::json!({
                            "blocked":false,
                            "connected":true,
                            "address":address.to_string(),
                            "local":local
                        })
                    );
                    Ok(4)
                }
            }
        }
        Some("egress-udp-probe") => {
            use std::net::{IpAddr, SocketAddr, UdpSocket};
            let address = required(args, "--address")?
                .parse::<SocketAddr>()
                .map_err(|_| AhrbError::Usage("--address must be host:port".to_owned()))?;
            let bind_address = match address.ip() {
                IpAddr::V4(_) => "0.0.0.0:0",
                IpAddr::V6(_) => "[::]:0",
            };
            let socket = UdpSocket::bind(bind_address)?;
            match socket.send_to(b"ahrb-row62-udp-control", address) {
                Err(error)
                    if matches!(error.raw_os_error(), Some(libc::EPERM) | Some(libc::EACCES)) =>
                {
                    println!(
                        "{}",
                        serde_json::json!({
                            "blocked":true,
                            "errno":error.raw_os_error(),
                            "protocol":"udp",
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
                            "protocol":"udp",
                            "address":address.to_string()
                        })
                    );
                    Ok(3)
                }
                Ok(bytes_sent) => {
                    println!(
                        "{}",
                        serde_json::json!({
                            "blocked":false,
                            "bytes_sent":bytes_sent,
                            "protocol":"udp",
                            "address":address.to_string()
                        })
                    );
                    Ok(4)
                }
            }
        }
        Some("egress-probe-child") => {
            let executable = std::env::current_exe()?;
            let status = std::process::Command::new(executable)
                .arg("egress-probe")
                .args(&args[1..])
                .stdin(std::process::Stdio::null())
                .status()?;
            Ok(status.code().unwrap_or(2))
        }
        Some("guarded-launch") => {
            let profile = PathBuf::from(required(args, "--profile")?);
            let expected = required(args, "--profile-sha256")?;
            let launcher = required(args, "--launcher")?;
            let actual = format!("{:x}", Sha256::digest(std::fs::read(&profile)?));
            if actual != expected {
                return Err(AhrbError::Protocol(format!(
                    "offline guard profile hash mismatch before launch: expected {expected}, found {actual}"
                )));
            }
            let separator = args.iter().position(|argument| argument == "--").ok_or_else(|| {
                AhrbError::Usage("guarded-launch requires -- before the command".to_owned())
            })?;
            let command = args.get(separator + 1..).unwrap_or_default();
            if command.is_empty() {
                return Err(AhrbError::Usage(
                    "guarded-launch requires a command after --".to_owned(),
                ));
            }
            let mut launch = std::process::Command::new(launcher);
            launch.arg("-f").arg(&profile).args(command);
            #[cfg(unix)]
            {
                use std::os::unix::process::CommandExt as _;
                Err(launch.exec().into())
            }
            #[cfg(not(unix))]
            {
                Ok(launch.status()?.code().unwrap_or(2))
            }
        }
        Some("profile-write-probe") => {
            let profile = PathBuf::from(required(args, "--path")?);
            let result = std::fs::OpenOptions::new()
                .append(true)
                .open(&profile)
                .and_then(|mut file| file.write_all(b"\n; ahrb profile mutation probe\n"));
            match result {
                Err(error)
                    if matches!(error.raw_os_error(), Some(libc::EPERM) | Some(libc::EACCES)) =>
                {
                    println!(
                        "{}",
                        serde_json::json!({
                            "profile_write_blocked":true,
                            "errno":error.raw_os_error(),
                            "path":profile,
                        })
                    );
                    Ok(0)
                }
                Err(error) => {
                    println!(
                        "{}",
                        serde_json::json!({
                            "profile_write_blocked":false,
                            "errno":error.raw_os_error(),
                            "error":error.to_string(),
                            "path":profile,
                        })
                    );
                    Ok(3)
                }
                Ok(()) => {
                    println!(
                        "{}",
                        serde_json::json!({"profile_write_blocked":false,"modified":true,"path":profile})
                    );
                    Ok(4)
                }
            }
        }
        Some("abort-stale-identity") => {
            let pid = required(args, "--pid")?
                .parse::<u32>()
                .map_err(|_| AhrbError::Usage("--pid must be an unsigned integer".to_owned()))?;
            let mut process = ahrb::process::live_process_info(pid)?.ok_or_else(|| {
                AhrbError::Protocol(format!("abort identity fixture PID {pid} is not live"))
            })?;
            let actual_start = process.identity.start_time;
            process.identity.start_time = actual_start.saturating_add(1);
            eprintln!(
                "ahrb-fixture: abort control PID {pid} actual start {actual_start}, registered stale start {}",
                process.identity.start_time
            );
            ahrb::process::install_cleanup_handlers();
            ahrb::process::track_profile_owned_processes(&[process])?;
            std::process::abort();
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
            // Keep the launch root inspectable long enough for the ownership
            // registry to capture its `(pid,start_time)` before deliberately
            // exiting and leaving the registered descendants behind.
            std::thread::sleep(std::time::Duration::from_millis(50));
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
        Some("profile-daemon-launcher") => {
            let profile = required(args, "--profile")?;
            let pid_file = required(args, "--pid-file")?;
            let executable = std::env::current_exe()?;
            let mut command = std::process::Command::new(executable);
            command
                .arg("profile-daemon")
                .arg("--profile")
                .arg(profile)
                .arg("--pid-file")
                .arg(pid_file)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null());
            if let Some(lock_file) = optional(args, "--lock-file") {
                command.arg("--lock-file").arg(lock_file);
            }
            if let Some(lock_kind) = optional(args, "--lock-kind") {
                command.arg("--lock-kind").arg(lock_kind);
            }
            let _child = command.spawn()?;
            Ok(0)
        }
        Some("profile-daemon") => {
            #[cfg(unix)]
            {
                // SAFETY: the fixture is a fresh child and deliberately
                // detaches to reproduce a reparented daemon.
                let _ = unsafe { libc::setsid() };
            }
            let _profile = required(args, "--profile")?;
            let pid_file = PathBuf::from(required(args, "--pid-file")?);
            let flock = optional(args, "--lock-kind") == Some("flock");
            let lock_file = optional(args, "--lock-file")
                .map(PathBuf::from)
                .map(|path| lock_fixture_file(path, flock))
                .transpose()?;
            if let Some(parent) = pid_file.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&pid_file, std::process::id().to_string())?;
            let _lock_file = lock_file;
            park_forever()
        }
        Some(other) => Err(AhrbError::Usage(format!(
            "unknown fixture command {other:?}"
        ))),
        None => Err(AhrbError::Usage(
            "expected write, read, fail, emit, egress-probe, egress-udp-probe, guarded-launch, profile-write-probe, abort-stale-identity, or process-tree fixture command"
                .to_owned(),
        )),
    }
}

fn optional<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.iter()
        .position(|argument| argument == flag)
        .and_then(|index| args.get(index + 1))
        .map(String::as_str)
}

/// Hold `path` locked for the fixture's lifetime: a POSIX record lock by
/// default, or a `flock(2)` lock (whose owner `F_GETLK` cannot name on macOS).
#[cfg(unix)]
fn lock_fixture_file(path: PathBuf, flock: bool) -> Result<std::fs::File> {
    use std::os::fd::AsRawFd as _;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)?;
    if flock {
        // SAFETY: `file` is open for the duration of the call.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        return Ok(file);
    }
    let write_lock_type =
        libc::c_short::try_from(libc::F_WRLCK).expect("F_WRLCK must fit the platform flock type");
    let lock = libc::flock {
        l_start: 0,
        l_len: 0,
        l_pid: 0,
        l_type: write_lock_type,
        l_whence: libc::SEEK_SET as i16,
    };
    // SAFETY: `file` is open and `lock` is a valid flock input structure.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &lock) } < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(file)
}

#[cfg(not(unix))]
fn lock_fixture_file(_path: PathBuf, _flock: bool) -> Result<std::fs::File> {
    Err(AhrbError::Unsupported(
        "profile lock fixture requires fcntl".to_owned(),
    ))
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
