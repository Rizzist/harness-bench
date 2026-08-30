use std::fs::File;
use std::path::Path;
use std::process::Output;

pub struct AhrbSubprocessGuard {
    #[cfg(unix)]
    _file: File,
}

pub fn serialize_ahrb_subprocesses() -> AhrbSubprocessGuard {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd as _;

        let path = std::env::temp_dir().join("ahrb-test-server-subprocess.lock");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .expect("open shared AHRB subprocess test lock");
        loop {
            // SAFETY: `file` owns this valid descriptor for the full lifetime of the guard.
            let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
            if result == 0 {
                return AhrbSubprocessGuard { _file: file };
            }
            let error = std::io::Error::last_os_error();
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::Interrupted,
                "lock shared AHRB subprocess test gate: {error}"
            );
        }
    }
    #[cfg(not(unix))]
    {
        AhrbSubprocessGuard {}
    }
}

#[allow(dead_code)]
pub fn read_ahrb_run_report(result: &Output, report_path: &Path, context: &str) -> Vec<u8> {
    let report = std::fs::read(report_path);
    if !result.status.success() || report.is_err() {
        let report_error = report
            .as_ref()
            .err()
            .map_or_else(|| "none".to_owned(), ToString::to_string);
        panic!(
            "{context}\nstatus={}\nexit-code={:?}\nreport={}\nreport-read-error={}\nstdout:\n{}\nstderr:\n{}",
            result.status,
            result.status.code(),
            report_path.display(),
            report_error,
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr),
        );
    }
    report.expect("report read was checked above")
}
