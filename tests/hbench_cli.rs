use std::path::Path;
use std::process::Command;

#[test]
fn unavailable_named_harness_exits_nonzero_without_panicking() {
    let result = Command::new(env!("CARGO_BIN_EXE_hbench"))
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .env("PATH", "")
        .arg("pi")
        .arg("--output")
        .arg("unused-output")
        .arg("--profile")
        .arg("quick")
        .arg("--junit")
        .output()
        .expect("execute hbench shorthand");
    assert_ne!(result.status.code(), Some(0));
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("harness pi not installed / adapter unvalidated"),
        "stderr: {stderr}"
    );
    assert!(!stderr.to_ascii_lowercase().contains("panicked"));
}

#[test]
fn unknown_name_is_rejected_with_the_same_clear_contract() {
    let result = Command::new(env!("CARGO_BIN_EXE_hbench"))
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .arg("not-a-harness")
        .output()
        .expect("execute hbench with an unknown name");
    assert_ne!(result.status.code(), Some(0));
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(stderr.contains("harness not-a-harness not installed / adapter unvalidated"));
}

#[cfg(unix)]
#[test]
fn haider_requires_both_client_and_daemon_executables() {
    use std::os::unix::fs::PermissionsExt;

    let bin_dir = std::env::temp_dir().join(format!("ahrb-haider-doctor-{}", std::process::id()));
    if bin_dir.exists() {
        std::fs::remove_dir_all(&bin_dir).expect("remove stale doctor fixture");
    }
    std::fs::create_dir(&bin_dir).expect("create doctor fixture");
    let client = bin_dir.join("haider");
    std::fs::write(&client, "#!/bin/sh\nprintf 'haider test-version\\n'\n")
        .expect("write fake haider client");
    let mut permissions = std::fs::metadata(&client)
        .expect("stat fake haider client")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&client, permissions).expect("make fake haider executable");

    let result = Command::new(env!("CARGO_BIN_EXE_hbench"))
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .env("PATH", &bin_dir)
        .arg("haider")
        .output()
        .expect("execute hbench with client-only Haider install");
    std::fs::remove_dir_all(&bin_dir).expect("remove doctor fixture");

    assert_ne!(result.status.code(), Some(0));
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(stderr.contains("harness haider not installed / adapter unvalidated"));
    assert!(stderr.contains("required executable \"haiderd\" does not exist"));
    assert!(!stderr.to_ascii_lowercase().contains("panicked"));
}
