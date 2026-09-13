use std::process::Command;

fn main() {
    build_durability_shim();
    println!("cargo:rerun-if-env-changed=AHRB_REVISION");
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs/heads");

    let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR");
    let mut git = Command::new("git");
    if let Some(directory) = manifest_dir {
        git.current_dir(directory);
    }
    let git_revision = git
        .args(["rev-parse", "--verify", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|revision| revision.trim().to_owned())
        .filter(|revision| !revision.is_empty());
    let revision = git_revision
        .or_else(|| std::env::var("AHRB_REVISION").ok())
        .map(|revision| revision.trim().to_owned())
        .filter(|revision| {
            !revision.is_empty()
                && revision
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
        })
        .unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=AHRB_BUILD_REVISION={revision}");
}

// The system C compiler is already part of the Rust native-link toolchain.
// Embed both libraries so installed ahrb binaries do not depend on target/.
fn build_durability_shim() {
    for file in ["control.c", "shim.c"] {
        println!("cargo:rerun-if-changed=src/storage/durability/{file}");
    }
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos")
        || std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() != Ok("aarch64")
    {
        return;
    }
    let out = std::path::PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR"));
    let control = out.join("libahrb-durability-control.dylib");
    let shim = out.join("libahrb-durability.dylib");
    let status = Command::new("cc")
        .args([
            "-dynamiclib",
            "-arch",
            "arm64",
            "-O2",
            "-Wall",
            "-Wextra",
            "-Werror",
            "-install_name",
            "@loader_path/libahrb-durability-control.dylib",
            "src/storage/durability/control.c",
            "-o",
        ])
        .arg(&control)
        .status()
        .expect("compile durability control");
    assert!(status.success(), "durability control compilation failed");
    let status = Command::new("cc")
        .args([
            "-dynamiclib",
            "-arch",
            "arm64",
            "-O2",
            "-Wall",
            "-Wextra",
            "-Werror",
            "src/storage/durability/shim.c",
            "-o",
        ])
        .arg(&shim)
        .arg(&control)
        .status()
        .expect("compile durability shim");
    assert!(status.success(), "durability shim compilation failed");
}
