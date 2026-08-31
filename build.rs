use std::process::Command;

fn main() {
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
