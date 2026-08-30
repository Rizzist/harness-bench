use ahrb::Result;
use std::path::Path;

#[test]
fn external_reference_manifests_parse_and_validate() -> Result<()> {
    for adapter in [
        "haider-agent",
        "pi",
        "rick",
        "cline-cli",
        "opencode",
        "goose",
        "oh-my-pi",
        "deepseek-harness",
        "aider",
    ] {
        let path = format!("adapters/{adapter}/manifest.toml");
        let manifest = ahrb::manifest::load(Path::new(&path))?;
        assert_eq!(manifest.identity.id, adapter);
    }
    Ok(())
}
