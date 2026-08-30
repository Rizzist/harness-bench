use ahrb::Result;
use ahrb::manifest::TransportKind;
use std::path::Path;

#[test]
fn external_reference_manifests_parse_and_validate() -> Result<()> {
    for adapter in [
        "codex",
        "claude-code",
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

#[test]
fn named_harness_adapters_declare_honest_architectures_and_exec_contracts() -> Result<()> {
    for adapter in ["codex", "claude-code", "opencode", "pi", "rick"] {
        let path = format!("adapters/{adapter}/manifest.toml");
        let manifest = ahrb::manifest::load(Path::new(&path))?;
        assert_eq!(manifest.transport.kind, TransportKind::Exec, "{adapter}");
        assert!(!manifest.daemon.persistent, "{adapter}");
        assert_eq!(manifest.concurrency.topology, "client-process-fanout");
        assert!(
            manifest
                .transport
                .command
                .iter()
                .any(|argument| argument.contains("{{prompt}}")),
            "{adapter} initial command must carry the route-marker prompt"
        );
        assert!(
            manifest
                .sessions
                .resume
                .iter()
                .any(|argument| argument.contains("{{session_id}}")),
            "{adapter} must reopen disk-backed session state"
        );
        assert_eq!(manifest.events.source, "stdout");
        assert!(!manifest.events.rules.is_empty());
    }

    let haider = ahrb::manifest::load(Path::new("adapters/haider-agent/manifest.toml"))?;
    assert!(haider.daemon.persistent);
    assert_eq!(haider.concurrency.topology, "shared-daemon-sessions");
    assert_eq!(haider.transport.kind, TransportKind::SocketJsonrpc);
    assert_eq!(haider.availability.exec_paths, ["haider"]);
    assert_eq!(haider.availability.required_exec_paths, ["haiderd"]);
    assert!(haider.sessions.create.is_empty());
    assert!(haider.sessions.submit.is_empty());
    Ok(())
}

#[test]
fn uncertain_external_contracts_are_marked_for_orchestrator_validation() -> Result<()> {
    for adapter in ["codex", "claude-code", "opencode", "rick", "haider-agent"] {
        let path = format!("adapters/{adapter}/manifest.toml");
        let source = std::fs::read_to_string(&path)?;
        assert!(
            source.contains("TODO(orchestrator):"),
            "{adapter} must state its remaining real-binary uncertainty"
        );
    }
    Ok(())
}
