use ahrb::Result;
use ahrb::manifest::TransportKind;
use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

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

#[test]
fn codex_manifest_maps_all_known_exec_tool_items_and_terminals() -> Result<()> {
    let manifest = ahrb::manifest::load(Path::new("adapters/codex/manifest.toml"))?;
    for item_type in [
        "command_execution",
        "function_call",
        "local_shell_call",
        "file_change",
        "patch_apply",
    ] {
        let started: Vec<_> = manifest
            .events
            .rules
            .iter()
            .filter(|rule| {
                rule.matches == "item.started"
                    && rule.match_fields.get("/item/type").map(String::as_str) == Some(item_type)
            })
            .collect();
        assert_eq!(started.len(), 1, "ambiguous started rule for {item_type}");
        assert_eq!(started[0].event, "tool-call");
        assert_eq!(started[0].payload_pointer, "/item");

        let completed: Vec<_> = manifest
            .events
            .rules
            .iter()
            .filter(|rule| {
                rule.matches == "item.completed"
                    && rule.match_fields.get("/item/type").map(String::as_str) == Some(item_type)
            })
            .collect();
        assert_eq!(
            completed.len(),
            1,
            "ambiguous completed rule for {item_type}"
        );
        assert_eq!(completed[0].event, "tool-result");
        assert_eq!(completed[0].payload_pointer, "/item");
    }
    assert!(manifest.events.rules.iter().all(|rule| {
        !matches!(rule.matches.as_str(), "item.started" | "item.completed")
            || !rule.match_fields.is_empty()
    }));
    assert!(manifest.events.rules.iter().any(|rule| {
        rule.matches == "item.completed"
            && rule.match_fields.get("/item/type").map(String::as_str) == Some("agent_message")
            && rule.event == "model-response"
    }));
    assert!(
        manifest
            .events
            .rules
            .iter()
            .any(|rule| { rule.matches == "turn.completed" && rule.event == "terminal-success" })
    );
    assert!(
        manifest
            .events
            .rules
            .iter()
            .any(|rule| { rule.matches == "turn.failed" && rule.event == "terminal-failure" })
    );
    Ok(())
}

#[test]
fn codex_manifest_binds_fixture_semantics_to_shell_argv() -> Result<()> {
    let manifest = ahrb::manifest::load(Path::new("adapters/codex/manifest.toml"))?;
    for semantic in ["write", "read", "fail"] {
        assert_eq!(
            manifest.tools.aliases.get(semantic).map(String::as_str),
            Some("shell")
        );
        assert!(manifest.tools.fixtures.contains_key(semantic));
    }
    assert_eq!(
        manifest
            .tools
            .bindings
            .get("command_argv")
            .map(String::as_str),
        Some("command")
    );
    Ok(())
}

#[test]
fn codex_fixture_templates_execute_write_read_and_fail_effects() -> Result<()> {
    let manifest = ahrb::manifest::load(Path::new("adapters/codex/manifest.toml"))?;
    let workspace =
        std::env::temp_dir().join(format!("ahrb-codex-fixture-test-{}", std::process::id()));
    if workspace.exists() {
        std::fs::remove_dir_all(&workspace)?;
    }
    std::fs::create_dir(&workspace)?;

    let run_fixture = |semantic: &str, values: &[(&str, &str)]| -> Result<_> {
        let mut variables = BTreeMap::from([(
            "ahrb_fixture".to_owned(),
            env!("CARGO_BIN_EXE_ahrb-fixture").to_owned(),
        )]);
        for (key, value) in values {
            variables.insert((*key).to_owned(), (*value).to_owned());
        }
        let argv = manifest.tools.fixtures[semantic]
            .iter()
            .map(|argument| ahrb::manifest::render_template(argument, &variables))
            .collect::<Result<Vec<_>>>()?;
        let (program, arguments) = argv.split_first().ok_or_else(|| {
            ahrb::AhrbError::Validation(format!("empty Codex fixture template {semantic:?}"))
        })?;
        Ok(Command::new(program)
            .args(arguments)
            .current_dir(&workspace)
            .output()?)
    };

    let write = run_fixture(
        "write",
        &[
            ("path", "nested/fixture.txt"),
            ("content", "fixture payload"),
        ],
    )?;
    assert!(write.status.success());
    assert_eq!(
        std::fs::read_to_string(workspace.join("nested/fixture.txt"))?,
        "fixture payload"
    );

    let read = run_fixture("read", &[("path", "nested/fixture.txt")])?;
    assert!(read.status.success());
    assert_eq!(read.stdout, b"fixture payload");

    let fail = run_fixture("fail", &[("message", "expected fixture failure")])?;
    assert_eq!(fail.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&fail.stderr).contains("expected fixture failure"));

    std::fs::remove_dir_all(workspace)?;
    Ok(())
}

#[test]
fn codex_manifest_overrides_context_and_ignores_nonfatal_metadata_items() -> Result<()> {
    let manifest = ahrb::manifest::load(Path::new("adapters/codex/manifest.toml"))?;
    for command in [&manifest.transport.command, &manifest.sessions.resume] {
        let positions: Vec<_> = command
            .iter()
            .enumerate()
            .filter_map(|(index, argument)| {
                (argument == "model_context_window=128000").then_some(index)
            })
            .collect();
        assert_eq!(positions.len(), 1);
        let position = positions[0];
        assert_eq!(
            command.get(position.wrapping_sub(1)).map(String::as_str),
            Some("-c")
        );
        let prompt = command
            .iter()
            .position(|argument| argument == "{{prompt}}")
            .expect("Codex command has prompt argument");
        assert!(position < prompt);
        if let Some(resume) = command.iter().position(|argument| argument == "resume") {
            assert!(position < resume);
        }
        assert!(
            command
                .iter()
                .all(|argument| !argument.contains("max_output"))
        );
    }
    assert_eq!(
        manifest.exit.success_stdout,
        ["\"type\":\"turn.completed\""]
    );
    assert_eq!(manifest.exit.failure_stdout, ["\"type\":\"turn.failed\""]);
    assert!(manifest.events.rules.iter().any(|rule| {
        rule.matches == "error" && rule.match_fields.is_empty() && rule.event == "terminal-failure"
    }));
    Ok(())
}
