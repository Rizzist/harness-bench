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
        "cline",
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
    let legacy = ahrb::manifest::load(Path::new("adapters/haider-agent-legacy/manifest.toml"))?;
    assert_eq!(legacy.identity.id, "haider-agent");
    Ok(())
}

#[test]
fn wave4_target_manifests_parse_and_declare_typed_injection_surfaces() -> Result<()> {
    for adapter in [
        "codex",
        "claude-code",
        "opencode",
        "pi",
        "rick",
        "haider-agent",
        "mock",
        "mock-exec",
        "aider",
        "goose",
        "cline",
    ] {
        let path = format!("adapters/{adapter}/manifest.toml");
        let manifest = ahrb::manifest::load(Path::new(&path))?;
        assert!(
            manifest.capabilities.injection_surface.is_some(),
            "{adapter} must declare the typed Wave-4 injection surface"
        );
    }
    Ok(())
}

#[test]
fn extra_adapters_use_schema_two_and_isolated_headless_profiles() -> Result<()> {
    for adapter in ["aider", "goose", "cline"] {
        let path = format!("adapters/{adapter}/manifest.toml");
        let manifest = ahrb::manifest::load(Path::new(&path))?;
        assert_eq!(manifest.identity.schema, 2, "{adapter}");
        assert_eq!(manifest.transport.kind, TransportKind::Exec, "{adapter}");
        assert!(!manifest.daemon.persistent, "{adapter}");
        assert!(
            manifest
                .transport
                .command
                .iter()
                .any(|arg| arg == "{{prompt}}")
        );
        assert!(
            !manifest
                .transport
                .command
                .iter()
                .any(|arg| arg.contains("{{credential}}"))
        );
        assert!(manifest.isolation.roots.contains_key("XDG_CACHE_HOME"));
        assert!(manifest.capabilities.injection_surface.is_some());
        assert_eq!(manifest.input.prompt_uses_stdin, Some(false));
        assert!(
            manifest.capabilities.optional.is_empty(),
            "{adapter}: optional support needs evidence"
        );
    }
    Ok(())
}

#[test]
fn extra_adapter_result_rules_preserve_command_and_protocol_errors() -> Result<()> {
    use ahrb::events::{EventNormalizer, EventVocab};
    use serde_json::json;

    let mut goose = ahrb::manifest::load(Path::new("adapters/goose/manifest.toml"))?;
    // The exec driver supplies a numeric observation cursor before normalization.
    goose.events.cursor_pointer = "/cursor".to_owned();
    for (native, expected) in [
        (
            json!({"status":"success","value":{"structuredContent":{
                "exit_code":1,"stdout":"","stderr":"command failed"
            }}}),
            json!({"exit_code":1,"stdout":"","stderr":"command failed"}),
        ),
        (
            json!({"status":"error","error":"invalid tool arguments"}),
            json!({"status":"error","error":"invalid tool arguments"}),
        ),
    ] {
        let raw = json!({
            "type":"message","message":{"id":"record"},"cursor":1,
            "_ahrb_expanded":{"type":"toolResponse","id":"call","toolResult":native}
        });
        let event = EventNormalizer::default()
            .normalize(&raw, &goose.events)?
            .expect("tool response must normalize");
        assert_eq!(event.event, EventVocab::ToolResult);
        assert_eq!(event.payload["call_id"], "call");
        assert_eq!(event.payload["result"], expected);
    }

    let mut cline = ahrb::manifest::load(Path::new("adapters/cline/manifest.toml"))?;
    cline.events.cursor_pointer = "/cursor".to_owned();
    // The exec driver applies every matching rule, rather than stopping after
    // the first. Both native shapes must therefore match exactly one result rule.
    let normalize_all_results = |raw: &serde_json::Value| {
        cline
            .events
            .rules
            .iter()
            .filter(|rule| rule.event == "tool-result")
            .filter_map(|rule| {
                let mut mapping = cline.events.clone();
                mapping.rules = vec![rule.clone()];
                EventNormalizer::default()
                    .normalize(raw, &mapping)
                    .ok()
                    .flatten()
            })
            .collect::<Vec<_>>()
    };
    let result = json!({"success":false,"result":"command failed"});
    let raw = json!({"type":"agent_event","cursor":1,"event":{
        "type":"content_end","contentType":"tool","toolCallId":"call",
        "toolName":"run_commands","output":[result.clone()]
    }});
    let events = normalize_all_results(&raw);
    assert_eq!(events.len(), 1, "command result must not be duplicated");
    let event = &events[0];
    assert_eq!(event.event, EventVocab::ToolResult);
    assert_eq!(event.payload["result"], json!([result]));
    let error = json!({"error":"invalid JSON arguments"});
    let raw = json!({"type":"agent_event","cursor":1,"event":{
        "type":"content_end","contentType":"tool","toolCallId":"call",
        "toolName":"unknown_fixture","output":error.clone(),
        "error":"invalid JSON arguments"
    }});
    let events = normalize_all_results(&raw);
    assert_eq!(events.len(), 1, "protocol error must normalize once");
    let event = &events[0];
    assert_eq!(event.payload["result"], error);
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
        assert_eq!(manifest.events.source, "stdout");
        assert!(!manifest.events.rules.is_empty());
    }

    for adapter in ["codex", "claude-code", "opencode", "pi"] {
        let path = format!("adapters/{adapter}/manifest.toml");
        let manifest = ahrb::manifest::load(Path::new(&path))?;
        assert!(
            manifest
                .sessions
                .resume
                .iter()
                .any(|argument| argument.contains("{{session_id}}")),
            "{adapter} must reopen disk-backed session state"
        );
    }
    let rick = ahrb::manifest::load(Path::new("adapters/rick/manifest.toml"))?;
    assert!(rick.sessions.resume.is_empty());
    assert!(!rick.capabilities.required.contains_key("sessions"));
    assert!(!rick.capabilities.required.contains_key("resume"));

    let haider = ahrb::manifest::load(Path::new("adapters/haider-agent/manifest.toml"))?;
    assert!(haider.daemon.persistent);
    assert_eq!(haider.concurrency.topology, "shared-daemon-sessions");
    assert_eq!(haider.transport.kind, TransportKind::Exec);
    assert_eq!(haider.availability.exec_paths, ["haider"]);
    assert_eq!(haider.availability.required_exec_paths, ["haiderd"]);
    assert_eq!(haider.daemon.start, ["haider", "status", "--json"]);
    assert!(haider.daemon.launcher_exits);
    assert_eq!(
        haider
            .isolation
            .roots
            .get("XDG_RUNTIME_DIR")
            .map(String::as_str),
        Some("{{profile}}/run")
    );
    assert_eq!(haider.daemon.readiness.kind, "command-json");
    assert_eq!(
        haider.daemon.readiness.command,
        ["haider", "status", "--json"]
    );
    assert_eq!(haider.daemon.readiness.timeout_ms, 30_000);
    assert_eq!(haider.daemon.readiness.pid_pointer, "/daemon/pid");
    assert_eq!(haider.daemon.readiness.ready_pointer, "/daemon/ready");
    assert_eq!(
        haider
            .daemon
            .readiness
            .json_pointer_roots
            .get("/runtime_dir")
            .map(String::as_str),
        Some("HAIDER_RUNTIME_DIR")
    );
    assert!(
        haider
            .daemon
            .initialize
            .windows(3)
            .any(|arguments| { arguments == ["account", "add", "ahrb"] })
    );
    assert!(
        haider
            .transport
            .command
            .windows(2)
            .any(|arguments| { arguments == ["--output", "jsonl"] })
    );
    assert!(
        haider
            .sessions
            .continue_turn
            .windows(2)
            .any(|arguments| arguments == ["--session", "{{session_id}}"])
    );
    assert!(
        haider
            .sessions
            .continue_turn
            .windows(2)
            .any(|arguments| arguments == ["--output", "jsonl"])
    );
    assert_eq!(haider.sessions.id_pointer, "/session_id");
    assert_eq!(haider.sessions.run_id_pointer, "/run_id");
    // The bench account must carry a stored credential (a credential-less custom
    // provider is auto-hermetic by contract: lockdown fs scope, no process_exec),
    // and that credential must never travel on argv.
    assert!(
        haider
            .daemon
            .initialize
            .windows(2)
            .any(|arguments| { arguments == ["--api-key-env", "OPENAI_API_KEY"] })
    );
    assert!(
        !haider
            .daemon
            .initialize
            .iter()
            .any(|argument| argument == "--no-auth" || argument == "--api-key")
    );
    // Runs select the provider/model pair directly; `--account` must NOT appear
    // as a run flag (the `account add` in `initialize` registers the provider).
    assert!(
        haider
            .transport
            .command
            .windows(2)
            .any(|arguments| { arguments == ["--model", "ahrb/{{model}}"] })
    );
    assert!(
        !haider
            .transport
            .command
            .iter()
            .any(|argument| argument == "--account")
    );
    assert!(haider.sessions.create.is_empty());
    assert!(haider.sessions.submit.is_empty());
    Ok(())
}

#[test]
fn uncertain_external_contracts_are_marked_for_orchestrator_validation() -> Result<()> {
    for adapter in [
        "codex",
        "claude-code",
        "opencode",
        "pi",
        "rick",
        "haider-agent",
    ] {
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
fn codex_manifest_binds_fixture_semantics_to_declared_shell_variants() -> Result<()> {
    let manifest = ahrb::manifest::load(Path::new("adapters/codex/manifest.toml"))?;
    for semantic in ["write", "read", "fail"] {
        assert_eq!(
            manifest
                .tools
                .aliases
                .get(semantic)
                .map(|alias| alias.candidates()),
            Some(
                ["shell_command", "exec_command", "shell"]
                    .map(str::to_owned)
                    .as_slice()
            )
        );
        assert!(manifest.tools.fixtures.contains_key(semantic));
    }
    for (binding, field) in [
        ("shell_command.command", "command"),
        ("exec_command.command", "cmd"),
        ("shell.command_argv", "command"),
    ] {
        assert_eq!(
            manifest.tools.bindings.get(binding).map(String::as_str),
            Some(field)
        );
    }
    Ok(())
}

#[test]
fn fixture_templates_execute_write_read_and_fail_effects_for_native_adapters() -> Result<()> {
    for adapter in [
        "codex",
        "claude-code",
        "haider-agent",
        "opencode",
        "pi",
        "rick",
    ] {
        let manifest =
            ahrb::manifest::load(Path::new(&format!("adapters/{adapter}/manifest.toml")))?;
        let workspace = std::env::temp_dir().join(format!(
            "ahrb-{adapter}-fixture-test-{}",
            std::process::id()
        ));
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
                ahrb::AhrbError::Validation(format!(
                    "empty {adapter} fixture template {semantic:?}"
                ))
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
        assert!(write.status.success(), "{adapter}");
        assert_eq!(
            std::fs::read_to_string(workspace.join("nested/fixture.txt"))?,
            "fixture payload",
            "{adapter}"
        );

        let read = run_fixture("read", &[("path", "nested/fixture.txt")])?;
        assert!(read.status.success(), "{adapter}");
        assert_eq!(read.stdout, b"fixture payload", "{adapter}");

        let fail = run_fixture("fail", &[("message", "expected fixture failure")])?;
        assert_eq!(fail.status.code(), Some(1), "{adapter}");
        assert!(
            String::from_utf8_lossy(&fail.stderr).contains("expected fixture failure"),
            "{adapter}"
        );

        std::fs::remove_dir_all(workspace)?;
    }
    Ok(())
}

#[test]
fn remaining_native_adapters_pin_injection_tools_and_structured_events() -> Result<()> {
    let claude = ahrb::manifest::load(Path::new("adapters/claude-code/manifest.toml"))?;
    assert_eq!(claude.fake_model.allowed_paths, ["/v1/messages"]);
    assert_eq!(
        claude.availability.version_probe,
        ["claude", "--bare", "--version"]
    );
    let launcher = claude
        .isolation
        .generated_files
        .iter()
        .find(|file| file.path == "{{profile}}/bin/claude-ahrb-bare")
        .expect("profile-local Claude bare launcher");
    assert_eq!(launcher.mode, "0700");
    assert!(launcher.content.contains("sandbox_exec=$2"));
    assert!(
        launcher
            .content
            .contains("sandbox-exec unavailable on macOS")
    );
    assert!(launcher.content.contains("/usr/bin/security"));
    for command in [&claude.transport.command, &claude.sessions.resume] {
        assert_eq!(command.first().map(String::as_str), Some("/usr/bin/env"));
        assert!(
            command
                .iter()
                .any(|argument| argument == "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1")
        );
        assert!(
            command
                .iter()
                .any(|argument| argument == "CLAUDE_CODE_SIMPLE=1")
        );
        assert!(
            command
                .iter()
                .any(|argument| argument == "{{profile}}/bin/claude-ahrb-bare")
        );
        assert!(
            command
                .windows(2)
                .any(|arguments| { arguments == ["/usr/bin/uname", "/usr/bin/sandbox-exec"] })
        );
        assert!(command.iter().any(|argument| argument == "claude"));
        assert!(command.iter().any(|argument| argument == "--bare"));
    }
    assert_eq!(claude.tools.aliases["write"].primary(), Some("Bash"));
    assert_eq!(
        claude
            .tools
            .bindings
            .get("Bash.command")
            .map(String::as_str),
        Some("command")
    );
    assert!(claude.events.rules.iter().any(|rule| {
        rule.event == "tool-result"
            && rule.payload_bindings.get("call_id").map(String::as_str)
                == Some("/_ahrb_expanded/tool_use_id")
    }));
    assert!(claude.events.rules.iter().any(|rule| {
        rule.matches == "result"
            && rule.match_fields.get("/is_error").map(String::as_str) == Some("true")
            && rule.event == "terminal-failure"
    }));
    for subtype in [
        "error_max_turns",
        "error_during_execution",
        "error_max_budget_usd",
        "error_max_structured_output_retries",
    ] {
        assert!(claude.events.rules.iter().any(|rule| {
            rule.matches == "result"
                && rule.match_fields.get("/subtype").map(String::as_str) == Some(subtype)
                && rule.event == "terminal-failure"
        }));
    }

    let pi = ahrb::manifest::load(Path::new("adapters/pi/manifest.toml"))?;
    assert_eq!(pi.fake_model.allowed_paths, ["/v1/chat/completions"]);
    assert_eq!(pi.sessions.id_pointer, "/id");
    assert_eq!(
        pi.sessions.store_paths,
        ["{{profile}}/home/.pi/agent/sessions"]
    );
    for command in [&pi.transport.command, &pi.sessions.resume] {
        assert!(!command.iter().any(|argument| argument == "--no-session"));
        assert!(command.windows(2).any(|arguments| {
            arguments == ["--session-dir", "{{profile}}/home/.pi/agent/sessions"]
        }));
    }
    assert!(
        pi.sessions
            .resume
            .windows(2)
            .any(|arguments| arguments == ["--session", "{{session_id}}"])
    );
    assert!(pi.capabilities.required.contains_key("sessions"));
    assert!(pi.capabilities.required.contains_key("resume"));
    assert_eq!(pi.isolation.generated_files.len(), 2);
    assert!(pi.isolation.generated_files.iter().any(|file| {
        file.path.ends_with("/.pi/agent/models.json")
            && file.content.contains("supportsDeveloperRole")
            && file.content.contains("{{base_url}}/v1")
    }));
    assert!(pi.isolation.generated_files.iter().any(|file| {
        file.path.ends_with("/.pi/agent/auth.json") && file.content.contains("{{credential}}")
    }));
    assert!(pi.events.rules.iter().any(|rule| {
        rule.matches == "tool_execution_end"
            && rule.payload_bindings.get("call_id").map(String::as_str) == Some("/toolCallId")
    }));
    assert!(
        pi.events
            .rules
            .iter()
            .any(|rule| { rule.matches == "agent_start" && rule.event == "turn-accepted" })
    );

    let rick = ahrb::manifest::load(Path::new("adapters/rick/manifest.toml"))?;
    assert_eq!(rick.fake_model.allowed_paths, ["/v1/chat/completions"]);
    assert_eq!(rick.isolation.generated_files.len(), 1);
    let config = &rick.isolation.generated_files[0];
    assert!(config.path.ends_with("/home/.config/rick/rick.json"));
    assert!(config.content.contains("\"provider\""));
    assert!(config.content.contains("\"type\": \"openai-compatible\""));
    assert_eq!(rick.tools.aliases["write"].primary(), Some("bash"));
    assert!(rick.events.rules.iter().any(|rule| {
        rule.matches == "tool_end" && rule.event == "tool-result" && rule.payload_pointer == "/tool"
    }));

    let opencode = ahrb::manifest::load(Path::new("adapters/opencode/manifest.toml"))?;
    assert_eq!(opencode.fake_model.allowed_paths, ["/v1/chat/completions"]);
    assert_eq!(opencode.isolation.generated_files.len(), 1);
    let config = &opencode.isolation.generated_files[0];
    assert!(config.path.ends_with("/config/opencode/opencode.json"));
    assert!(!opencode.isolation.roots.contains_key("OPENCODE_CONFIG"));
    assert_eq!(
        opencode
            .isolation
            .environment
            .get("OPENCODE_CONFIG")
            .map(String::as_str),
        Some("{{profile}}/config/opencode/opencode.json")
    );
    assert!(config.content.contains("@ai-sdk/openai-compatible"));
    assert!(config.content.contains("{{base_url}}/v1"));
    assert!(config.content.contains("\"{{model}}\""));
    let config_json: serde_json::Value = serde_json::from_str(&config.content)?;
    assert_eq!(
        config_json.pointer("/agent/title/disable"),
        Some(&serde_json::Value::Bool(true))
    );
    assert!(config_json.get("small_model").is_none());
    assert!(!opencode.model_roles.contains_key("title"));
    for command in [&opencode.transport.command, &opencode.sessions.resume] {
        assert!(command.windows(2).any(|pair| pair == ["--format", "json"]));
        assert!(command.iter().any(|argument| argument == "--auto"));
        assert!(command.iter().all(|argument| argument != "--json"));
        assert!(command.iter().all(|argument| argument != "--pure"));
    }
    assert_eq!(opencode.tools.aliases["write"].primary(), Some("bash"));
    assert_eq!(
        opencode
            .tools
            .bindings
            .get("bash.command")
            .map(String::as_str),
        Some("command")
    );
    for status in ["completed", "error"] {
        assert!(opencode.events.rules.iter().any(|rule| {
            rule.matches == "tool_use"
                && rule
                    .match_fields
                    .get("/part/state/status")
                    .map(String::as_str)
                    == Some(status)
                && rule.event == "tool-call"
                && rule.payload_bindings.get("call_id").map(String::as_str) == Some("/part/callID")
        }));
        assert!(opencode.events.rules.iter().any(|rule| {
            rule.matches == "tool_use"
                && rule
                    .match_fields
                    .get("/part/state/status")
                    .map(String::as_str)
                    == Some(status)
                && rule.event == "tool-result"
                && rule.payload_bindings.get("result").map(String::as_str) == Some("/part/state")
        }));
    }
    assert!(opencode.events.rules.iter().any(|rule| {
        rule.matches == "step_finish"
            && rule.match_fields.get("/part/reason").map(String::as_str) == Some("stop")
            && rule.event == "terminal-success"
    }));
    assert!(
        opencode
            .events
            .rules
            .iter()
            .any(|rule| { rule.matches == "error" && rule.event == "terminal-failure" })
    );

    let haider = ahrb::manifest::load(Path::new("adapters/haider-agent/manifest.toml"))?;
    assert_eq!(
        haider.fake_model.allowed_paths,
        ["/v1/models", "/v1/chat/completions"]
    );
    assert!(!haider.fake_model.auth_required);
    assert_eq!(
        haider.tools.aliases["write"].primary(),
        Some("process_exec")
    );
    assert_eq!(
        haider
            .tools
            .bindings
            .get("process_exec.command")
            .map(String::as_str),
        Some("command")
    );
    assert_eq!(haider.events.type_pointer, "/payload/type");
    assert_eq!(haider.events.schema_version_pointer, "/schema_version");
    assert_eq!(haider.events.schema_versions, [1]);
    assert!(haider.events.warn_unmapped_payload_kinds);
    assert_eq!(haider.events.replay_mode, "document");
    assert_eq!(haider.events.replay_records_pointer, "/events");
    assert!(haider.events.replay_envelope_pointer.is_empty());
    assert_eq!(
        haider
            .events
            .replay_assertions
            .get("/provider_requests")
            .map(String::as_str),
        Some("0")
    );
    assert!(haider.events.replay_compare_live_records);
    assert_eq!(
        haider.events.replay_state_pointers,
        ["/result/head_seq", "/result/terminal_seq"]
    );
    assert_eq!(haider.events.replay_cursor_start, Some(2));
    // The tool call maps from the `started` item: the daemon journals the
    // tool_result before the `completed` tool_call item (verified on 0.0.967).
    assert!(haider.events.rules.iter().any(|rule| {
        rule.matches == "item"
            && rule.match_fields.get("/payload/event").map(String::as_str) == Some("started")
            && rule
                .match_fields
                .get("/payload/item/item")
                .map(String::as_str)
                == Some("tool_call")
            && rule.event == "tool-call"
    }));
    assert!(!haider.events.rules.iter().any(|rule| {
        rule.matches == "item"
            && rule.match_fields.get("/payload/event").map(String::as_str) == Some("completed")
            && rule
                .match_fields
                .get("/payload/item/item")
                .map(String::as_str)
                == Some("tool_call")
    }));
    assert!(haider.events.rules.iter().any(|rule| {
        rule.matches == "tool_result"
            && rule.event == "tool-result"
            && rule.payload_bindings.get("call_id").map(String::as_str) == Some("/payload/call_id")
    }));
    // Terminals key on the DURABLE `state`; `terminal_kind`/`error_code` exist
    // only on the live carrier, and a rule keyed on them makes `run --replay`
    // one event shorter than the live stream (verified on 0.0.967).
    for (state, event) in [
        ("done", "terminal-success"),
        ("errored", "terminal-failure"),
        ("cancelled", "terminal-cancelled"),
    ] {
        assert!(haider.events.rules.iter().any(|rule| {
            rule.matches == "run_state"
                && rule.match_fields.get("/payload/state").map(String::as_str) == Some(state)
                && rule.event == event
        }));
    }
    assert!(!haider.events.rules.iter().any(|rule| {
        rule.match_fields.contains_key("/payload/terminal_kind")
            || rule.match_fields.contains_key("/payload/error_code")
    }));
    // `run_failed` is the adjacent cause record, never a second typed terminal.
    assert!(
        !haider
            .events
            .rules
            .iter()
            .any(|rule| rule.matches == "run_failed" && rule.event.starts_with("terminal-"))
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn claude_launcher_fails_closed_and_wraps_on_macos() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    use std::time::{SystemTime, UNIX_EPOCH};

    let claude = ahrb::manifest::load(Path::new("adapters/claude-code/manifest.toml"))?;
    let launcher = claude
        .isolation
        .generated_files
        .iter()
        .find(|file| file.path == "{{profile}}/bin/claude-ahrb-bare")
        .expect("profile-local Claude bare launcher");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after Unix epoch")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "ahrb-claude-launcher-test-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root)?;

    let write_executable = |path: &Path, content: &str| -> std::io::Result<()> {
        std::fs::write(path, content)?;
        let mut permissions = std::fs::metadata(path)?.permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(path, permissions)
    };
    let launcher_path = root.join("claude-ahrb-bare");
    let platform_probe = root.join("platform-probe");
    let sandbox_exec = root.join("sandbox-exec");
    let fake_claude = root.join("claude");
    let missing_sandbox = root.join("missing-sandbox-exec");
    let claude_marker = root.join("claude-ran");
    let sandbox_log = root.join("sandbox-argv");
    write_executable(&launcher_path, &launcher.content)?;
    write_executable(
        &platform_probe,
        "#!/bin/sh\nset -eu\nprintf '%s\\n' \"$AHRB_TEST_PLATFORM\"\n",
    )?;
    write_executable(
        &sandbox_exec,
        "#!/bin/sh\nset -eu\nprintf '%s\\n' \"$@\" > \"$AHRB_TEST_SANDBOX_LOG\"\n[ \"$1\" = -p ]\nshift 2\nexec \"$@\"\n",
    )?;
    write_executable(
        &fake_claude,
        "#!/bin/sh\nset -eu\n: > \"$AHRB_TEST_CLAUDE_MARKER\"\nprintf '%s\\n' wrapped-claude\n",
    )?;

    let run_launcher = |platform: &str, sandbox: &Path| {
        Command::new("/bin/sh")
            .arg(&launcher_path)
            .arg(&platform_probe)
            .arg(sandbox)
            .arg(&fake_claude)
            .env("AHRB_TEST_PLATFORM", platform)
            .env("AHRB_TEST_CLAUDE_MARKER", &claude_marker)
            .env("AHRB_TEST_SANDBOX_LOG", &sandbox_log)
            .output()
    };

    let missing = run_launcher("Darwin", &missing_sandbox)?;
    assert!(!missing.status.success());
    assert_eq!(missing.status.code(), Some(126));
    assert_eq!(
        String::from_utf8_lossy(&missing.stderr).trim(),
        "ahrb claude-code launcher: sandbox-exec unavailable on macOS; refusing to run claude without the keychain-lookup guard"
    );
    assert!(
        !claude_marker.exists(),
        "missing sandbox-exec must not launch Claude"
    );

    let wrapped = run_launcher("Darwin", &sandbox_exec)?;
    assert!(wrapped.status.success());
    assert_eq!(String::from_utf8_lossy(&wrapped.stdout), "wrapped-claude\n");
    assert!(claude_marker.exists(), "sandbox wrapper must launch Claude");
    let sandbox_arguments = std::fs::read_to_string(&sandbox_log)?;
    let mut sandbox_arguments = sandbox_arguments.lines();
    assert_eq!(sandbox_arguments.next(), Some("-p"));
    assert_eq!(
        sandbox_arguments.next(),
        Some("(version 1) (allow default) (deny process-exec (literal \"/usr/bin/security\"))")
    );
    assert_eq!(sandbox_arguments.next(), fake_claude.to_str());

    std::fs::remove_file(&claude_marker)?;
    std::fs::remove_file(&sandbox_log)?;
    let non_macos = run_launcher("Linux", &missing_sandbox)?;
    assert!(non_macos.status.success());
    assert_eq!(
        String::from_utf8_lossy(&non_macos.stdout),
        "wrapped-claude\n"
    );
    assert!(
        claude_marker.exists(),
        "non-macOS launcher must execute Claude directly"
    );
    assert!(
        !sandbox_log.exists(),
        "non-macOS launcher must not invoke sandbox-exec"
    );

    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn codex_launcher_denies_owner_config_and_fails_closed_on_macos() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    use std::time::{SystemTime, UNIX_EPOCH};

    let codex = ahrb::manifest::load(Path::new("adapters/codex/manifest.toml"))?;
    assert_eq!(codex.availability.version_probe, ["codex", "--version"]);
    let launcher = codex
        .isolation
        .generated_files
        .iter()
        .find(|file| file.path == "{{profile}}/bin/codex-ahrb-isolated")
        .expect("profile-local Codex isolation launcher");
    assert_eq!(launcher.mode, "0700");
    for command in [&codex.transport.command, &codex.sessions.resume] {
        assert_eq!(
            command.first().map(String::as_str),
            Some("{{profile}}/bin/codex-ahrb-isolated")
        );
        assert_eq!(
            &command[1..5],
            [
                "/usr/bin/uname",
                "/usr/bin/sandbox-exec",
                "/usr/bin/id",
                "/usr/bin/dscl",
            ]
        );
        assert_eq!(command.get(5).map(String::as_str), Some("codex"));
    }

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after Unix epoch")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "ahrb-codex-launcher-test-{}-{nonce}",
        std::process::id()
    ));
    let real_home = root.join("real-home");
    let codex_dir = real_home.join(".codex/packages/bin");
    std::fs::create_dir_all(&codex_dir)?;

    let write_executable = |path: &Path, content: &str| -> std::io::Result<()> {
        std::fs::write(path, content)?;
        let mut permissions = std::fs::metadata(path)?.permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(path, permissions)
    };
    let launcher_path = root.join("codex-ahrb-isolated");
    let platform_probe = root.join("platform-probe");
    let sandbox_exec = root.join("sandbox-exec");
    let account_probe = root.join("account-probe");
    let directory_service = root.join("directory-service");
    let missing_sandbox = root.join("missing-sandbox-exec");
    let missing_directory_service = root.join("missing-directory-service");
    let fake_codex = codex_dir.join("codex");
    let codex_marker = root.join("codex-ran");
    let environment_log = root.join("codex-environment");
    let sandbox_log = root.join("sandbox-argv");
    write_executable(&launcher_path, &launcher.content)?;
    write_executable(
        &platform_probe,
        "#!/bin/sh\nset -eu\nprintf '%s\\n' \"$AHRB_TEST_PLATFORM\"\n",
    )?;
    write_executable(
        &sandbox_exec,
        "#!/bin/sh\nset -eu\nprintf '%s\\n' \"$@\" > \"$AHRB_TEST_SANDBOX_LOG\"\n[ \"$1\" = -p ]\nshift 2\nexec \"$@\"\n",
    )?;
    write_executable(
        &account_probe,
        "#!/bin/sh\nset -eu\n[ \"$1\" = -un ]\nprintf '%s\\n' ahrb-test-account\n",
    )?;
    write_executable(
        &directory_service,
        "#!/bin/sh\nset -eu\nprintf 'NFSHomeDirectory: %s\\n' \"$AHRB_TEST_REAL_HOME\"\n",
    )?;
    write_executable(
        &fake_codex,
        "#!/bin/sh\nset -eu\n: > \"$AHRB_TEST_CODEX_MARKER\"\nprintf '%s|%s|%s|%s\\n' \"${CODEX_ACCESS_TOKEN-unset}\" \"${OPENAI_IDENTITY_TOKEN_FILE-unset}\" \"${OPENAI_FEDERATION_RULE_ID-unset}\" \"${OPENAI_WORKLOAD_IDENTITY_CONTEXT-unset}\" > \"$AHRB_TEST_ENVIRONMENT_LOG\"\nprintf '%s\\n' wrapped-codex\n",
    )?;

    let run_launcher = |platform: &str, sandbox: &Path, directory: &Path| {
        Command::new("/bin/sh")
            .arg(&launcher_path)
            .arg(&platform_probe)
            .arg(sandbox)
            .arg(&account_probe)
            .arg(directory)
            .arg(&fake_codex)
            .env("AHRB_TEST_PLATFORM", platform)
            .env("AHRB_TEST_REAL_HOME", &real_home)
            .env("AHRB_TEST_CODEX_MARKER", &codex_marker)
            .env("AHRB_TEST_ENVIRONMENT_LOG", &environment_log)
            .env("AHRB_TEST_SANDBOX_LOG", &sandbox_log)
            .env("CODEX_ACCESS_TOKEN", "owner-token-must-not-propagate")
            .env("OPENAI_IDENTITY_TOKEN_FILE", "/owner/identity-token")
            .env("OPENAI_FEDERATION_RULE_ID", "owner-rule")
            .env("OPENAI_WORKLOAD_IDENTITY_CONTEXT", "owner-context")
            .output()
    };

    let missing = run_launcher("Darwin", &missing_sandbox, &directory_service)?;
    assert_eq!(missing.status.code(), Some(126));
    assert!(!codex_marker.exists());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("sandbox-exec unavailable"));

    let missing_home = run_launcher("Darwin", &sandbox_exec, &missing_directory_service)?;
    assert_eq!(missing_home.status.code(), Some(126));
    assert!(!codex_marker.exists());
    assert!(String::from_utf8_lossy(&missing_home.stderr).contains("account database omitted"));

    let wrapped = run_launcher("Darwin", &sandbox_exec, &directory_service)?;
    assert!(wrapped.status.success());
    assert_eq!(String::from_utf8_lossy(&wrapped.stdout), "wrapped-codex\n");
    assert!(codex_marker.exists());
    assert_eq!(
        std::fs::read_to_string(&environment_log)?,
        "unset|unset|unset|unset\n"
    );
    let sandbox_arguments = std::fs::read_to_string(&sandbox_log)?;
    let mut sandbox_arguments = sandbox_arguments.lines();
    assert_eq!(sandbox_arguments.next(), Some("-p"));
    let policy = sandbox_arguments.next().expect("sandbox policy");
    assert!(policy.contains(&format!(
        "(literal \"{}/.codex/config.toml\")",
        real_home.display()
    )));
    assert!(policy.contains(&format!(
        "(literal \"{}/.codex/auth.json\")",
        real_home.display()
    )));
    assert!(!policy.contains("(subpath"));
    assert_eq!(sandbox_arguments.next(), fake_codex.to_str());

    std::fs::remove_file(&codex_marker)?;
    std::fs::remove_file(&environment_log)?;
    std::fs::remove_file(&sandbox_log)?;
    let non_macos = run_launcher("Linux", &missing_sandbox, &missing_directory_service)?;
    assert!(non_macos.status.success());
    assert!(codex_marker.exists());
    assert_eq!(
        std::fs::read_to_string(&environment_log)?,
        "owner-token-must-not-propagate|/owner/identity-token|owner-rule|owner-context\n"
    );
    assert!(!sandbox_log.exists());

    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn remaining_adapter_provider_files_render_as_scoped_valid_json() -> Result<()> {
    let profile =
        std::env::temp_dir().join(format!("ahrb-provider-render-test-{}", std::process::id()));
    let variables = BTreeMap::from([
        ("profile".to_owned(), profile.to_string_lossy().into_owned()),
        ("base_url".to_owned(), "http://127.0.0.1:43123".to_owned()),
        ("credential".to_owned(), "dummy".to_owned()),
        ("model".to_owned(), "ahrb-fake-v1".to_owned()),
    ]);

    for adapter in ["opencode", "pi", "rick"] {
        let manifest =
            ahrb::manifest::load(Path::new(&format!("adapters/{adapter}/manifest.toml")))?;
        assert!(!manifest.isolation.generated_files.is_empty(), "{adapter}");
        for specification in &manifest.isolation.generated_files {
            let path = ahrb::manifest::render_template(&specification.path, &variables)?;
            assert!(Path::new(&path).starts_with(&profile), "{adapter}: {path}");
            let content = ahrb::manifest::render_template(&specification.content, &variables)?;
            assert!(!content.contains("{{"), "{adapter}: {content}");
            let _: serde_json::Value = serde_json::from_str(&content)?;
        }
    }
    Ok(())
}

#[test]
fn codex_manifest_overrides_context_and_ignores_nonfatal_metadata_items() -> Result<()> {
    let manifest = ahrb::manifest::load(Path::new("adapters/codex/manifest.toml"))?;
    for command in [&manifest.transport.command, &manifest.sessions.resume] {
        assert!(
            command
                .windows(2)
                .any(|arguments| arguments == ["--profile", "ahrb"])
        );
        let prompt = command
            .iter()
            .position(|argument| argument == "{{prompt}}")
            .expect("Codex command has prompt argument");
        let profile = command
            .iter()
            .position(|argument| argument == "--profile")
            .expect("Codex command has generated profile argument");
        assert!(profile < prompt);
        if let Some(resume) = command.iter().position(|argument| argument == "resume") {
            assert!(profile < resume);
        }
        assert!(
            command
                .iter()
                .all(|argument| !argument.contains("max_output"))
        );
    }
    let profile = manifest
        .isolation
        .generated_files
        .iter()
        .find(|file| file.path == "{{profile}}/codex/ahrb.config.toml")
        .expect("Codex generated provider profile");
    let parsed: toml::Value = toml::from_str(&profile.content)?;
    assert_eq!(parsed["model_context_window"].as_integer(), Some(128_000));
    assert_eq!(
        parsed["model_providers"]["ahrb"]["base_url"].as_str(),
        Some("{{base_url}}/v1")
    );
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

#[test]
fn versioned_haider_manifests_separate_continuation_from_legacy_controls() {
    let manifest = ahrb::manifest::load(Path::new("adapters/haider-agent/manifest.toml")).unwrap();
    assert_eq!(
        manifest.identity.revision,
        "0.0.972-torn-tail-capability-v1"
    );
    assert_eq!(
        manifest.resources.log_paths.as_ref().unwrap(),
        &["{{profile}}/home/.haider/dev-profile/daemon.log"]
    );
    assert!(
        manifest
            .resources
            .log_paths
            .as_ref()
            .unwrap()
            .iter()
            .all(|path| path.contains("{{profile}}/home/.haider/dev-profile/"))
    );
    assert!(!manifest.sessions.continue_turn.is_empty());
    assert!(manifest.sessions.close_delete.is_empty());
    assert!(manifest.capture.truncation_marker.is_none());
    // Row-47 session-journal attribution uses the typed journal locator, not logs.
    assert_eq!(
        manifest.resources.journal_paths.as_ref().unwrap(),
        &[
            "{{profile}}/home/.haider/dev-profile/store.sqlite",
            "{{profile}}/home/.haider/dev-profile/store.sqlite-wal",
        ]
    );

    let legacy =
        ahrb::manifest::load(Path::new("adapters/haider-agent-legacy/manifest.toml")).unwrap();
    assert_eq!(
        legacy.identity.revision,
        "0.0.970-0.0.971-measurement-capability-audit-legacy-v1"
    );
    assert!(legacy.sessions.continue_turn.is_empty());
    assert_eq!(
        legacy.sessions.resume_control,
        manifest.sessions.resume_control
    );
    assert_eq!(
        legacy.sessions.recover_probe,
        manifest.sessions.recover_probe
    );
    assert!(legacy.capture.truncation_marker.is_none());
}

#[test]
fn six_harnesses_declare_row47_log_evidence() -> Result<()> {
    let expected = [
        (
            "codex",
            vec![
                "{{profile}}/codex/logs_1.sqlite",
                "{{profile}}/codex/logs_1.sqlite-shm",
                "{{profile}}/codex/logs_1.sqlite-wal",
                "{{profile}}/codex/logs_2.sqlite",
                "{{profile}}/codex/logs_2.sqlite-shm",
                "{{profile}}/codex/logs_2.sqlite-wal",
            ],
        ),
        ("claude-code", vec![]),
        (
            "opencode",
            vec!["{{profile}}/data/opencode/log/opencode.log"],
        ),
        ("pi", vec![]),
        ("rick", vec![]),
        (
            "haider-agent",
            vec!["{{profile}}/home/.haider/dev-profile/daemon.log"],
        ),
    ];
    for (adapter, paths) in expected {
        let manifest =
            ahrb::manifest::load(Path::new(&format!("adapters/{adapter}/manifest.toml")))?;
        let paths = paths.into_iter().map(str::to_owned).collect::<Vec<_>>();
        assert_eq!(
            manifest.resources.log_paths.as_deref(),
            Some(paths.as_slice())
        );
    }
    Ok(())
}

#[test]
fn codex_and_opencode_row65_carriers_run_before_positional_prompts() -> Result<()> {
    let codex = ahrb::manifest::load(Path::new("adapters/codex/manifest.toml"))?;
    let surface = codex.capabilities.injection_surface.as_ref().unwrap();
    assert_eq!(
        surface.provider.argv_position,
        ahrb::manifest::ArgvPosition::ReplaceOption
    );
    assert_eq!(
        surface.base_url.method,
        ahrb::manifest::InjectionMethod::GeneratedConfig
    );
    assert_eq!(
        surface.base_url.json_pointer,
        "/model_providers/ahrb/base_url"
    );

    let opencode = ahrb::manifest::load(Path::new("adapters/opencode/manifest.toml"))?;
    let provider = &opencode
        .capabilities
        .injection_surface
        .as_ref()
        .unwrap()
        .provider;
    assert_eq!(
        provider.method,
        ahrb::manifest::InjectionMethod::GeneratedConfig
    );
    assert_eq!(
        provider.json_pointer,
        "/provider/ahrb/models/ahrb-fake-v1/id"
    );
    Ok(())
}

#[test]
fn six_harness_storage_declarations_are_evidence_scoped() -> Result<()> {
    let load = |adapter: &str| {
        ahrb::manifest::load(Path::new(&format!("adapters/{adapter}/manifest.toml")))
    };
    let strings = |values: &[&str]| values.iter().map(|v| (*v).to_owned()).collect::<Vec<_>>();

    // Verbs only where a public, disposable-profile-scoped surface was proven;
    // neither CLI takes a store-root argument, so scope is the isolated profile.
    let codex = load("codex")?;
    let storage = codex.storage.as_ref().expect("codex [storage]");
    let delete = storage.session_delete.as_ref().expect("codex delete");
    assert_eq!(
        delete,
        &strings(&[
            "{{harness}}",
            "delete",
            "--force",
            "--disable",
            "plugins",
            "{{session_id}}"
        ])
    );
    assert!(ahrb::storage::scoped_by_environment(
        "session_delete",
        delete
    ));
    assert!(
        codex
            .sessions
            .store_paths
            .contains(&"{{profile}}/codex/sessions".to_owned())
    );
    for command in [&codex.transport.command, &codex.sessions.resume] {
        assert!(
            command
                .windows(2)
                .any(|a| a == ["--disable", "shell_snapshot"])
        );
        assert!(
            command
                .windows(2)
                .any(|a| a == ["-c", "allow_login_shell=false"])
        );
    }
    let areas = storage.areas.as_ref().expect("codex areas");
    assert_eq!(areas["transient"], ["codex/.tmp/**"]);
    assert!(areas["store"].contains(&"codex/sessions/**".to_owned()));

    let opencode = load("opencode")?;
    let storage = opencode.storage.as_ref().expect("opencode [storage]");
    assert_eq!(
        storage.session_delete.as_ref().expect("opencode delete"),
        &strings(&["{{harness}}", "session", "delete", "{{session_id}}"])
    );
    assert!(!opencode.sessions.store_paths.is_empty());

    for adapter in ["claude-code", "pi"] {
        let manifest = load(adapter)?;
        let storage = manifest.storage.as_ref().expect("[storage] areas");
        assert!(!storage.areas.as_ref().expect("areas")["store"].is_empty());
    }
    // Rick has no headless continuation. Haider 0.0.972 has continuation but
    // storage declarations stay absent while daemon.log aliases a rotated file
    // by hard link; the capture must report that repeated identity honestly.
    for adapter in ["rick", "haider-agent"] {
        assert!(load(adapter)?.storage.is_none(), "{adapter}");
    }
    for adapter in [
        "codex",
        "claude-code",
        "opencode",
        "pi",
        "rick",
        "haider-agent",
    ] {
        let manifest = load(adapter)?;
        let Some(storage) = manifest.storage.as_ref() else {
            continue;
        };
        storage.validate()?;
        // No uninstall, close, caps or sweep are documented for any of the six.
        assert!(storage.uninstall_cleanup.is_none(), "{adapter}");
        assert!(storage.session_close.is_none(), "{adapter}");
        assert!(storage.retention_cap_bytes.is_none(), "{adapter}");
        assert!(storage.auxiliary_cap_bytes.is_none(), "{adapter}");
        assert!(storage.sweep_interval_s.is_none(), "{adapter}");
    }
    Ok(())
}
