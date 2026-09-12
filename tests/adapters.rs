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
    for command in [&claude.transport.command, &claude.sessions.resume] {
        assert_eq!(command.first().map(String::as_str), Some("/usr/bin/env"));
        assert!(
            command
                .iter()
                .any(|argument| argument == "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1")
        );
        assert!(command.iter().any(|argument| argument == "claude"));
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
