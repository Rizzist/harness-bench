use ahrb::evaluate::TestOutcome;
use ahrb::matrix_evidence::{CapabilityStatus, capability_for_row};
use std::path::Path;

#[test]
fn topology_and_persistence_must_describe_the_same_architecture() {
    let mut daemon = ahrb::manifest::load(Path::new("adapters/mock/manifest.toml"))
        .expect("load daemon manifest");
    daemon.daemon.persistent = false;
    let error = ahrb::manifest::validate(&daemon)
        .expect_err("shared-daemon topology cannot claim transient lifecycle");
    assert!(
        error
            .to_string()
            .contains("conflicts with concurrency.topology")
    );

    let mut invocation = ahrb::manifest::load(Path::new("adapters/mock-exec/manifest.toml"))
        .expect("load exec manifest");
    invocation.daemon.persistent = true;
    let error = ahrb::manifest::validate(&invocation)
        .expect_err("client fan-out cannot claim a resident daemon lifecycle");
    assert!(
        error
            .to_string()
            .contains("conflicts with concurrency.topology")
    );
}

#[test]
fn credential_argv_requires_an_explicit_capture_policy_opt_in() {
    let mut manifest = ahrb::manifest::load(Path::new("adapters/mock-exec/manifest.toml"))
        .expect("load opted-in exec manifest");
    manifest.capture.allow_credential_argv = false;
    let error = ahrb::manifest::validate(&manifest)
        .expect_err("credential argv must remain rejected by default");
    assert!(error.to_string().contains("credential value in argv"));
}

#[test]
fn cold_isolation_requires_profile_scoped_home_and_xdg_roots() {
    let mut manifest =
        ahrb::manifest::load(Path::new("adapters/mock/manifest.toml")).expect("load mock manifest");
    manifest.isolation.roots.remove("XDG_STATE_HOME");
    let error = ahrb::manifest::validate(&manifest)
        .expect_err("missing state root must not share ambient daemon state");
    assert!(error.to_string().contains("XDG_STATE_HOME"));

    let mut ambient =
        ahrb::manifest::load(Path::new("adapters/mock/manifest.toml")).expect("load mock manifest");
    ambient
        .isolation
        .roots
        .insert("HOME".to_owned(), "/tmp/prewarmed-home".to_owned());
    let error = ahrb::manifest::validate(&ambient).expect_err("ambient home must not be accepted");
    assert!(error.to_string().contains("{{profile}}"));

    let mut traversal =
        ahrb::manifest::load(Path::new("adapters/mock/manifest.toml")).expect("load mock manifest");
    traversal
        .isolation
        .roots
        .insert("HOME".to_owned(), "{{profile}}/../shared-home".to_owned());
    let error = ahrb::manifest::validate(&traversal)
        .expect_err("profile-prefixed traversal must not escape the cold root");
    assert!(error.to_string().contains("lexically contained"));

    let mut extra_root =
        ahrb::manifest::load(Path::new("adapters/mock/manifest.toml")).expect("load mock manifest");
    extra_root
        .isolation
        .roots
        .insert("TMPDIR".to_owned(), "/tmp/shared-daemon-tmp".to_owned());
    let error = ahrb::manifest::validate(&extra_root)
        .expect_err("every configured isolation root must remain in the cold profile");
    assert!(error.to_string().contains("TMPDIR"));

    let mut generated =
        ahrb::manifest::load(Path::new("adapters/mock/manifest.toml")).expect("load mock manifest");
    generated
        .isolation
        .generated_files
        .push(ahrb::manifest::GeneratedFile {
            path: "{{profile}}/../shared-config.toml".to_owned(),
            content: "cold = false".to_owned(),
            mode: "0600".to_owned(),
        });
    let error = ahrb::manifest::validate(&generated)
        .expect_err("generated config traversal must not escape the cold root");
    assert!(error.to_string().contains("generated file"));
}

#[test]
fn per_invocation_crash_and_journal_trials_require_declared_disk_surfaces() {
    let manifest = ahrb::manifest::load(Path::new("adapters/mock-exec/manifest.toml"))
        .expect("load exec manifest");
    for row in [35_u8, 40] {
        assert!(matches!(
            capability_for_row(&manifest, row),
            CapabilityStatus::Supported
        ));
    }

    let mut without_resume = manifest.clone();
    without_resume.capabilities.required.remove("resume");
    assert!(matches!(
        capability_for_row(&without_resume, 35),
        CapabilityStatus::Unsupported(_)
    ));

    let mut without_journal = manifest;
    without_journal
        .capabilities
        .required
        .remove("durable_journal");
    assert!(matches!(
        capability_for_row(&without_journal, 40),
        CapabilityStatus::Unsupported(_)
    ));

    let unsupported = TestOutcome::Unsupported("disk surface is absent".to_owned());
    assert_eq!(
        serde_json::to_value(unsupported).expect("serialize")["class"],
        "UNSUPPORTED"
    );
}
