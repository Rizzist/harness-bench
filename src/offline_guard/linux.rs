use super::*;

pub(super) fn build(
    _profile_root: &Path,
    base_url: &str,
    _provider_bind_address: &str,
    _delivery: &dyn LocalDeliveryOracle,
) -> Result<OfflineGuard> {
    let _provider = parse_loopback_http(base_url)?;
    // A private network namespace alone cannot reach AHRB's host-namespace
    // listener, while an unprivileged cgroup cannot attach the required BPF
    // connect policy. Until the supported Linux environment supplies the
    // reviewed namespace/cgroup launcher, fail closed instead of substituting
    // proxy variables or a differently confined probe.
    Err(AhrbError::Protocol(
        "reviewed Linux row-62 network namespace/cgroup guard is unavailable in this build; refusing proxy-only evidence"
            .to_owned(),
    ))
}
