//! `checked_call` — runtime capability choke point for Extism host fns.
//!
//! The Component Model loader gates host imports by **linker absence**:
//! a plugin without the right capability literally cannot resolve the
//! host function symbol. Extism, lacking that structural enforcement, has
//! to gate at call time. This module ships the canonical helper every
//! gateable host-fn body invokes first:
//!
//! ```ignore
//! pub fn host_fs_read(plugin: &mut CurrentPlugin, args: ..., grants: &[String])
//!     -> Result<Vec<u8>, ExtismError>
//! {
//!     checked_call(registry, grants, "host_fs_read", || {
//!         // actual filesystem read
//!     })
//! }
//! ```
//!
//! Per proposal §5.6.2, threading this single helper through every host
//! fn turns "did the author remember to check the capability" from a
//! per-fn correctness concern into a one-line wrapping that's
//! grep-able and consistent across the host-fn surface.
//!
//! ### Defense-in-depth, not the primary gate
//!
//! `ExtismLoader::build_plugin` already filters the plugin's import
//! table by effective capability set — a plugin without `Filesystem`
//! never sees `host_fs_read` in its imports (M6a.1.1). `checked_call`
//! is the *second* line of defense: if a host author accidentally adds
//! a host fn to the import set without capability gating, this helper
//! still rejects unauthorized calls at runtime.

use crate::error::ExtismError;
use crate::host_fns::HostFnRegistry;

/// Invoke `body` only if the plugin's `grants` include the
/// capability `host_fn` requires (per its [`HostFnSpec`]).
///
/// Lookup semantics:
/// - If `host_fn` is not registered in `registry`, returns
///   [`ExtismError::InvalidPlugin`] — the host author has wired an
///   unregistered fn; this is a bug in the host, not the plugin.
/// - If the spec's `required_capability` is `None` (always-available
///   fn), `body` runs unconditionally.
/// - If the required capability is **not** in `grants`, returns
///   [`ExtismError::CapabilityDenied`] *without* invoking `body`.
/// - Otherwise `body()` runs and its result propagates.
///
/// # Errors
///
/// See variants enumerated above.
///
/// [`HostFnSpec`]: crate::host_fns::HostFnSpec
pub fn checked_call<F, R>(
    registry: &HostFnRegistry,
    grants: &[String],
    host_fn: &str,
    body: F,
) -> Result<R, ExtismError>
where
    F: FnOnce() -> Result<R, ExtismError>,
{
    let spec = registry
        .get(host_fn)
        .ok_or_else(|| ExtismError::InvalidPlugin(format!("unknown host fn `{host_fn}`")))?;
    if let Some(cap) = &spec.required_capability
        && !grants.iter().any(|g| g == cap)
    {
        return Err(ExtismError::CapabilityDenied {
            host_fn: host_fn.to_owned(),
            capability: cap.clone(),
        });
    }
    body()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host_fns::HostFnSpec;

    fn fs_registry() -> HostFnRegistry {
        let mut r = HostFnRegistry::new();
        r.register(HostFnSpec {
            name: "host_fs_read".to_owned(),
            required_capability: Some("Filesystem".to_owned()),
            docs: "Read file.".to_owned(),
        });
        r.register(HostFnSpec {
            name: "host_log".to_owned(),
            required_capability: None,
            docs: "Always available.".to_owned(),
        });
        r
    }

    #[test]
    fn always_available_fn_runs_without_grants() {
        let r = fs_registry();
        let result = checked_call(&r, &[], "host_log", || Ok::<_, ExtismError>(42));
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn gated_fn_with_matching_grant_runs_body() {
        let r = fs_registry();
        let grants = vec!["Filesystem".to_owned()];
        let out = checked_call(&r, &grants, "host_fs_read", || {
            Ok::<_, ExtismError>(vec![0_u8, 1, 2])
        })
        .unwrap();
        assert_eq!(out, vec![0, 1, 2]);
    }

    #[test]
    fn gated_fn_without_grant_denies_without_running_body() {
        let r = fs_registry();
        let mut body_ran = false;
        let err = checked_call(&r, &[], "host_fs_read", || {
            body_ran = true;
            Ok::<_, ExtismError>(())
        })
        .unwrap_err();
        match err {
            ExtismError::CapabilityDenied {
                host_fn,
                capability,
            } => {
                assert_eq!(host_fn, "host_fs_read");
                assert_eq!(capability, "Filesystem");
            }
            other => panic!("expected CapabilityDenied, got: {other:?}"),
        }
        assert!(!body_ran, "body must not execute when capability is denied");
    }

    #[test]
    fn unregistered_fn_returns_invalid_plugin_not_denied() {
        let r = fs_registry();
        let err = checked_call(&r, &[], "host_mystery", || Ok::<_, ExtismError>(())).unwrap_err();
        assert!(
            matches!(err, ExtismError::InvalidPlugin(_)),
            "expected InvalidPlugin (host bug), got: {err:?}"
        );
    }

    #[test]
    fn body_error_propagates_through_helper() {
        let r = fs_registry();
        let grants = vec!["Filesystem".to_owned()];
        let err = checked_call(&r, &grants, "host_fs_read", || {
            Err::<(), _>(ExtismError::MemoryExchange("simulated".to_owned()))
        })
        .unwrap_err();
        assert!(matches!(err, ExtismError::MemoryExchange(_)));
    }

    #[test]
    fn extra_grants_dont_change_outcome() {
        // Plugin granted superset of what host fn needs — still passes.
        let r = fs_registry();
        let grants = vec![
            "Filesystem".to_owned(),
            "Network".to_owned(),
            "Kms".to_owned(),
        ];
        let res = checked_call(&r, &grants, "host_fs_read", || Ok::<_, ExtismError>(7));
        assert_eq!(res.unwrap(), 7);
    }
}
