//! Secret host fn — gated by [`uni_plugin::Capability::Secret`].
//!
//! `uni_secret_acquire` returns an opaque handle for a named secret, looked up
//! in the loader's [`SecretStore`](uni_plugin::secrets::SecretStore). Call-time
//! attenuation matches `id` against
//! the granted `Capability::Secret` allow-list; a missing store or an
//! out-of-list id errors loudly (never returns a fake handle).

#![cfg(feature = "extism-runtime")]

use serde::{Deserialize, Serialize};
use uni_plugin::FnError;

use super::HostSvcCtx;

/// `uni_secret_acquire` request.
#[derive(Debug, Deserialize)]
struct AcquireReq {
    id: String,
}

/// `uni_secret_acquire` response: the opaque handle id.
#[derive(Debug, Serialize)]
struct AcquireResp {
    opaque_id: u64,
}

/// Acquire dispatch: attenuation check → store lookup.
///
/// # Errors
///
/// Returns [`FnError`] when `id` is outside the granted `Secret` allow-list, no
/// store is configured, or the secret is absent from the store.
fn do_acquire(ctx: &HostSvcCtx, req: AcquireReq) -> Result<AcquireResp, FnError> {
    if !ctx.effective.iter().any(|c| c.secret_allows(&req.id)) {
        return Err(FnError::new(
            0xC10,
            format!(
                "uni.secret.acquire: id `{}` not in granted Secret allow-list",
                req.id
            ),
        ));
    }
    let store = ctx
        .secrets
        .as_ref()
        .ok_or_else(|| FnError::new(0xC11, "uni.secret.acquire: no secret store configured"))?;
    let handle = store.acquire(&req.id)?;
    Ok(AcquireResp {
        opaque_id: handle.opaque_id(),
    })
}

extism::host_fn!(pub(crate) uni_secret_acquire(ctx: HostSvcCtx; req_json: String) -> String {
    let bundle = ctx.get()?;
    let bundle = bundle
        .lock()
        .map_err(|_| extism::Error::msg("uni.secret.acquire: host service ctx poisoned"))?;
    super::dispatch_json(&bundle, &req_json, "uni.secret.acquire", do_acquire)
        .map_err(|e| extism::Error::msg(e.to_string()))
});

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use uni_plugin::secrets::SecretStore;
    use uni_plugin::{Capability, CapabilitySet};

    fn ctx_with(caps: CapabilitySet, store: Option<Arc<SecretStore>>) -> HostSvcCtx {
        HostSvcCtx {
            effective: caps,
            kms: None,
            secrets: store,
            http: None,
        }
    }

    fn secret_caps(pattern: &str) -> CapabilitySet {
        CapabilitySet::from_iter_of([Capability::Secret {
            ids: vec![pattern.into()],
        }])
    }

    #[test]
    fn acquire_returns_handle_when_granted() {
        let store = Arc::new(SecretStore::new());
        store.seal("db-password", b"hunter2".to_vec());
        let ctx = ctx_with(secret_caps("db-*"), Some(store));
        let resp = do_acquire(
            &ctx,
            AcquireReq {
                id: "db-password".into(),
            },
        )
        .expect("acquire");
        assert!(resp.opaque_id > 0);
    }

    #[test]
    fn acquire_denied_out_of_allowlist() {
        let store = Arc::new(SecretStore::new());
        store.seal("api-key", b"secret".to_vec());
        let ctx = ctx_with(secret_caps("db-*"), Some(store));
        let err = do_acquire(
            &ctx,
            AcquireReq {
                id: "api-key".into(),
            },
        )
        .expect_err("must deny");
        assert!(err.message.contains("not in granted Secret allow-list"));
    }

    #[test]
    fn acquire_fails_loudly_without_store() {
        let ctx = ctx_with(secret_caps("*"), None);
        let err = do_acquire(&ctx, AcquireReq { id: "x".into() }).expect_err("no store");
        assert!(err.message.contains("no secret store configured"));
    }
}
