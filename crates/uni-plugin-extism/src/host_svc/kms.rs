//! KMS host fns — gated by [`uni_plugin::Capability::Kms`].
//!
//! `uni_kms_sign` and `uni_kms_verify` dispatch to the loader's
//! [`KmsProvider`](uni_plugin::KmsProvider). Bytes and signatures cross the
//! Extism boundary as lowercase hex inside a JSON envelope. Call-time
//! attenuation matches `key_id` against the granted `Capability::Kms`
//! allow-list; a missing provider or an out-of-list key errors loudly.

#![cfg(feature = "extism-runtime")]

use serde::{Deserialize, Serialize};
use uni_plugin::FnError;

use super::{HostSvcCtx, from_hex, to_hex};

/// `uni_kms_sign` request: hex-encoded `data` signed under `key_id`.
#[derive(Debug, Deserialize)]
struct SignReq {
    key_id: String,
    data_hex: String,
}

/// `uni_kms_sign` response: the hex-encoded signature.
#[derive(Debug, Serialize)]
struct SignResp {
    sig_hex: String,
}

/// `uni_kms_verify` request.
#[derive(Debug, Deserialize)]
struct VerifyReq {
    key_id: String,
    data_hex: String,
    sig_hex: String,
}

/// `uni_kms_verify` response.
#[derive(Debug, Serialize)]
struct VerifyResp {
    valid: bool,
}

/// Sign dispatch: attenuation check → provider call → hex-encode.
///
/// # Errors
///
/// Returns [`FnError`] when `key_id` is outside the granted `Kms` allow-list,
/// no provider is configured, the data hex is malformed, or the provider fails.
fn do_sign(ctx: &HostSvcCtx, req: SignReq) -> Result<SignResp, FnError> {
    if !ctx.effective.iter().any(|c| c.kms_allows(&req.key_id)) {
        return Err(FnError::new(
            0xC01,
            format!(
                "uni.kms.sign: key `{}` not in granted Kms allow-list",
                req.key_id
            ),
        ));
    }
    let kms = ctx
        .kms
        .as_ref()
        .ok_or_else(|| FnError::new(0xC02, "uni.kms.sign: no KMS provider configured"))?;
    let data = from_hex(&req.data_hex)
        .map_err(|e| FnError::new(0xC03, format!("uni.kms.sign: data hex: {e}")))?;
    let sig = kms.sign(&req.key_id, &data)?;
    Ok(SignResp {
        sig_hex: to_hex(&sig),
    })
}

/// Verify dispatch: attenuation check → provider call.
///
/// # Errors
///
/// Returns [`FnError`] as for [`do_sign`], plus malformed signature hex.
fn do_verify(ctx: &HostSvcCtx, req: VerifyReq) -> Result<VerifyResp, FnError> {
    if !ctx.effective.iter().any(|c| c.kms_allows(&req.key_id)) {
        return Err(FnError::new(
            0xC04,
            format!(
                "uni.kms.verify: key `{}` not in granted Kms allow-list",
                req.key_id
            ),
        ));
    }
    let kms = ctx
        .kms
        .as_ref()
        .ok_or_else(|| FnError::new(0xC05, "uni.kms.verify: no KMS provider configured"))?;
    let data = from_hex(&req.data_hex)
        .map_err(|e| FnError::new(0xC06, format!("uni.kms.verify: data hex: {e}")))?;
    let sig = from_hex(&req.sig_hex)
        .map_err(|e| FnError::new(0xC07, format!("uni.kms.verify: signature hex: {e}")))?;
    let valid = kms.verify(&req.key_id, &data, &sig)?;
    Ok(VerifyResp { valid })
}

// The `host_fn!`-generated fns are thin shells: parse JSON → dispatch → encode
// JSON. All attenuation/dispatch logic lives in the `do_*` fns above so it is
// unit-testable without a WASM guest.
extism::host_fn!(pub(crate) uni_kms_sign(ctx: HostSvcCtx; req_json: String) -> String {
    let bundle = ctx.get()?;
    let bundle = bundle
        .lock()
        .map_err(|_| extism::Error::msg("uni.kms.sign: host service ctx poisoned"))?;
    let req: SignReq = serde_json::from_str(&req_json)
        .map_err(|e| extism::Error::msg(format!("uni.kms.sign: bad request json: {e}")))?;
    let resp = do_sign(&bundle, req).map_err(|e| extism::Error::msg(e.to_string()))?;
    serde_json::to_string(&resp).map_err(|e| extism::Error::msg(e.to_string()))
});

extism::host_fn!(pub(crate) uni_kms_verify(ctx: HostSvcCtx; req_json: String) -> String {
    let bundle = ctx.get()?;
    let bundle = bundle
        .lock()
        .map_err(|_| extism::Error::msg("uni.kms.verify: host service ctx poisoned"))?;
    let req: VerifyReq = serde_json::from_str(&req_json)
        .map_err(|e| extism::Error::msg(format!("uni.kms.verify: bad request json: {e}")))?;
    let resp = do_verify(&bundle, req).map_err(|e| extism::Error::msg(e.to_string()))?;
    serde_json::to_string(&resp).map_err(|e| extism::Error::msg(e.to_string()))
});

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use uni_plugin::{Capability, CapabilitySet, KmsProvider};

    /// Deterministic fake: "signature" is the input bytes reversed.
    struct FakeKms;
    impl KmsProvider for FakeKms {
        fn sign(&self, _key_id: &str, data: &[u8]) -> Result<Vec<u8>, FnError> {
            Ok(data.iter().rev().copied().collect())
        }
        fn verify(&self, _key_id: &str, data: &[u8], signature: &[u8]) -> Result<bool, FnError> {
            let expected: Vec<u8> = data.iter().rev().copied().collect();
            Ok(expected == signature)
        }
    }

    fn ctx_with(caps: CapabilitySet, kms: Option<Arc<dyn KmsProvider>>) -> HostSvcCtx {
        HostSvcCtx {
            effective: caps,
            kms,
            secrets: None,
            http: None,
        }
    }

    fn kms_caps(pattern: &str) -> CapabilitySet {
        CapabilitySet::from_iter_of([Capability::Kms {
            key_ids: vec![pattern.into()],
        }])
    }

    #[test]
    fn sign_succeeds_when_key_in_allowlist() {
        let ctx = ctx_with(kms_caps("signing-*"), Some(Arc::new(FakeKms)));
        let resp = do_sign(
            &ctx,
            SignReq {
                key_id: "signing-1".into(),
                data_hex: "0102".into(),
            },
        )
        .expect("sign");
        // reverse of [0x01, 0x02] = [0x02, 0x01]
        assert_eq!(resp.sig_hex, "0201");
    }

    #[test]
    fn sign_denied_out_of_allowlist() {
        let ctx = ctx_with(kms_caps("signing-*"), Some(Arc::new(FakeKms)));
        let err = do_sign(
            &ctx,
            SignReq {
                key_id: "prod-master".into(),
                data_hex: "01".into(),
            },
        )
        .expect_err("must deny");
        assert!(err.message.contains("not in granted Kms allow-list"));
    }

    #[test]
    fn sign_fails_loudly_without_provider() {
        let ctx = ctx_with(kms_caps("*"), None);
        let err = do_sign(
            &ctx,
            SignReq {
                key_id: "k".into(),
                data_hex: "00".into(),
            },
        )
        .expect_err("no provider");
        assert!(err.message.contains("no KMS provider configured"));
    }

    #[test]
    fn verify_round_trips_against_sign() {
        let ctx = ctx_with(kms_caps("*"), Some(Arc::new(FakeKms)));
        let sig = do_sign(
            &ctx,
            SignReq {
                key_id: "k".into(),
                data_hex: "deadbeef".into(),
            },
        )
        .unwrap();
        let v = do_verify(
            &ctx,
            VerifyReq {
                key_id: "k".into(),
                data_hex: "deadbeef".into(),
                sig_hex: sig.sig_hex,
            },
        )
        .unwrap();
        assert!(v.valid);
    }
}
