//! Network host fns — gated by [`uni_plugin::Capability::Network`].
//!
//! `uni_http_get` and `uni_http_post` dispatch to the loader's
//! [`HttpEgress`](uni_plugin::HttpEgress). Call-time attenuation matches the
//! requested URL against the granted `Capability::Network` patterns *before*
//! any socket is opened; the per-call timeout is taken from
//! `Capability::WallClockMillisPerCall` (else a conservative default), and the
//! response body is size-capped. A missing egress or an out-of-allow-list URL
//! errors loudly, as does an HTTP status `>= 400` (parity with the Rhai loader).
//!
//! Bodies cross the boundary as lowercase hex inside a JSON envelope so binary
//! payloads survive intact.

#![cfg(feature = "extism-runtime")]

use std::time::Duration;

use serde::{Deserialize, Serialize};
use uni_plugin::{Capability, FnError};

use super::{HostSvcCtx, from_hex, to_hex};

/// Default per-call HTTP timeout when the grant carries no
/// `WallClockMillisPerCall`.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum response body bytes read before truncation — bounds host memory.
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// `uni_http_get` / `uni_http_post` request. `body_hex` present ⇒ POST.
#[derive(Debug, Deserialize)]
struct HttpReq {
    url: String,
    #[serde(default)]
    body_hex: Option<String>,
}

/// HTTP response: status plus hex-encoded body.
#[derive(Debug, Serialize)]
struct HttpResp {
    status: u16,
    body_hex: String,
}

/// Shared GET/POST dispatch: enforce allow-list, resolve timeout, dispatch.
///
/// `traceparent` is computed by the macro shell (the host's active W3C trace
/// context) and threaded to the egress so the trace propagates across the guest
/// boundary; it is a parameter here purely so the dispatch is unit-testable.
///
/// # Errors
///
/// Returns [`FnError`] when the URL is outside the granted `Network`
/// allow-list, no egress is configured, the request body hex is malformed, the
/// transport fails, or the response status is `>= 400`.
fn do_http(ctx: &HostSvcCtx, req: HttpReq, traceparent: Option<&str>) -> Result<HttpResp, FnError> {
    if !ctx.effective.iter().any(|c| c.network_allows(&req.url)) {
        return Err(FnError::new(
            0xC20,
            format!(
                "uni.http: url `{}` not in granted Network allow-list",
                req.url
            ),
        ));
    }
    let egress = ctx
        .http
        .as_ref()
        .ok_or_else(|| FnError::new(0xC21, "uni.http: no HTTP egress configured"))?;
    let timeout = ctx
        .effective
        .iter()
        .find_map(|c| match c {
            Capability::WallClockMillisPerCall(ms) => Some(Duration::from_millis(*ms)),
            _ => None,
        })
        .unwrap_or(DEFAULT_TIMEOUT);
    let response = match &req.body_hex {
        Some(h) => {
            let body = from_hex(h)
                .map_err(|e| FnError::new(0xC22, format!("uni.http.post: body hex: {e}")))?;
            egress.post(&req.url, &body, timeout, MAX_RESPONSE_BYTES, traceparent)?
        }
        None => egress.get(&req.url, timeout, MAX_RESPONSE_BYTES, traceparent)?,
    };
    if response.status >= 400 {
        return Err(FnError::new(
            0xC23,
            format!("uni.http(`{}`): HTTP status {}", req.url, response.status),
        ));
    }
    Ok(HttpResp {
        status: response.status,
        body_hex: to_hex(&response.body),
    })
}

extism::host_fn!(pub(crate) uni_http_get(ctx: HostSvcCtx; req_json: String) -> String {
    let bundle = ctx.get()?;
    let bundle = bundle
        .lock()
        .map_err(|_| extism::Error::msg("uni.http.get: host service ctx poisoned"))?;
    let req: HttpReq = serde_json::from_str(&req_json)
        .map_err(|e| extism::Error::msg(format!("uni.http.get: bad request json: {e}")))?;
    // Propagate the host's trace context (real only in `otel`-enabled builds;
    // `None` otherwise — no fabricated trace ids).
    let traceparent = uni_plugin::observability::current_trace_context().to_traceparent();
    let resp = do_http(&bundle, req, traceparent.as_deref())
        .map_err(|e| extism::Error::msg(e.to_string()))?;
    serde_json::to_string(&resp).map_err(|e| extism::Error::msg(e.to_string()))
});

extism::host_fn!(pub(crate) uni_http_post(ctx: HostSvcCtx; req_json: String) -> String {
    let bundle = ctx.get()?;
    let bundle = bundle
        .lock()
        .map_err(|_| extism::Error::msg("uni.http.post: host service ctx poisoned"))?;
    let req: HttpReq = serde_json::from_str(&req_json)
        .map_err(|e| extism::Error::msg(format!("uni.http.post: bad request json: {e}")))?;
    let traceparent = uni_plugin::observability::current_trace_context().to_traceparent();
    let resp = do_http(&bundle, req, traceparent.as_deref())
        .map_err(|e| extism::Error::msg(e.to_string()))?;
    serde_json::to_string(&resp).map_err(|e| extism::Error::msg(e.to_string()))
});

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::Mutex;
    use uni_plugin::{CapabilitySet, HttpEgress, HttpResponse};

    /// Fake egress that records the traceparent it was handed and echoes the URL.
    struct RecordingHttp {
        last_traceparent: Mutex<Option<String>>,
        status: u16,
    }
    impl RecordingHttp {
        fn new(status: u16) -> Self {
            Self {
                last_traceparent: Mutex::new(None),
                status,
            }
        }
    }
    impl HttpEgress for RecordingHttp {
        fn get(
            &self,
            url: &str,
            _t: Duration,
            _m: usize,
            tp: Option<&str>,
        ) -> Result<HttpResponse, FnError> {
            *self.last_traceparent.lock().unwrap() = tp.map(str::to_owned);
            Ok(HttpResponse {
                status: self.status,
                body: format!("GET {url}").into_bytes(),
            })
        }
        fn post(
            &self,
            url: &str,
            body: &[u8],
            _t: Duration,
            _m: usize,
            tp: Option<&str>,
        ) -> Result<HttpResponse, FnError> {
            *self.last_traceparent.lock().unwrap() = tp.map(str::to_owned);
            Ok(HttpResponse {
                status: self.status,
                body: format!("POST {url} {}", body.len()).into_bytes(),
            })
        }
    }

    fn net_caps(pattern: &str) -> CapabilitySet {
        CapabilitySet::from_iter_of([Capability::Network {
            allow: vec![pattern.into()],
        }])
    }

    fn ctx_with(caps: CapabilitySet, http: Option<Arc<dyn HttpEgress>>) -> HostSvcCtx {
        HostSvcCtx {
            effective: caps,
            kms: None,
            secrets: None,
            http,
        }
    }

    #[test]
    fn get_succeeds_and_injects_traceparent() {
        let egress = Arc::new(RecordingHttp::new(200));
        let ctx = ctx_with(net_caps("https://api.example.com/**"), Some(egress.clone()));
        let resp = do_http(
            &ctx,
            HttpReq {
                url: "https://api.example.com/v1/x".into(),
                body_hex: None,
            },
            Some("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"),
        )
        .expect("get");
        assert_eq!(resp.status, 200);
        assert_eq!(
            *egress.last_traceparent.lock().unwrap(),
            Some("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01".to_owned()),
            "host-active traceparent must reach the egress"
        );
    }

    #[test]
    fn denied_out_of_allowlist() {
        let egress = Arc::new(RecordingHttp::new(200));
        let ctx = ctx_with(net_caps("https://api.example.com/**"), Some(egress));
        let err = do_http(
            &ctx,
            HttpReq {
                url: "https://evil.test/".into(),
                body_hex: None,
            },
            None,
        )
        .expect_err("must deny");
        assert!(err.message.contains("not in granted Network allow-list"));
    }

    #[test]
    fn fails_loudly_without_egress() {
        let ctx = ctx_with(net_caps("**"), None);
        let err = do_http(
            &ctx,
            HttpReq {
                url: "https://x/".into(),
                body_hex: None,
            },
            None,
        )
        .expect_err("no egress");
        assert!(err.message.contains("no HTTP egress configured"));
    }

    #[test]
    fn status_4xx_is_loud_error() {
        let egress = Arc::new(RecordingHttp::new(404));
        let ctx = ctx_with(net_caps("**"), Some(egress));
        let err = do_http(
            &ctx,
            HttpReq {
                url: "https://x/missing".into(),
                body_hex: None,
            },
            None,
        )
        .expect_err("4xx");
        assert!(err.message.contains("HTTP status 404"));
    }

    #[test]
    fn post_carries_body() {
        let egress = Arc::new(RecordingHttp::new(200));
        let ctx = ctx_with(net_caps("**"), Some(egress));
        // body_hex "414243" = "ABC" (3 bytes)
        let resp = do_http(
            &ctx,
            HttpReq {
                url: "https://x/".into(),
                body_hex: Some("414243".into()),
            },
            None,
        )
        .expect("post");
        let body = String::from_utf8(from_hex(&resp.body_hex).unwrap()).unwrap();
        assert_eq!(body, "POST https://x/ 3");
    }
}
