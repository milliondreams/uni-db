// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Every provider uni-db advertises a `provider-*` feature for is registered.
//!
//! `build_model_runtime` gates each `register_provider` call behind its own
//! `#[cfg(feature = ...)]`, and the catalog validator rejects a `provider_id`
//! with no matching provider compiled in. Those two lists are maintained by
//! hand in different files — `crates/uni/Cargo.toml` and
//! `crates/uni/src/api/xervo.rs` — so a feature can exist while nothing
//! registers behind it. The symptom is not a build error: it is a catalog that
//! fails to open with "no matching provider", which reads like a user's typo.
//!
//! `remote/llamacpp` was exactly that gap (#256): uni-xervo shipped the
//! provider and uni-db exposed neither the feature nor the arm.

use serde_json::json;
use uni_db::xervo::build_model_runtime;
use uni_xervo::api::ModelAliasSpec;

/// Build a one-entry catalog for `provider_id` from its JSON form.
///
/// Constructed through serde rather than a struct literal because that is how a
/// catalog actually reaches this code — `Uni::open(...).xervo_catalog(...)`
/// takes it from configuration — so the test exercises the same defaults a
/// caller gets.
fn catalog(provider_id: &str, options: serde_json::Value) -> Vec<ModelAliasSpec> {
    vec![
        serde_json::from_value(json!({
            "alias": "embed/test",
            "task": "embed",
            "provider_id": provider_id,
            "model_id": "bge-small-en-v1.5",
            "options": options,
        }))
        .expect("catalog entry must deserialize"),
    ]
}

/// A `remote/llamacpp` catalog builds a runtime (#256).
///
/// Lazy warmup by default, so this reaches the provider registry and the
/// options validator without contacting a server — which is the half of the
/// issue's acceptance that can be checked without one running.
#[tokio::test]
async fn a_llamacpp_catalog_builds_a_runtime() {
    let rt = build_model_runtime(catalog(
        "remote/llamacpp",
        json!({
            "base_url": "http://127.0.0.1:8080",
            "max_input_tokens": 512,
            "embedding_dimensions": 384,
        }),
    ))
    .await;

    assert!(
        rt.is_ok(),
        "a remote/llamacpp catalog must build: {:?}",
        rt.err()
    );
}

/// The provider is reached, not merely accepted.
///
/// Omitting a required option must be refused by llama.cpp's own validator
/// rather than passing because nothing looked. Without this, the test above
/// would also pass if `remote/llamacpp` were silently treated as an unvalidated
/// provider id.
#[tokio::test]
async fn a_llamacpp_catalog_missing_an_option_is_refused() {
    let msg = match build_model_runtime(catalog(
        "remote/llamacpp",
        json!({ "base_url": "http://127.0.0.1:8080" }),
    ))
    .await
    {
        Ok(_) => panic!("a catalog missing required options must not build"),
        Err(e) => e.to_string(),
    };
    assert!(
        msg.contains("max_input_tokens") || msg.contains("embedding_dimensions"),
        "the refusal must name the missing option, so it came from the \
         provider's own validator: {msg}"
    );
}

/// An unknown provider id still fails, and says so.
///
/// The control: it is what a `remote/llamacpp` catalog did before this landed,
/// so if this ever starts passing the registry has stopped checking at all.
#[tokio::test]
async fn an_unknown_provider_id_is_still_refused() {
    let msg = match build_model_runtime(catalog("remote/not-a-provider", json!({}))).await {
        Ok(_) => panic!("an unknown provider id must not build"),
        Err(e) => e.to_string().to_lowercase(),
    };
    assert!(
        msg.contains("provider"),
        "the refusal must mention the provider: {msg}"
    );
}
