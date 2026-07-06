#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for crates/uni/src/api/impl_locy.rs:509 (finding [11]).
//!
//! `LocyEngine::evaluate_with_config_capturing` compiles via
//! `compile_only(program)`, whose branches call `compile`/
//! `compile_with_external_rules` — both resolve to `compile_with_context(...,
//! neural_predicates_preview = false, ...)`, hardcoding the flag and never
//! forwarding the `LocyConfig`. So `config.neural_predicates_preview` is
//! ignored on the transaction Locy path (TxLocyBuilder::run), while the
//! session path (`evaluate_with_db_and_config_capturing` →
//! `compile_with_config`) forwards it.
//!
//! `LocyConfig` defaults `neural_predicates_preview = true`, so a `CREATE
//! MODEL` program compiles on the session path but is rejected on the tx path
//! with `NeuralPreviewDisabled` regardless of the flag.

use uni_db::Uni;

const CREATE_MODEL: &str = "CREATE MODEL supplier_risk AS
    INPUT (s:Supplier)
    OUTPUT PROB risk
    USING xervo('classify/supplier-risk-v3')";

#[tokio::test]
async fn create_model_rejected_on_tx_path_despite_preview_default() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;

    // Session path: default config has neural_predicates_preview = true, and it
    // IS forwarded to the compiler → CREATE MODEL compiles cleanly.
    let session = db.session();
    let session_result = session.locy(CREATE_MODEL).await;
    assert!(
        session_result.is_ok(),
        "session path forwards preview=true → CREATE MODEL should compile; got {session_result:?}"
    );

    // FIXED (impl_locy.rs): the tx path now forwards the LocyConfig
    // (preview=true default) into compile_only_with_config, so CREATE MODEL
    // compiles on the tx path exactly like the session path.
    let tx = session.tx().await?;
    let tx_result = tx.locy_with(CREATE_MODEL).run().await;
    assert!(
        tx_result.is_ok(),
        "tx path must forward preview=true → CREATE MODEL should compile; got {tx_result:?}"
    );

    Ok(())
}
