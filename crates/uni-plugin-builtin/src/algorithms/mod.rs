//! Built-in algorithm registrations (M5c.1).
//!
//! Wraps every `uni_algo::AlgoProcedure` from
//! `uni_algo::algo::AlgorithmRegistry::new()` as an
//! `uni_plugin::traits::algorithm::AlgorithmProvider` and registers each under the
//! `uni.<name>` qname so the M4 adapter (`procedures_plugin::algo`)
//! finds them via `registry.iter_algorithms()`.
//!
//! Each algorithm bridges its `AlgoContext` requirements (storage +
//! L0 visibility) through the opaque
//! [`uni_plugin::traits::algorithm::AlgorithmHost`] callback. The host
//! implementation lives in `uni-query` (where `StorageManager` is
//! available); this crate's bridge only downcasts.
//!
//! Pregel programs registered here once executor lands (M5c follow-up).
//
// Rust guideline compliant

use uni_plugin::{PluginError, PluginRegistrar};

pub mod bridge;
pub mod reachability;

pub use bridge::{AlgoProviderBridge, AlgorithmHostBridge};
pub use reachability::ReachabilityProvider;

/// Register every built-in `uni.algo.*` algorithm into `r` as an
/// `uni_plugin::traits::algorithm::AlgorithmProvider`.
///
/// Source of truth: `uni_algo::algo::AlgorithmRegistry::new()` — the
/// same static registry the M4 adapter consults today. M5c.5 will
/// retire the static registry once all consumers have moved to the
/// plugin path.
///
/// # Errors
///
/// Returns [`PluginError`] only if a qname collides; with a fresh
/// registry no collisions are possible.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    use std::sync::Arc;
    use uni_plugin::qname::QName;

    let static_registry = uni_algo::algo::AlgorithmRegistry::new();
    for name in static_registry.list() {
        let Some(proc) = static_registry.get(name) else {
            continue;
        };
        // Algo names are like "algo.pageRank"; the registry already
        // namespaces by "uni" so we strip an optional leading "uni."
        // and use the rest as the QName local (matches the M4 adapter
        // in `uni-query::procedures_plugin::algo`).
        let local = name.strip_prefix("uni.").unwrap_or(name).to_owned();
        let qname = QName::new("uni", local);
        let provider = Arc::new(AlgoProviderBridge::new(proc));
        r.algorithm(qname, provider)?;
    }

    // First-party algorithm authored purely against the public
    // `AlgorithmProvider` + `GraphView` surface. Registered ONLY as a
    // provider (absent from the static `uni_algo` registry), so a
    // `CALL uni.algo.reachability(...)` routes through the provider
    // dispatch path rather than the M4 procedure adapter.
    r.algorithm(
        QName::new("uni", "algo.reachability"),
        Arc::new(ReachabilityProvider::new()),
    )?;

    Ok(())
}
