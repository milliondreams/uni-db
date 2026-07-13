#![allow(dead_code, unused_imports, clippy::all)]
//! M5c.1 — verify all 36 built-in algorithms are reachable through
//! `PluginRegistry::iter_algorithms()` / `registry.algorithm(qname)`.
//
// Rust guideline compliant

use uni_db::api::Uni;
use uni_plugin::qname::QName;

#[tokio::test(flavor = "multi_thread")]
async fn all_algorithms_registered_in_plugin_registry() {
    let uni = Uni::in_memory().build().await.expect("Uni::in_memory");
    let registry = uni.plugin_registry();

    let listed: Vec<QName> = registry
        .iter_algorithms()
        .into_iter()
        .map(|(q, _)| q)
        .collect();

    // The plugin registry contains every algorithm from the static
    // `uni-algo` registry (each registered as both a procedure adapter
    // and a provider) PLUS seven first-party providers authored directly
    // against `AlgorithmProvider` / `GraphView` and deliberately absent
    // from the static registry: `uni.algo.reachability`, `uni.algo.pagerank`,
    // `uni.algo.sssp` (Pregel), `uni.path.expand`, `uni.algo.gcpagerank`
    // (the GraphCompute kernel-driven Personalized PageRank),
    // `uni.algo.gcwalks` (GraphCompute node2vec/DeepWalk walk generation), and
    // `uni.algo.gcoverlap` (GraphCompute all-pairs neighbourhood overlap).
    const FIRST_PARTY_PROVIDERS: usize = 7;
    let static_registry = uni_algo::algo::AlgorithmRegistry::new();
    let expected_count = static_registry.list().len() + FIRST_PARTY_PROVIDERS;
    assert_eq!(
        listed.len(),
        expected_count,
        "registry must contain the {} static algorithms plus {FIRST_PARTY_PROVIDERS} first-party providers; got {}: {listed:?}",
        static_registry.list().len(),
        listed.len()
    );

    for name in static_registry.list() {
        let local = name.strip_prefix("uni.").unwrap_or(name);
        let qname = QName::new("uni", local);
        assert!(
            registry.algorithm(&qname).is_some(),
            "algorithm {name} (qname {qname:?}) must be registered"
        );
    }

    // The provider-only first-party algorithms resolve too.
    for local in ["algo.reachability", "algo.pagerank", "algo.sssp"] {
        assert!(
            registry.algorithm(&QName::new("uni", local)).is_some(),
            "the first-party uni.{local} provider must be registered"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn at_least_36_algorithms_registered() {
    let uni = Uni::in_memory().build().await.expect("Uni::in_memory");
    let count = uni.plugin_registry().iter_algorithms().len();
    assert!(
        count >= 36,
        "M5c.1 must register all 36 built-in algorithms; got {count}"
    );
}
