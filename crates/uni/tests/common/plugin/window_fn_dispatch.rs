// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end test for the (previously dead) `WindowPluginFn` surface.
//!
//! Registers a third-party window function and calls it from a Cypher
//! `... OVER (PARTITION BY ...)` clause, proving `df_planner`'s
//! "Unsupported window function" fallthrough now resolves plugin window fns
//! through the `PluginWindowUdwf` adapter.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use arrow_array::{ArrayRef, Float64Array, RecordBatch};
use datafusion::logical_expr::Volatility;
use uni_db::{DataType, Uni};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::traits::window::{WindowFrame, WindowPluginFn, WindowSignature};
use uni_plugin::{
    AbiRange, Capability, CapabilitySet, Determinism, FnError, Plugin, PluginError, PluginManifest,
    PluginRegistrar, ProvidedSurfaces, QName, Scope, SideEffects,
};

/// `myplugin.partition_sum(x)` — every row gets the sum of `x` over its whole
/// partition (a trivial whole-partition window function).
#[derive(Debug)]
struct PartitionSum {
    sig: WindowSignature,
}

impl PartitionSum {
    fn new() -> Self {
        Self {
            sig: WindowSignature {
                args: vec![ArgType::Primitive(arrow_schema::DataType::Float64)],
                returns: ArgType::Primitive(arrow_schema::DataType::Float64),
                volatility: Volatility::Immutable,
            },
        }
    }
}

impl WindowPluginFn for PartitionSum {
    fn signature(&self) -> &WindowSignature {
        &self.sig
    }
    fn evaluate(&self, partition: &RecordBatch, frame: WindowFrame) -> Result<ArrayRef, FnError> {
        let col = partition
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| FnError::new(0, "partition_sum expects a float column"))?;
        let sum: f64 = (frame.start..frame.end).map(|i| col.value(i)).sum();
        let n = frame.end - frame.start;
        Ok(Arc::new(Float64Array::from(vec![sum; n])))
    }
}

struct WindowPlugin {
    manifest: OnceLock<PluginManifest>,
}

impl Plugin for WindowPlugin {
    fn manifest(&self) -> &PluginManifest {
        self.manifest.get_or_init(|| PluginManifest {
            id: uni_plugin::PluginId::new("myplugin"),
            version: "0.1.0".parse().expect("static version"),
            abi: AbiRange::parse("^1").expect("static abi"),
            depends_on: vec![],
            capabilities: CapabilitySet::from_iter_of([Capability::WindowFn]),
            determinism: Determinism::Pure,
            side_effects: SideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: ProvidedSurfaces::default(),
            docs: "third-party window function (test)".to_owned(),
            metadata: BTreeMap::new(),
        })
    }

    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        r.window_fn(
            QName::new("myplugin", "partition_sum"),
            PartitionSum::new().sig,
            Arc::new(PartitionSum::new()),
        )?;
        Ok(())
    }
}

/// A registered plugin window function is dispatched via `OVER (PARTITION BY)`.
#[tokio::test]
async fn plugin_window_fn_dispatches_over_partition() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.add_plugin(WindowPlugin {
        manifest: OnceLock::new(),
    })?;

    db.schema()
        .label("Sale")
        .property("region", DataType::String)
        .property("amount", DataType::Float64)
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    // region A: 10 + 30 = 40; region B: 100.
    tx.execute("CREATE (:Sale {region: 'A', amount: 10.0})")
        .await?;
    tx.execute("CREATE (:Sale {region: 'A', amount: 30.0})")
        .await?;
    tx.execute("CREATE (:Sale {region: 'B', amount: 100.0})")
        .await?;
    tx.commit().await?;

    let rows = session
        .query_with(
            "MATCH (s:Sale) \
             RETURN s.region AS region, \
             myplugin.partition_sum(s.amount) OVER (PARTITION BY s.region) AS total",
        )
        .fetch_all()
        .await?;

    let mut by_region: Vec<(String, f64)> = rows
        .iter()
        .filter_map(|r| Some((r.get::<String>("region").ok()?, r.get::<f64>("total").ok()?)))
        .collect();
    by_region.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.partial_cmp(&b.1).unwrap()));

    assert_eq!(
        by_region,
        vec![
            ("A".to_string(), 40.0),
            ("A".to_string(), 40.0),
            ("B".to_string(), 100.0),
        ],
        "each row carries its partition's amount sum"
    );
    Ok(())
}
