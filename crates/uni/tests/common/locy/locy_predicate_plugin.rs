// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end test for the (previously dead) `LocyPredicate` plugin surface.
//!
//! Registers a third-party filter predicate and uses it inside a Locy rule
//! `WHERE`, proving it is dispatched at runtime rather than dying as an
//! "unknown function". The single `eval_function` interception serves every
//! in-memory Locy eval path (SLG, DERIVE, delta, QUERY).

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use arrow_array::{Array, BooleanArray, Float64Array};
use datafusion::logical_expr::{ColumnarValue, Volatility};
use uni_db::{DataType, Uni};
use uni_plugin::traits::locy::{BatchHint, LocyPredicate, PredSignature};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{
    AbiRange, Capability, CapabilitySet, Determinism, FnError, Plugin, PluginError, PluginManifest,
    PluginRegistrar, ProvidedSurfaces, QName, Scope, SideEffects,
};

/// `myplugin.is_positive(x)` — true when `x > 0`.
#[derive(Debug)]
struct IsPositive {
    sig: PredSignature,
}

impl IsPositive {
    fn new() -> Self {
        Self {
            sig: PredSignature {
                args: vec![ArgType::Primitive(arrow_schema::DataType::Float64)],
                volatility: Volatility::Immutable,
                supports_fuzzy: false,
                batch_hint: BatchHint::Small,
            },
        }
    }
}

impl LocyPredicate for IsPositive {
    fn signature(&self) -> &PredSignature {
        &self.sig
    }
    fn evaluate(&self, args: &[ColumnarValue], rows: usize) -> Result<BooleanArray, FnError> {
        let ColumnarValue::Array(a) = &args[0] else {
            return Err(FnError::new(0, "expected array arg"));
        };
        let f = a
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| FnError::new(0, "is_positive expects a float"))?;
        Ok((0..rows).map(|i| Some(f.value(i) > 0.0)).collect())
    }
}

struct PredicatePlugin {
    manifest: OnceLock<PluginManifest>,
}

impl Plugin for PredicatePlugin {
    fn manifest(&self) -> &PluginManifest {
        self.manifest.get_or_init(|| PluginManifest {
            id: uni_plugin::PluginId::new("myplugin"),
            version: "0.1.0".parse().expect("static version"),
            abi: AbiRange::parse("^1").expect("static abi"),
            depends_on: vec![],
            capabilities: CapabilitySet::from_iter_of([Capability::LocyPredicate]),
            determinism: Determinism::Pure,
            side_effects: SideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: ProvidedSurfaces::default(),
            docs: "third-party Locy filter predicate (test)".to_owned(),
            metadata: BTreeMap::new(),
        })
    }

    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        r.locy_predicate(
            QName::new("myplugin", "is_positive"),
            IsPositive::new().sig,
            Arc::new(IsPositive::new()),
        )?;
        Ok(())
    }
}

/// A registered Locy filter predicate actually filters a rule's rows.
#[tokio::test]
async fn locy_filter_predicate_dispatches_in_rule_where() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.add_plugin(PredicatePlugin {
        manifest: OnceLock::new(),
    })?;

    db.schema()
        .label("Num")
        .property("val", DataType::Float64)
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    for v in [-3.0_f64, 2.0, -1.0, 4.0] {
        tx.execute(&format!("CREATE (:Num {{val: {v}}})")).await?;
    }
    tx.commit().await?;

    // The predicate name is namespaced, so it must be called dotted. Before this
    // wiring it died as "unknown function"; now it filters to the positives.
    let program = "CREATE RULE positives AS \
         MATCH (n:Num) WHERE myplugin.is_positive(n.val) YIELD KEY n.val AS v\n\
         QUERY positives RETURN v";
    let result = session.locy(program).await?;
    let empty = vec![];
    let rows = result.rows().unwrap_or(&empty);

    let mut vals: Vec<f64> = rows
        .iter()
        .filter_map(|r| r.get("v").and_then(uni_db::Value::as_f64))
        .collect();
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(
        vals,
        vec![2.0, 4.0],
        "only positive vals survive the predicate"
    );
    Ok(())
}
