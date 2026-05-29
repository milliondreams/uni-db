//! Integration test exercising the full Plugin → Registrar → Registry path
//! end-to-end with a minimal scalar-fn plugin and a Locy aggregate.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::OnceLock;

use arrow_schema::DataType;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use datafusion::scalar::ScalarValue;
use semver::Version;
use uni_plugin::traits::aggregate::{AggSignature, AggregatePluginFn, PluginAccumulator};
use uni_plugin::traits::locy::{LocyAggState, LocyAggregate, Semilattice};
use uni_plugin::traits::scalar::{ArgType, FnSignature, NullHandling, ScalarPluginFn};
use uni_plugin::{
    Capability, CapabilitySet, Determinism, FnError, Plugin, PluginError, PluginId, PluginManifest,
    PluginRegistrar, PluginRegistry, ProvidedSurfaces, QName, Scope, SideEffects,
};

// --------------------------------------------------------------------------
// Example plugin: registers a scalar fn, a Cypher aggregate, a Locy aggregate.
// --------------------------------------------------------------------------

struct DemoPlugin {
    manifest: OnceLock<PluginManifest>,
}

impl DemoPlugin {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            manifest: OnceLock::new(),
        })
    }

    fn manifest_value() -> PluginManifest {
        PluginManifest {
            id: PluginId::new("test.demo"),
            version: Version::new(0, 1, 0),
            abi: uni_plugin::AbiRange::parse("^1").unwrap(),
            depends_on: vec![],
            capabilities: CapabilitySet::from_iter_of([
                Capability::ScalarFn,
                Capability::AggregateFn,
                Capability::LocyAggregate,
            ]),
            determinism: Determinism::Pure,
            side_effects: SideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: ProvidedSurfaces::default(),
            docs: "Demo plugin used by uni-plugin's end-to-end integration test.".to_owned(),
            metadata: BTreeMap::new(),
        }
    }
}

impl Plugin for DemoPlugin {
    fn manifest(&self) -> &PluginManifest {
        self.manifest.get_or_init(Self::manifest_value)
    }

    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        r.scalar_fn(
            QName::new("test.demo", "noop"),
            FnSignature {
                args: vec![ArgType::Primitive(DataType::Float64)],
                returns: ArgType::Primitive(DataType::Float64),
                volatility: Volatility::Immutable,
                null_handling: NullHandling::PropagateNulls,
            },
            Arc::new(NoopScalar),
        )?;
        r.locy_aggregate(QName::new("test.demo", "MAX"), Arc::new(MaxLocyAgg))?;
        r.aggregate_fn(
            QName::new("test.demo", "sum"),
            AggSignature::new(
                vec![ArgType::Primitive(DataType::Float64)],
                ArgType::Primitive(DataType::Float64),
                vec![],
                Volatility::Immutable,
            ),
            Arc::new(SumAgg),
        )?;
        Ok(())
    }
}

// --- Scalar fn stub -------------------------------------------------------

struct NoopScalar;

impl ScalarPluginFn for NoopScalar {
    fn signature(&self) -> &FnSignature {
        // For the test we only exercise registration; signature() is not invoked
        // by the registry-level paths under test.
        static SIG: OnceLock<FnSignature> = OnceLock::new();
        SIG.get_or_init(|| {
            FnSignature::new(
                vec![ArgType::Primitive(DataType::Float64)],
                ArgType::Primitive(DataType::Float64),
                Volatility::Immutable,
            )
        })
    }

    fn invoke(&self, args: &[ColumnarValue], _rows: usize) -> Result<ColumnarValue, FnError> {
        Ok(args[0].clone())
    }
}

// --- Locy aggregate stub --------------------------------------------------

#[derive(Debug)]
struct MaxLocyAgg;

impl LocyAggregate for MaxLocyAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice::BOUNDED_MIN_MAX
    }

    fn output_type(&self) -> DataType {
        DataType::Float64
    }

    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(MaxLocyState { current: f64::MIN })
    }
}

struct MaxLocyState {
    current: f64,
}

impl LocyAggState for MaxLocyState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn ingest(
        &mut self,
        _batch: &datafusion::arrow::record_batch::RecordBatch,
        _value_col: usize,
    ) -> Result<(), FnError> {
        Ok(())
    }

    fn merge(&mut self, _other: &dyn LocyAggState) -> Result<(), FnError> {
        Ok(())
    }

    fn finalize(&self) -> Result<ScalarValue, FnError> {
        Ok(ScalarValue::Float64(Some(self.current)))
    }
}

// --- Cypher aggregate stub -----------------------------------------------

struct SumAgg;

impl AggregatePluginFn for SumAgg {
    fn signature(&self) -> &AggSignature {
        static SIG: OnceLock<AggSignature> = OnceLock::new();
        SIG.get_or_init(|| {
            AggSignature::new(
                vec![ArgType::Primitive(DataType::Float64)],
                ArgType::Primitive(DataType::Float64),
                vec![],
                Volatility::Immutable,
            )
        })
    }

    fn create_accumulator(&self) -> Box<dyn PluginAccumulator> {
        Box::new(SumAccumulator { acc: 0.0 })
    }
}

struct SumAccumulator {
    acc: f64,
}

impl PluginAccumulator for SumAccumulator {
    fn update_batch(&mut self, _values: &[arrow_array::ArrayRef]) -> Result<(), FnError> {
        Ok(())
    }

    fn merge_batch(&mut self, _states: &[arrow_array::ArrayRef]) -> Result<(), FnError> {
        Ok(())
    }

    fn state(&self) -> Result<Vec<ScalarValue>, FnError> {
        Ok(vec![ScalarValue::Float64(Some(self.acc))])
    }

    fn evaluate(&self) -> Result<ScalarValue, FnError> {
        Ok(ScalarValue::Float64(Some(self.acc)))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

// --- The integration test ------------------------------------------------

#[test]
fn end_to_end_plugin_registration_round_trip() {
    let registry = PluginRegistry::new();
    let plugin = DemoPlugin::new();
    let manifest = plugin.manifest();

    // Simulate the loader: effective caps = manifest caps (test harness
    // grants everything the plugin declares).
    let effective = manifest.capabilities.clone();

    let mut registrar = PluginRegistrar::new(manifest.id.clone(), &effective, &registry);
    plugin.register(&mut registrar).expect("registration");
    registrar.commit_to_registry().expect("commit");

    // Verify each registration is observable through the registry.
    assert!(
        registry
            .scalar_fn(&QName::new("test.demo", "noop"))
            .is_some()
    );
    assert!(
        registry
            .locy_aggregate(&QName::new("test.demo", "MAX"))
            .is_some()
    );
    assert!(
        registry
            .aggregate(&QName::new("test.demo", "sum"))
            .is_some()
    );

    // A bogus qname lookup misses.
    assert!(
        registry
            .scalar_fn(&QName::new("test.demo", "missing"))
            .is_none()
    );

    // Verify the registered scalar's signature is preserved.
    let entry = registry
        .scalar_fn(&QName::new("test.demo", "noop"))
        .unwrap();
    assert_eq!(entry.plugin.as_str(), "test.demo");
    assert_eq!(entry.signature.args.len(), 1);
}

#[test]
fn registration_without_capability_is_rejected() {
    let registry = PluginRegistry::new();
    let plugin_id = PluginId::new("test.lacking_caps");
    let empty = CapabilitySet::new();
    let mut registrar = PluginRegistrar::new(plugin_id.clone(), &empty, &registry);

    let result = registrar.scalar_fn(
        QName::new("test.lacking_caps", "anything"),
        FnSignature::new(
            vec![ArgType::Primitive(DataType::Float64)],
            ArgType::Primitive(DataType::Float64),
            Volatility::Immutable,
        ),
        Arc::new(NoopScalar),
    );

    assert!(matches!(
        result,
        Err(PluginError::CapabilityRequired(Capability::ScalarFn))
    ));
}

#[test]
fn cross_namespace_registration_is_rejected() {
    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut registrar = PluginRegistrar::new(PluginId::new("test.alice"), &caps, &registry);

    // Plugin "test.alice" cannot register a qname in "test.bob" namespace.
    let result = registrar.scalar_fn(
        QName::new("test.bob", "stolen"),
        FnSignature::new(
            vec![ArgType::Primitive(DataType::Float64)],
            ArgType::Primitive(DataType::Float64),
            Volatility::Immutable,
        ),
        Arc::new(NoopScalar),
    );

    assert!(matches!(result, Err(PluginError::Internal(_))));
}

#[test]
fn duplicate_registration_in_same_batch_is_rejected() {
    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);

    // First plugin registers `foo.bar`.
    {
        let mut registrar = PluginRegistrar::new(PluginId::new("foo"), &caps, &registry);
        registrar
            .scalar_fn(
                QName::new("foo", "bar"),
                FnSignature::new(
                    vec![ArgType::Primitive(DataType::Float64)],
                    ArgType::Primitive(DataType::Float64),
                    Volatility::Immutable,
                ),
                Arc::new(NoopScalar),
            )
            .unwrap();
        registrar.commit_to_registry().unwrap();
    }

    // A second plugin registering the *same* qname (would require namespace
    // clobbering, which validate_qname catches first). Here we re-attempt as
    // the same plugin id; the registry's duplicate-check kicks in.
    {
        let mut registrar = PluginRegistrar::new(PluginId::new("foo"), &caps, &registry);
        registrar
            .scalar_fn(
                QName::new("foo", "bar"),
                FnSignature::new(
                    vec![ArgType::Primitive(DataType::Float64)],
                    ArgType::Primitive(DataType::Float64),
                    Volatility::Immutable,
                ),
                Arc::new(NoopScalar),
            )
            .unwrap();
        let err = registrar.commit_to_registry().unwrap_err();
        assert!(matches!(err, PluginError::DuplicateRegistration(_)));
    }
}

#[test]
fn remove_plugin_clears_registrations() {
    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn, Capability::LocyAggregate]);

    let mut registrar = PluginRegistrar::new(PluginId::new("test.demo"), &caps, &registry);
    registrar
        .scalar_fn(
            QName::new("test.demo", "noop"),
            FnSignature::new(
                vec![ArgType::Primitive(DataType::Float64)],
                ArgType::Primitive(DataType::Float64),
                Volatility::Immutable,
            ),
            Arc::new(NoopScalar),
        )
        .unwrap();
    registrar
        .locy_aggregate(QName::new("test.demo", "MAX"), Arc::new(MaxLocyAgg))
        .unwrap();
    registrar.commit_to_registry().unwrap();

    assert!(
        registry
            .scalar_fn(&QName::new("test.demo", "noop"))
            .is_some()
    );

    registry.remove_plugin(&PluginId::new("test.demo"));

    assert!(
        registry
            .scalar_fn(&QName::new("test.demo", "noop"))
            .is_none()
    );
    assert!(
        registry
            .locy_aggregate(&QName::new("test.demo", "MAX"))
            .is_none()
    );
}
