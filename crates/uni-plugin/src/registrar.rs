//! The [`PluginRegistrar`] a plugin's `register()` method calls.
//!
//! Every registration method is capability-gated against the effective
//! capability set computed at load time (manifest-declared ∩ host-granted).
//! Registrations claiming a `QName` that is already taken fail with
//! [`crate::PluginError::DuplicateRegistration`].

use std::sync::Arc;

use smol_str::SmolStr;

use crate::capability::{Capability, CapabilitySet};
use crate::errors::PluginError;
use crate::plugin::PluginId;
use crate::qname::QName;
use crate::registry::PluginRegistry;
use crate::surfaces::{
    AggregateSurface, AlgorithmSurface, AppendReg, AuthSurface, AuthzSurface, BackgroundJobSurface,
    CatalogSurface, CdcSurface, CollationSurface, CrdtSurface,
    DynPendingRegistration, HookSurface, IndexKindSurface, KeyedUniqueReg, LabelStorageSurface,
    LocyAggregateSurface, LocyPredicateSurface, LogicalTypeSurface, NamedUniqueReg,
    OptimizerRuleSurface, ProcedureSurface, ReplacementScanSurface,
    ScalarSurface, TriggerSurface, VersionedReg, WindowSurface,
};
use crate::traits::aggregate::{AggSignature, AggregatePluginFn};
use crate::traits::algorithm::AlgorithmProvider;
use crate::traits::background::BackgroundJobProvider;
use crate::traits::catalog::{CatalogProvider, ReplacementScanProvider};
use crate::traits::cdc::CdcOutputProvider;
use crate::traits::collation::CollationProvider;
use crate::traits::connector::{AuthProvider, AuthzPolicy};
use crate::traits::crdt::{CrdtKind, CrdtKindProvider};
use crate::traits::hook::SessionHook;
use crate::traits::index::{IndexKind, IndexKindProvider};
use crate::traits::locy::{LocyAggregate, LocyPredicate, PredSignature};
use crate::traits::operator::OptimizerRuleProvider;
use crate::traits::procedure::{ProcedurePlugin, ProcedureSignature};
use crate::traits::scalar::{FnSignature, ScalarPluginFn};
use crate::traits::trigger::TriggerPlugin;
use crate::traits::types::LogicalTypeProvider;
use crate::traits::window::{WindowPluginFn, WindowSignature};

/// The builder passed to [`crate::Plugin::register`].
///
/// Each registration method takes a [`QName`] and a trait-object
/// implementation. The registrar verifies the corresponding capability is
/// present in the effective set, rejects duplicate qnames, and forwards the
/// registration to the [`PluginRegistry`].
///
/// The registrar is short-lived: one is created per `register()` call;
/// changes flush to the [`PluginRegistry`] when `register()` returns
/// successfully. A failed `register()` rolls back any partial state.
pub struct PluginRegistrar<'a> {
    plugin_id: PluginId,
    effective_caps: &'a CapabilitySet,
    registry: &'a PluginRegistry,
    pending: Vec<Box<dyn DynPendingRegistration>>,
    /// QNames of aggregate functions staged via [`Self::aggregate_fn`]. The
    /// pending registrations are type-erased, so we record aggregate qnames
    /// separately to let the host loader publish each one's Cypher
    /// routing hint (`uni_cypher::register_plugin_aggregate`) after a
    /// successful commit — without this, the Cypher planner classifies
    /// `RETURN myAgg(x)` as a scalar UDF and fails to resolve it.
    aggregate_qnames: Vec<QName>,
}

impl<'a> std::fmt::Debug for PluginRegistrar<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginRegistrar")
            .field("plugin_id", &self.plugin_id)
            .field("pending", &self.pending.len())
            .finish_non_exhaustive()
    }
}

impl<'a> PluginRegistrar<'a> {
    /// Construct a registrar for the given plugin.
    ///
    /// Created by the host loader; plugin authors never construct these
    /// directly.
    #[must_use]
    pub fn new(
        plugin_id: PluginId,
        effective_caps: &'a CapabilitySet,
        registry: &'a PluginRegistry,
    ) -> Self {
        Self {
            plugin_id,
            effective_caps,
            registry,
            pending: Vec::new(),
            aggregate_qnames: Vec::new(),
        }
    }

    /// QNames of aggregate functions staged on this registrar (in registration
    /// order). The host loader uses these, after a successful
    /// [`Self::commit_to_registry`], to publish each aggregate's Cypher
    /// routing hint so `RETURN myAgg(x)` is planned as an aggregate rather
    /// than a scalar UDF. Empty until [`Self::aggregate_fn`] is called.
    #[must_use]
    pub fn staged_aggregate_qnames(&self) -> &[QName] {
        &self.aggregate_qnames
    }

    /// Returns the plugin id being registered.
    #[must_use]
    pub fn plugin_id(&self) -> &PluginId {
        &self.plugin_id
    }

    /// Override the plugin id mid-registration.
    ///
    /// Used by external loaders (`uni-plugin-extism`, `uni-plugin-wasm`)
    /// during their two-pass dance: pass 1 reads the plugin's
    /// `manifest` export to learn the canonical id, then sets it here
    /// so that `validate_qname` accepts qnames in the plugin's
    /// declared namespace.
    pub fn set_plugin_id(&mut self, plugin_id: PluginId) {
        self.plugin_id = plugin_id;
    }

    fn require(&self, cap: &Capability) -> Result<(), PluginError> {
        if self.effective_caps.contains_variant(cap) {
            Ok(())
        } else {
            Err(PluginError::CapabilityRequired(cap.clone()))
        }
    }

    fn validate_qname(&self, qname: &QName) -> Result<(), PluginError> {
        if !qname.is_builtin() && qname.namespace() != self.plugin_id.as_str() {
            return Err(PluginError::internal(format!(
                "plugin `{}` cannot register qname `{}` outside its namespace",
                self.plugin_id, qname
            )));
        }
        Ok(())
    }

    /// Register a Cypher scalar function.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if [`Capability::ScalarFn`]
    /// is absent, or [`PluginError::DuplicateRegistration`] (raised at
    /// commit time) on qname collision.
    pub fn scalar_fn(
        &mut self,
        qname: QName,
        sig: FnSignature,
        f: Arc<dyn ScalarPluginFn>,
    ) -> Result<&mut Self, PluginError> {
        self.require(&Capability::ScalarFn)?;
        self.validate_qname(&qname)?;
        self.pending.push(Box::new(NamedUniqueReg::<ScalarSurface> {
            q: qname,
            sig,
            provider: f,
        }));
        Ok(self)
    }

    /// Register a Cypher aggregate function.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if [`Capability::AggregateFn`] is absent.
    pub fn aggregate_fn(
        &mut self,
        qname: QName,
        sig: AggSignature,
        f: Arc<dyn AggregatePluginFn>,
    ) -> Result<&mut Self, PluginError> {
        self.require(&Capability::AggregateFn)?;
        self.validate_qname(&qname)?;
        self.aggregate_qnames.push(qname.clone());
        self.pending
            .push(Box::new(NamedUniqueReg::<AggregateSurface> {
                q: qname,
                sig,
                provider: f,
            }));
        Ok(self)
    }

    /// Register a Cypher window function.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if [`Capability::WindowFn`] is absent.
    pub fn window_fn(
        &mut self,
        qname: QName,
        sig: WindowSignature,
        f: Arc<dyn WindowPluginFn>,
    ) -> Result<&mut Self, PluginError> {
        self.require(&Capability::WindowFn)?;
        self.validate_qname(&qname)?;
        self.pending.push(Box::new(NamedUniqueReg::<WindowSurface> {
            q: qname,
            sig,
            provider: f,
        }));
        Ok(self)
    }

    /// Register a Cypher procedure.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if the procedure's mode's
    /// required capability is absent.
    pub fn procedure(
        &mut self,
        qname: QName,
        sig: ProcedureSignature,
        p: Arc<dyn ProcedurePlugin>,
    ) -> Result<&mut Self, PluginError> {
        use crate::traits::procedure::ProcedureMode;
        self.require(&Capability::Procedure)?;
        match sig.mode {
            ProcedureMode::Write => self.require(&Capability::ProcedureWrites)?,
            ProcedureMode::Schema => self.require(&Capability::ProcedureSchema)?,
            ProcedureMode::Dbms => self.require(&Capability::ProcedureDbms)?,
            ProcedureMode::Read => {}
        }
        self.validate_qname(&qname)?;
        self.pending
            .push(Box::new(VersionedReg::<ProcedureSurface> {
                q: qname,
                sig,
                provider: p,
            }));
        Ok(self)
    }

    /// Register a Locy aggregate.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if [`Capability::LocyAggregate`] is absent.
    pub fn locy_aggregate(
        &mut self,
        qname: QName,
        a: Arc<dyn LocyAggregate>,
    ) -> Result<&mut Self, PluginError> {
        self.require(&Capability::LocyAggregate)?;
        self.validate_qname(&qname)?;
        self.pending
            .push(Box::new(NamedUniqueReg::<LocyAggregateSurface> {
                q: qname,
                sig: (),
                provider: a,
            }));
        Ok(self)
    }

    /// Register a Locy predicate.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if [`Capability::LocyPredicate`] is absent.
    pub fn locy_predicate(
        &mut self,
        qname: QName,
        sig: PredSignature,
        p: Arc<dyn LocyPredicate>,
    ) -> Result<&mut Self, PluginError> {
        self.require(&Capability::LocyPredicate)?;
        self.validate_qname(&qname)?;
        self.pending
            .push(Box::new(NamedUniqueReg::<LocyPredicateSurface> {
                q: qname,
                sig,
                provider: p,
            }));
        Ok(self)
    }

    /// Register an optimizer rule.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if [`Capability::Operator`] is absent.
    pub fn optimizer_rule(
        &mut self,
        r: Arc<dyn OptimizerRuleProvider>,
    ) -> Result<&mut Self, PluginError> {
        self.require(&Capability::Operator)?;
        self.pending
            .push(Box::new(AppendReg::<OptimizerRuleSurface> { provider: r }));
        Ok(self)
    }

    /// Register an index kind.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if [`Capability::Index`] is absent.
    pub fn index_kind(
        &mut self,
        kind: IndexKind,
        p: Arc<dyn IndexKindProvider>,
    ) -> Result<&mut Self, PluginError> {
        self.require(&Capability::Index)?;
        self.pending
            .push(Box::new(KeyedUniqueReg::<IndexKindSurface> {
                key_override: Some(kind),
                provider: p,
            }));
        Ok(self)
    }

    /// Register a per-label plugin storage (M5h.2).
    ///
    /// Native-schema label scans for `label` will be routed through
    /// `storage` instead of the host's native backend.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if
    /// [`Capability::Storage`] is absent.
    pub fn label_storage(
        &mut self,
        label: impl Into<SmolStr>,
        storage: Arc<dyn crate::traits::storage::Storage>,
    ) -> Result<&mut Self, PluginError> {
        self.require(&Capability::Storage)?;
        self.pending
            .push(Box::new(KeyedUniqueReg::<LabelStorageSurface> {
                key_override: Some(label.into()),
                provider: storage,
            }));
        Ok(self)
    }

    /// Register a graph algorithm.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if [`Capability::Algorithm`] is absent.
    pub fn algorithm(
        &mut self,
        qname: QName,
        p: Arc<dyn AlgorithmProvider>,
    ) -> Result<&mut Self, PluginError> {
        self.require(&Capability::Algorithm)?;
        self.validate_qname(&qname)?;
        self.pending
            .push(Box::new(NamedUniqueReg::<AlgorithmSurface> {
                q: qname,
                sig: (),
                provider: p,
            }));
        Ok(self)
    }

    /// Register a CRDT kind.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if [`Capability::Crdt`] is absent.
    pub fn crdt_kind(
        &mut self,
        kind: CrdtKind,
        p: Arc<dyn CrdtKindProvider>,
    ) -> Result<&mut Self, PluginError> {
        self.require(&Capability::Crdt)?;
        self.pending.push(Box::new(KeyedUniqueReg::<CrdtSurface> {
            key_override: Some(kind),
            provider: p,
        }));
        Ok(self)
    }

    /// Register a session-lifecycle hook.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if [`Capability::Hook`] is absent.
    pub fn hook(&mut self, h: Arc<dyn SessionHook>) -> Result<&mut Self, PluginError> {
        self.require(&Capability::Hook)?;
        self.pending
            .push(Box::new(AppendReg::<HookSurface> { provider: h }));
        Ok(self)
    }

    /// Register a logical type.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if [`Capability::Type`] is absent.
    pub fn logical_type(
        &mut self,
        t: Arc<dyn LogicalTypeProvider>,
    ) -> Result<&mut Self, PluginError> {
        self.require(&Capability::Type)?;
        self.pending
            .push(Box::new(KeyedUniqueReg::<LogicalTypeSurface> {
                key_override: None,
                provider: t,
            }));
        Ok(self)
    }

    /// Register an authentication provider.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if [`Capability::Auth`] is absent.
    pub fn auth_provider(&mut self, p: Arc<dyn AuthProvider>) -> Result<&mut Self, PluginError> {
        self.require(&Capability::Auth)?;
        self.pending
            .push(Box::new(AppendReg::<AuthSurface> { provider: p }));
        Ok(self)
    }

    /// Register an authorization policy.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if [`Capability::Authz`] is absent.
    pub fn authz_policy(&mut self, p: Arc<dyn AuthzPolicy>) -> Result<&mut Self, PluginError> {
        self.require(&Capability::Authz)?;
        self.pending
            .push(Box::new(AppendReg::<AuthzSurface> { provider: p }));
        Ok(self)
    }

    /// Register a fine-grained trigger.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if [`Capability::Trigger`] is absent.
    pub fn trigger(&mut self, t: Arc<dyn TriggerPlugin>) -> Result<&mut Self, PluginError> {
        self.require(&Capability::Trigger)?;
        self.pending
            .push(Box::new(AppendReg::<TriggerSurface> { provider: t }));
        Ok(self)
    }

    /// Register a collation.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if [`Capability::Collation`] is absent.
    pub fn collation(&mut self, c: Arc<dyn CollationProvider>) -> Result<&mut Self, PluginError> {
        self.require(&Capability::Collation)?;
        self.pending
            .push(Box::new(KeyedUniqueReg::<CollationSurface> {
                key_override: None,
                provider: c,
            }));
        Ok(self)
    }

    /// Register a CDC output sink.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if [`Capability::Cdc`] is absent.
    pub fn cdc_output(&mut self, c: Arc<dyn CdcOutputProvider>) -> Result<&mut Self, PluginError> {
        self.require(&Capability::Cdc)?;
        self.pending.push(Box::new(KeyedUniqueReg::<CdcSurface> {
            key_override: None,
            provider: c,
        }));
        Ok(self)
    }

    /// Register a catalog provider.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if [`Capability::Catalog`] is absent.
    pub fn catalog(&mut self, c: Arc<dyn CatalogProvider>) -> Result<&mut Self, PluginError> {
        self.require(&Capability::Catalog)?;
        self.pending
            .push(Box::new(KeyedUniqueReg::<CatalogSurface> {
                key_override: None,
                provider: c,
            }));
        Ok(self)
    }

    /// Register a replacement-scan provider.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if [`Capability::Catalog`] is absent.
    pub fn replacement_scan(
        &mut self,
        r: Arc<dyn ReplacementScanProvider>,
    ) -> Result<&mut Self, PluginError> {
        self.require(&Capability::Catalog)?;
        self.pending
            .push(Box::new(AppendReg::<ReplacementScanSurface> {
                provider: r,
            }));
        Ok(self)
    }

    /// Register a background-job provider.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::CapabilityRequired`] if no `BackgroundJob`
    /// capability variant is present in the effective set.
    pub fn background_job(
        &mut self,
        j: Arc<dyn BackgroundJobProvider>,
    ) -> Result<&mut Self, PluginError> {
        self.require(&Capability::BackgroundJob { max_concurrent: 0 })?;
        self.pending
            .push(Box::new(AppendReg::<BackgroundJobSurface> { provider: j }));
        Ok(self)
    }

    /// Commit batched registrations to the registry.
    ///
    /// Called by the host loader after the plugin's `register()` returns
    /// successfully; failures during `register()` are rolled back by simply
    /// dropping the registrar without committing.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::DuplicateRegistration`] if any pending qname
    /// is already taken in the registry.
    pub fn commit_to_registry(self) -> Result<(), PluginError> {
        self.registry.apply_pending(&self.plugin_id, self.pending)
    }

    /// Returns the number of pending registrations.
    ///
    /// Exposed for diagnostics and integration tests that want to verify
    /// a plugin's `register()` queued the expected number of items before
    /// the registrar commits.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }
}
