//! M10 registry-dispatch bridge for the [`Crdt`] enum.
//!
//! This module is the *contract* that lets a host route a `Crdt` merge
//! through a registered [`uni_plugin::traits::crdt::CrdtKindProvider`]
//! instead of the closed-enum dispatch in [`Crdt::try_merge`]. Plumbing
//! it through hot mutation paths (e.g.,
//! `uni-store::runtime::property_manager`) is a separate refactor; this
//! module ships the entry point and an in-tree round-trip path so
//! call-site migration becomes a one-line swap.
//!
//! # Why the indirection?
//!
//! Without [`Crdt::merge_via_registry`], every CRDT merge in `uni-crdt`
//! goes through the native `try_merge` match. That bypasses the plugin
//! registry, so users cannot supply alternative CRDT implementations
//! (different schema versions, different conflict-resolution rules).
//! Going through the registry means hot-reload of a CRDT provider can
//! actually swap merge semantics for already-stored values — provided
//! the new provider's
//! [`CrdtKindProvider::schema_compat_check`](uni_plugin::traits::crdt::CrdtKindProvider::schema_compat_check)
//! accepts the old provider's persisted shape.
//!
//! # Round-trip envelope
//!
//! Providers exchange state via opaque `Vec<u8>` bytes; the encoding is
//! per-provider. For the registry path to interoperate with the native
//! `Crdt` enum, both sides have to agree on the wire shape. The
//! convention this module establishes is the same MessagePack envelope
//! the native enum uses: [`Crdt::to_msgpack`] →
//! `CrdtKindProvider::from_persisted` → mutate → `CrdtState::persist`
//! → [`Crdt::from_msgpack`]. Providers that opt in to this envelope
//! (e.g., the `Native*Provider` test fixtures in
//! `tests/registry_dispatch.rs`) round-trip cleanly. Providers using
//! their own envelope (e.g., the builtin `LwwRegisterProvider` in
//! `uni-plugin-builtin/src/crdts.rs`, which uses JSON over a
//! provider-local state struct) cannot share state with the native
//! enum and stay accessible only through the direct registry API.

use uni_plugin::PluginRegistry;
use uni_plugin::traits::crdt::{CrdtKind, CrdtOp};

use crate::{Crdt, CrdtError};

impl Crdt {
    /// The canonical [`CrdtKind`] this variant maps to.
    ///
    /// Used by [`Self::merge_via_registry`] to look up the provider.
    /// The strings are scoped under `uni-crdt:*` to keep them distinct
    /// from the `uni-plugin-builtin` provider kinds (e.g.,
    /// `lww-register`) which wrap independent state types.
    #[must_use]
    pub fn kind(&self) -> CrdtKind {
        macro_rules! kind_arms {
            ($($variant:ident => $type_name:literal => $kind:literal,)*) => {
                match self {
                    $(
                        Crdt::$variant(_) => $kind,
                    )*
                }
            };
        }
        CrdtKind::new(crate::for_each_crdt_variant!(kind_arms))
    }

    /// Merge `other` into `self` through the plugin registry.
    ///
    /// Looks up a [`uni_plugin::traits::crdt::CrdtKindProvider`] keyed
    /// by `self.kind()`. If absent, falls back to
    /// [`Self::try_merge`] so hosts that have not registered any
    /// provider get the native behavior. If present, both operands are
    /// round-tripped through the MessagePack envelope and the
    /// provider's `merge` is invoked.
    ///
    /// # Errors
    ///
    /// - [`CrdtError::TypeMismatch`] if the variants differ.
    /// - [`CrdtError::Serialization`] on round-trip failure.
    pub fn merge_via_registry(
        &mut self,
        other: &Self,
        registry: &PluginRegistry,
    ) -> Result<(), CrdtError> {
        if std::mem::discriminant(self) != std::mem::discriminant(other) {
            return Err(CrdtError::TypeMismatch(
                self.type_name().to_owned(),
                other.type_name().to_owned(),
            ));
        }

        let kind = self.kind();
        let Some(provider) = registry.crdt_kind(&kind) else {
            // No provider registered → fall back to the native dispatch.
            return self.try_merge(other);
        };

        let self_bytes = self.to_msgpack()?;
        let other_bytes = other.to_msgpack()?;
        let mut lhs = provider
            .from_persisted(&self_bytes)
            .map_err(|e| CrdtError::Serialization(format!("{kind:?} from_persisted (lhs): {e}")))?;
        let rhs = provider
            .from_persisted(&other_bytes)
            .map_err(|e| CrdtError::Serialization(format!("{kind:?} from_persisted (rhs): {e}")))?;
        lhs.merge(rhs.as_ref())
            .map_err(|e| CrdtError::Serialization(format!("{kind:?} merge: {e}")))?;
        let merged = lhs
            .persist()
            .map_err(|e| CrdtError::Serialization(format!("{kind:?} persist: {e}")))?;
        *self = Crdt::from_msgpack(&merged)?;
        Ok(())
    }
}

/// Convenience: wrap any byte slice as a [`CrdtOp`] payload.
///
/// Useful for callers that have already serialized an op via msgpack
/// and want to feed it to a provider's
/// [`CrdtState::apply`](uni_plugin::traits::crdt::CrdtState::apply) for
/// registry-dispatched mutation.
#[must_use]
pub fn op_from_bytes(bytes: Vec<u8>) -> CrdtOp {
    CrdtOp { bytes }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GCounter;

    #[test]
    fn kind_maps_each_variant_to_a_distinct_string() {
        let mut seen = std::collections::HashSet::new();
        let crdts = [
            Crdt::GCounter(GCounter::new()),
            Crdt::GSet(crate::GSet::new()),
            Crdt::ORSet(crate::ORSet::new()),
            Crdt::LWWRegister(crate::LWWRegister::new(serde_json::Value::Null, 0)),
        ];
        for c in &crdts {
            assert!(
                seen.insert(c.kind().0.to_string()),
                "kind not unique: {c:?}"
            );
        }
    }

    #[test]
    fn merge_via_registry_falls_back_to_native_when_no_provider() {
        let registry = PluginRegistry::new();
        let mut a = GCounter::new();
        a.increment("r1", 5);
        let mut b = GCounter::new();
        b.increment("r2", 3);
        let mut x = Crdt::GCounter(a);
        let y = Crdt::GCounter(b);
        x.merge_via_registry(&y, &registry).unwrap();
        match x {
            Crdt::GCounter(c) => assert_eq!(c.value(), 8),
            other => panic!("expected GCounter, got {other:?}"),
        }
    }
}
