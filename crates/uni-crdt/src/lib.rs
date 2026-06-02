// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod gcounter;
pub mod gset;
pub mod lww_map;
pub mod lww_register;
pub mod orset;
pub mod registry_dispatch;
pub mod rga;
pub mod vc_register;
pub mod vector_clock;

pub use gcounter::GCounter;
pub use gset::GSet;
pub use lww_map::LWWMap;
pub use lww_register::LWWRegister;
pub use orset::ORSet;
pub use rga::Rga;
pub use vc_register::VCRegister;
pub use vector_clock::VectorClock;

#[derive(Error, Debug)]
pub enum CrdtError {
    #[error("Type mismatch: cannot merge {0} with {1}")]
    TypeMismatch(String, String),
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Trait for state-based CRDTs (CvRDTs)
pub trait CrdtMerge {
    /// Merge another instance into self.
    /// Must satisfy: commutativity, associativity, idempotency.
    fn merge(&mut self, other: &Self);
}

/// Dynamic CRDT wrapper for storage and query layers.
/// Using MessagePack for binary serialization in the storage layer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "t", content = "d")]
pub enum Crdt {
    #[serde(rename = "gc")]
    GCounter(GCounter),
    #[serde(rename = "gs")]
    GSet(GSet<String>),
    #[serde(rename = "os")]
    ORSet(ORSet<String>),
    #[serde(rename = "lr")]
    LWWRegister(LWWRegister<serde_json::Value>),
    #[serde(rename = "lm")]
    LWWMap(LWWMap<String, serde_json::Value>),
    #[serde(rename = "rg")]
    Rga(Rga<String>),
    #[serde(rename = "vc")]
    VectorClock(VectorClock),
    #[serde(rename = "vr")]
    VCRegister(VCRegister<serde_json::Value>),
}

/// Single source of truth for the `Crdt` variant table.
///
/// Expands a passed-in `$mac` over the rows
/// `<Variant> => <type_name_str> => <registry_kind_str>`. Every consumer
/// of the variant set — `try_merge`, `type_name`, and
/// `registry_dispatch::Crdt::kind` — drives off this list, so adding a
/// new CRDT means editing exactly one place.
#[macro_export]
macro_rules! for_each_crdt_variant {
    ($mac:ident) => {
        $mac! {
            GCounter    => "GCounter"    => "uni-crdt:g-counter",
            GSet        => "GSet"        => "uni-crdt:g-set",
            ORSet       => "ORSet"       => "uni-crdt:or-set",
            LWWRegister => "LWWRegister" => "uni-crdt:lww-register",
            LWWMap      => "LWWMap"      => "uni-crdt:lww-map",
            Rga         => "Rga"         => "uni-crdt:rga",
            VectorClock => "VectorClock" => "uni-crdt:vector-clock",
            VCRegister  => "VCRegister"  => "uni-crdt:vc-register",
        }
    };
}

macro_rules! try_merge_body {
    ($($variant:ident => $type_name:literal => $kind:literal,)*) => {
        impl Crdt {
            /// Try to merge another CRDT into this one.
            /// Returns an error if the types don't match.
            /// This is the safe, non-panicking version of merge.
            pub fn try_merge(&mut self, other: &Self) -> Result<(), CrdtError> {
                match (self, other) {
                    $(
                        (Crdt::$variant(a), Crdt::$variant(b)) => a.merge(b),
                    )*
                    (a, b) => {
                        return Err(CrdtError::TypeMismatch(
                            a.type_name().to_owned(),
                            b.type_name().to_owned(),
                        ));
                    }
                }
                Ok(())
            }

            /// Returns the type name of this CRDT variant for error messages.
            pub fn type_name(&self) -> &'static str {
                match self {
                    $(
                        Crdt::$variant(_) => $type_name,
                    )*
                }
            }
        }
    };
}
for_each_crdt_variant!(try_merge_body);

impl CrdtMerge for Crdt {
    /// Merge another CRDT into this one.
    /// Panics if the types don't match. For a non-panicking version, use `try_merge`.
    fn merge(&mut self, other: &Self) {
        if let Err(e) = self.try_merge(other) {
            panic!("CRDT merge failed: {e}");
        }
    }
}

impl Crdt {
    /// Serialize the CRDT to MessagePack bytes.
    pub fn to_msgpack(&self) -> Result<Vec<u8>, CrdtError> {
        rmp_serde::to_vec_named(self).map_err(|e| CrdtError::Serialization(e.to_string()))
    }

    /// Deserialize a CRDT from MessagePack bytes.
    pub fn from_msgpack(bytes: &[u8]) -> Result<Self, CrdtError> {
        rmp_serde::from_slice(bytes).map_err(|e| CrdtError::Serialization(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crdt_serialization() {
        let mut gc = GCounter::new();
        gc.increment("actor1", 42);
        let crdt = Crdt::GCounter(gc);

        let bytes = crdt.to_msgpack().unwrap();
        let decoded = Crdt::from_msgpack(&bytes).unwrap();

        assert_eq!(crdt, decoded);
    }

    #[test]
    fn try_merge_type_mismatch_surfaces_readable_names() {
        // Regression: previously formatted via `mem::discriminant`, producing
        // opaque `Discriminant(...)` strings. Both names should now be the
        // same human-readable identifiers `type_name()` returns.
        let mut a = Crdt::GCounter(GCounter::new());
        let b = Crdt::GSet(GSet::new());
        let err = a.try_merge(&b).expect_err("type mismatch must error");
        match err {
            CrdtError::TypeMismatch(left, right) => {
                assert_eq!(left, "GCounter");
                assert_eq!(right, "GSet");
            }
            other => panic!("expected TypeMismatch, got {other:?}"),
        }
    }
}
