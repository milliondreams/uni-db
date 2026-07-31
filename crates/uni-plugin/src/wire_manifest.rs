// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The wire-level argument type a plugin manifest declares.
//!
//! Guest plugins describe their signatures as JSON. Every loader that reads a
//! manifest — the Extism loader and the Component-Model (`uni-plugin-wasm`)
//! loader — has to agree on that schema, or the same manifest loads under one
//! runtime and is rejected by the other.
//!
//! They did not agree. Each loader carried its own `WireArgType` and the two
//! had drifted: Extism's had `Primitive`, `CypherValue`, `Vector` and
//! `Variadic`; the Component-Model loader's had only the first two. Because
//! both use `#[serde(tag = "kind", deny_unknown_fields)]`, a manifest
//! declaring `kind: "vector"` or `kind: "variadic"` deserialized fine over
//! Extism and failed to parse under the Component-Model loader.
//!
//! The type lives here so there is one schema. It carries the union of what
//! the two loaders needed from their derives: `Serialize` (Extism round-trips
//! manifests), `Deserialize` (both parse them), and `PartialEq + Eq` (Extism's
//! export tests match on values).
//!
//! Mapping to the internal [`crate::traits::scalar::ArgType`] stays in each
//! loader, since the error types differ.

use serde::{Deserialize, Serialize};

/// Wire-level argument type shipped by a plugin manifest.
///
/// Primitive types use the lowercase Arrow names (`"int64"`, `"float64"`,
/// `"utf8"`, `"boolean"`, `"date64"`, `"timestamp_ms"`, `"binary"`,
/// `"largebinary"`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WireArgType {
    /// A native Arrow primitive — `kind: "primitive", arrow: "<name>"`.
    Primitive {
        /// Arrow primitive name.
        arrow: String,
    },
    /// A `CypherValue` shipped via `LargeBinary` opaque transport.
    CypherValue,
    /// A fixed-size vector — `kind: "vector", len: N, element: "<arrow>"`.
    Vector {
        /// Number of elements per row.
        len: usize,
        /// Element type.
        element: String,
    },
    /// Variadic — repeats `inner` zero or more times.
    Variadic {
        /// Inner element type.
        inner: Box<WireArgType>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shape that used to load over Extism and fail under the
    /// Component-Model loader.
    #[test]
    fn vector_manifest_parses() {
        let json = r#"{"kind":"vector","len":128,"element":"float32"}"#;
        let parsed: WireArgType = serde_json::from_str(json).expect("vector must parse");
        assert_eq!(
            parsed,
            WireArgType::Vector {
                len: 128,
                element: "float32".to_owned(),
            }
        );
    }

    #[test]
    fn variadic_manifest_parses() {
        let json = r#"{"kind":"variadic","inner":{"kind":"cypher_value"}}"#;
        let parsed: WireArgType = serde_json::from_str(json).expect("variadic must parse");
        assert_eq!(
            parsed,
            WireArgType::Variadic {
                inner: Box::new(WireArgType::CypherValue),
            }
        );
    }

    #[test]
    fn round_trips_through_json() {
        for value in [
            WireArgType::Primitive {
                arrow: "int64".to_owned(),
            },
            WireArgType::CypherValue,
            WireArgType::Vector {
                len: 4,
                element: "float64".to_owned(),
            },
            WireArgType::Variadic {
                inner: Box::new(WireArgType::Primitive {
                    arrow: "utf8".to_owned(),
                }),
            },
        ] {
            let json = serde_json::to_string(&value).expect("serialize");
            let back: WireArgType = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, value, "round-trip must be lossless for {value:?}");
        }
    }

    #[test]
    fn unknown_kind_is_rejected() {
        let json = r#"{"kind":"quaternion"}"#;
        assert!(serde_json::from_str::<WireArgType>(json).is_err());
    }

    /// `deny_unknown_fields` bites on struct variants...
    #[test]
    fn unknown_field_on_struct_variant_is_rejected() {
        let json = r#"{"kind":"primitive","arrow":"int64","bogus":1}"#;
        assert!(serde_json::from_str::<WireArgType>(json).is_err());
    }

    /// ...but NOT on unit variants. serde does not enforce
    /// `deny_unknown_fields` for a unit variant of an internally-tagged enum,
    /// so `cypher_value` silently tolerates junk keys. Pinned so the gap is a
    /// known property of the wire schema rather than a surprise.
    #[test]
    fn unknown_field_on_unit_variant_is_tolerated() {
        let json = r#"{"kind":"cypher_value","bogus":1}"#;
        assert_eq!(
            serde_json::from_str::<WireArgType>(json).expect("unit variant ignores extra keys"),
            WireArgType::CypherValue
        );
    }
}
