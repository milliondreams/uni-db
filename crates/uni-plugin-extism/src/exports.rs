//! Plugin-export readers — `manifest` and `register`.
//!
//! Every Extism plugin exposes two canonical-JSON control-surface exports:
//!
//! - **`manifest`** — returns the plugin's [`ExtismPluginManifest`]
//!   (id, version, declared capabilities, resource limits, …). Read once
//!   at load time to drive the capability intersection.
//! - **`register`** — returns a [`RegistrationManifest`] enumerating every
//!   qname the plugin provides plus its wire-level signature. Read after
//!   capability negotiation; one [`RegistrationEntry`] is converted to a
//!   `ScalarPluginFn` / `AggregatePluginFn` / `ProcedurePlugin` adapter
//!   downstream (M6a.1.5).
//!
//! This module splits parsing (pure, byte-slice in / value out) from the
//! Extism-call wrapper (`read_*_export`). The split lets us unit-test JSON
//! contracts without standing up a wasm plugin; the call-wrapper is
//! exercised end-to-end by the M6a.1.7 example plugin.

use serde::{Deserialize, Serialize};

use crate::error::ExtismError;
use crate::loader::ExtismPluginManifest;

/// Wire-level scalar / aggregate / procedure signature shipped by a
/// plugin's `register` export.
///
/// String-based for wire stability — plugins shouldn't have to encode
/// `arrow_schema::DataType` JSON. Translation to internal `FnSignature`
/// / `AggSignature` / `ProcedureSignature` happens at adapter
/// construction time (M6a.1.5 / M6a.2).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WireFnSignature {
    /// Argument types in `WireArgType` form.
    pub args: Vec<WireArgType>,
    /// Return type.
    pub returns: WireArgType,
    /// Volatility — `"immutable"`, `"stable"`, or `"volatile"`. Default
    /// `"immutable"`.
    #[serde(default = "default_volatility")]
    pub volatility: String,
    /// Null handling — `"propagate"` (default) or `"user_handled"`.
    #[serde(default = "default_null_handling")]
    pub null_handling: String,
}

fn default_volatility() -> String {
    "immutable".to_owned()
}

fn default_null_handling() -> String {
    "propagate".to_owned()
}

/// Wire-level argument type shipped by a plugin.
///
/// Each variant maps to the corresponding `uni_plugin::traits::scalar::ArgType`
/// at adapter time. Primitive types use the lowercase Arrow names
/// (`"int64"`, `"float64"`, `"utf8"`, `"boolean"`, `"date64"`,
/// `"timestamp_ms"`, `"binary"`, `"largebinary"`).
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

/// One registration entry — a single qname plus its kind + signature.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RegistrationEntry {
    /// A Cypher scalar function.
    Scalar {
        /// Fully-qualified name (`"ns.fn"`).
        qname: String,
        /// Signature.
        signature: WireFnSignature,
    },
    /// A Cypher aggregate function. Wire shape mirrors Scalar with the
    /// state type carried as a separate `WireArgType`.
    Aggregate {
        /// Fully-qualified name.
        qname: String,
        /// Per-row input + return types.
        signature: WireFnSignature,
        /// State type — opaque to the wire; Adapter side wraps as
        /// Arrow Binary.
        state: WireArgType,
    },
    /// A Cypher procedure.
    Procedure {
        /// Fully-qualified name.
        qname: String,
        /// Argument signatures.
        args: Vec<WireArgType>,
        /// Yielded column types, in declared order.
        yields: Vec<WireArgType>,
        /// Mode — `"read"`, `"write"`, `"schema"`, or `"dbms"`. Default `"read"`.
        #[serde(default = "default_proc_mode")]
        mode: String,
    },
}

fn default_proc_mode() -> String {
    "read".to_owned()
}

impl RegistrationEntry {
    /// Fully-qualified name of this entry.
    #[must_use]
    pub fn qname(&self) -> &str {
        match self {
            Self::Scalar { qname, .. }
            | Self::Aggregate { qname, .. }
            | Self::Procedure { qname, .. } => qname,
        }
    }
}

/// Top-level `register` export payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RegistrationManifest {
    /// One entry per qname provided by the plugin.
    pub entries: Vec<RegistrationEntry>,
}

/// Parse the bytes returned by a plugin's `manifest` export into an
/// [`ExtismPluginManifest`].
///
/// # Errors
///
/// - [`ExtismError::ManifestInvalid`] if the JSON doesn't parse or
///   doesn't match the expected shape.
pub fn parse_manifest_json(bytes: &[u8]) -> Result<ExtismPluginManifest, ExtismError> {
    serde_json::from_slice(bytes)
        .map_err(|e| ExtismError::ManifestInvalid(format!("json parse: {e}")))
}

/// Parse the bytes returned by a plugin's `register` export into a
/// [`RegistrationManifest`].
///
/// # Errors
///
/// - [`ExtismError::OutputDecode`] if the JSON doesn't parse or doesn't
///   match the expected shape.
pub fn parse_registration_json(bytes: &[u8]) -> Result<RegistrationManifest, ExtismError> {
    serde_json::from_slice(bytes)
        .map_err(|e| ExtismError::OutputDecode(format!("register json parse: {e}")))
}

/// Call a live plugin's `manifest` export and parse the response.
///
/// The `manifest` export takes no input and returns canonical-JSON
/// matching [`ExtismPluginManifest`]. The plugin produces this once and
/// caches internally; the host reads it once at load and never again.
///
/// # Errors
///
/// - [`ExtismError::InvalidPlugin`] if the export doesn't exist or the
///   underlying Extism call fails.
/// - [`ExtismError::ManifestInvalid`] if the returned JSON is malformed.
pub fn read_manifest_export(
    plugin: &mut extism::Plugin,
) -> Result<ExtismPluginManifest, ExtismError> {
    if !plugin.function_exists("manifest") {
        return Err(ExtismError::InvalidPlugin(
            "plugin does not export required `manifest` function".to_owned(),
        ));
    }
    let bytes: &[u8] = plugin
        .call("manifest", "")
        .map_err(|e| ExtismError::InvalidPlugin(format!("call manifest: {e}")))?;
    parse_manifest_json(bytes)
}

/// Call a live plugin's `register` export and parse the response.
///
/// The `register` export takes no input and returns canonical-JSON
/// matching [`RegistrationManifest`]. The host reads this after
/// capability negotiation and converts each entry into an adapter
/// implementing the corresponding capability trait.
///
/// # Errors
///
/// - [`ExtismError::InvalidPlugin`] if the export doesn't exist or the
///   underlying Extism call fails.
/// - [`ExtismError::OutputDecode`] if the returned JSON is malformed.
pub fn read_register_export(
    plugin: &mut extism::Plugin,
) -> Result<RegistrationManifest, ExtismError> {
    if !plugin.function_exists("register") {
        return Err(ExtismError::InvalidPlugin(
            "plugin does not export required `register` function".to_owned(),
        ));
    }
    let bytes: &[u8] = plugin
        .call("register", "")
        .map_err(|e| ExtismError::InvalidPlugin(format!("call register: {e}")))?;
    parse_registration_json(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_manifest() {
        let json = br#"{"id":"a.b","version":"0.0.1"}"#;
        let m = parse_manifest_json(json).unwrap();
        assert_eq!(m.id, "a.b");
        assert_eq!(m.version, "0.0.1");
        assert!(m.capabilities.is_empty());
        assert!(m.fuel_per_call.is_none());
    }

    #[test]
    fn parses_manifest_with_resource_limits() {
        let json = br#"{
            "id": "a.b",
            "version": "0.0.1",
            "capabilities": ["Filesystem"],
            "fuel_per_call": 1000,
            "memory_max_pages": 4,
            "timeout_ms": 500
        }"#;
        let m = parse_manifest_json(json).unwrap();
        assert_eq!(m.fuel_per_call, Some(1000));
        assert_eq!(m.memory_max_pages, Some(4));
        assert_eq!(m.timeout_ms, Some(500));
        assert_eq!(m.capabilities, vec!["Filesystem".to_owned()]);
    }

    #[test]
    fn rejects_unknown_manifest_field() {
        let json = br#"{"id":"a.b","version":"0.0.1","mystery":"surprise"}"#;
        let err = parse_manifest_json(json).unwrap_err();
        assert!(matches!(err, ExtismError::ManifestInvalid(_)));
    }

    #[test]
    fn parses_empty_registration() {
        let json = br#"{"entries":[]}"#;
        let r = parse_registration_json(json).unwrap();
        assert!(r.entries.is_empty());
    }

    #[test]
    fn parses_scalar_registration_entry() {
        let json = br#"{
            "entries": [{
                "kind": "scalar",
                "qname": "geo.haversine",
                "signature": {
                    "args": [
                        {"kind":"primitive","arrow":"float64"},
                        {"kind":"primitive","arrow":"float64"},
                        {"kind":"primitive","arrow":"float64"},
                        {"kind":"primitive","arrow":"float64"}
                    ],
                    "returns": {"kind":"primitive","arrow":"float64"}
                }
            }]
        }"#;
        let r = parse_registration_json(json).unwrap();
        assert_eq!(r.entries.len(), 1);
        match &r.entries[0] {
            RegistrationEntry::Scalar { qname, signature } => {
                assert_eq!(qname, "geo.haversine");
                assert_eq!(signature.args.len(), 4);
                assert_eq!(signature.volatility, "immutable");
                assert_eq!(signature.null_handling, "propagate");
                assert!(matches!(
                    signature.returns,
                    WireArgType::Primitive { ref arrow } if arrow == "float64"
                ));
            }
            other => panic!("expected Scalar, got: {other:?}"),
        }
    }

    #[test]
    fn parses_aggregate_registration_entry() {
        let json = br#"{
            "entries": [{
                "kind": "aggregate",
                "qname": "stats.weighted_mean",
                "signature": {
                    "args": [
                        {"kind":"primitive","arrow":"float64"},
                        {"kind":"primitive","arrow":"float64"}
                    ],
                    "returns": {"kind":"primitive","arrow":"float64"},
                    "volatility": "stable"
                },
                "state": {"kind":"primitive","arrow":"binary"}
            }]
        }"#;
        let r = parse_registration_json(json).unwrap();
        match &r.entries[0] {
            RegistrationEntry::Aggregate {
                qname,
                signature,
                state,
            } => {
                assert_eq!(qname, "stats.weighted_mean");
                assert_eq!(signature.volatility, "stable");
                assert!(matches!(state, WireArgType::Primitive { arrow } if arrow == "binary"));
            }
            other => panic!("expected Aggregate, got: {other:?}"),
        }
    }

    #[test]
    fn parses_procedure_registration_entry() {
        let json = br#"{
            "entries": [{
                "kind": "procedure",
                "qname": "myorg.scan",
                "args": [{"kind":"primitive","arrow":"utf8"}],
                "yields": [
                    {"kind":"primitive","arrow":"int64"},
                    {"kind":"cypher_value"}
                ],
                "mode": "write"
            }]
        }"#;
        let r = parse_registration_json(json).unwrap();
        match &r.entries[0] {
            RegistrationEntry::Procedure {
                qname,
                args,
                yields,
                mode,
            } => {
                assert_eq!(qname, "myorg.scan");
                assert_eq!(args.len(), 1);
                assert_eq!(yields.len(), 2);
                assert_eq!(mode, "write");
                assert!(matches!(yields[1], WireArgType::CypherValue));
            }
            other => panic!("expected Procedure, got: {other:?}"),
        }
    }

    #[test]
    fn procedure_mode_defaults_to_read() {
        let json = br#"{
            "entries": [{
                "kind": "procedure",
                "qname": "myorg.scan",
                "args": [],
                "yields": []
            }]
        }"#;
        let r = parse_registration_json(json).unwrap();
        match &r.entries[0] {
            RegistrationEntry::Procedure { mode, .. } => assert_eq!(mode, "read"),
            _ => unreachable!(),
        }
    }

    #[test]
    fn registration_entry_exposes_qname() {
        let e = RegistrationEntry::Scalar {
            qname: "x.y".to_owned(),
            signature: WireFnSignature {
                args: vec![],
                returns: WireArgType::CypherValue,
                volatility: "immutable".to_owned(),
                null_handling: "propagate".to_owned(),
            },
        };
        assert_eq!(e.qname(), "x.y");
    }

    #[test]
    fn rejects_unknown_registration_kind() {
        let json = br#"{"entries":[{"kind":"telegraphic","qname":"x"}]}"#;
        let err = parse_registration_json(json).unwrap_err();
        assert!(matches!(err, ExtismError::OutputDecode(_)));
    }

    #[test]
    fn parses_vector_and_variadic_argtypes() {
        let json = br#"{
            "entries": [{
                "kind": "scalar",
                "qname": "vec.norm",
                "signature": {
                    "args": [
                        {"kind":"vector","len":128,"element":"float32"},
                        {"kind":"variadic","inner":{"kind":"primitive","arrow":"int64"}}
                    ],
                    "returns": {"kind":"primitive","arrow":"float32"}
                }
            }]
        }"#;
        let r = parse_registration_json(json).unwrap();
        match &r.entries[0] {
            RegistrationEntry::Scalar { signature, .. } => {
                assert!(matches!(
                    signature.args[0],
                    WireArgType::Vector { len: 128, ref element } if element == "float32"
                ));
                assert!(matches!(
                    signature.args[1],
                    WireArgType::Variadic { ref inner } if matches!(
                        **inner,
                        WireArgType::Primitive { ref arrow } if arrow == "int64"
                    )
                ));
            }
            _ => unreachable!(),
        }
    }
}
