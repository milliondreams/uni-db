//! Rhai-side manifest reader.
//!
//! Rhai plugins declare their identity, version, capabilities, and
//! provided functions by exporting a `uni_manifest()` function that
//! returns a Rhai map. This module compiles the script, calls the
//! function, and walks the returned `rhai::Map` into a structured
//! manifest the loader can use.
//!
//! Expected shape (Rhai source):
//!
//! ```rhai
//! fn uni_manifest() {
//!   #{
//!     id:          "ai.example.score",
//!     version:     "0.1.0",
//!     determinism: "pure",
//!     scalar_fns: [
//!       #{ name: "score", args: ["float","float"], returns: "float" },
//!     ],
//!     aggregate_fns: [
//!       #{ name: "stats", args: ["float"], returns: "map", state: "map" },
//!     ],
//!     procedures: [
//!       #{ name: "rows", args: [], yields: ["int","string"] },
//!     ],
//!   }
//! }
//! ```

#![cfg(feature = "rhai-runtime")]

use rhai::{AST, Dynamic, Engine, Map, Scope};

use crate::error::RhaiError;

/// Result of parsing a `uni_manifest()` return value.
#[derive(Debug, Clone, Default)]
pub struct RhaiManifest {
    /// Plugin id (`"ai.example.score"`).
    pub id: String,
    /// Plugin version (semver string).
    pub version: String,
    /// Determinism: `"pure"`, `"session"`, or `"nondeterministic"`.
    pub determinism: String,
    /// Declared scalar functions.
    pub scalar_fns: Vec<ScalarEntry>,
    /// Declared aggregate functions.
    pub aggregate_fns: Vec<AggregateEntry>,
    /// Declared procedures.
    pub procedures: Vec<ProcedureEntry>,
}

/// One scalar fn entry from the Rhai manifest.
#[derive(Debug, Clone)]
pub struct ScalarEntry {
    /// Function name as declared in the script (also the Rhai callable).
    pub name: String,
    /// Argument type names (`"float"`, `"int"`, …).
    pub args: Vec<String>,
    /// Return type name.
    pub returns: String,
    /// Opt-in vectorised mode — the function takes column userdata.
    /// Defaults to `false` (row mode).
    pub vectorized: bool,
}

/// One aggregate fn entry.
#[derive(Debug, Clone)]
pub struct AggregateEntry {
    /// Aggregate name; must also be the name of a `const` map in the
    /// script carrying `init` / `accumulate` / `merge` / `finalize`
    /// closures.
    pub name: String,
    /// Input type names.
    pub args: Vec<String>,
    /// Final return type name.
    pub returns: String,
    /// State type — informational; v1 always wraps as JSON-blob
    /// `LargeBinary` regardless.
    pub state: String,
}

/// One procedure entry.
#[derive(Debug, Clone)]
pub struct ProcedureEntry {
    /// Procedure name.
    pub name: String,
    /// Argument type names.
    pub args: Vec<String>,
    /// Yielded column type names (in declaration order).
    pub yields: Vec<String>,
    /// Mode: `"read"`, `"write"`, `"schema"`, or `"dbms"`. Default
    /// `"read"`.
    pub mode: String,
}

/// Compile a Rhai script into an AST.
///
/// Returns [`RhaiError::ParseFailed`] on syntax errors, with the Rhai
/// position information preserved in the error message.
pub fn compile(engine: &Engine, script: &str) -> Result<AST, RhaiError> {
    engine
        .compile(script)
        .map_err(|e| RhaiError::ParseFailed(format!("{e}")))
}

/// Call the script's `uni_manifest()` function and parse the returned
/// map into a [`RhaiManifest`].
pub fn parse_manifest(engine: &Engine, ast: &AST) -> Result<RhaiManifest, RhaiError> {
    let mut scope = Scope::new();
    let dynamic: Dynamic = engine
        .call_fn(&mut scope, ast, "uni_manifest", ())
        .map_err(|e| RhaiError::ManifestInvalid(format!("calling uni_manifest: {e}")))?;

    let map: Map = dynamic
        .try_cast::<Map>()
        .ok_or_else(|| RhaiError::ManifestInvalid("uni_manifest() must return a map".into()))?;

    let id = required_string(&map, "id")?;
    let version = required_string(&map, "version")?;
    let determinism = optional_string(&map, "determinism").unwrap_or_else(|| "pure".into());

    let scalar_fns = parse_scalar_entries(&map)?;
    let aggregate_fns = parse_aggregate_entries(&map)?;
    let procedures = parse_procedure_entries(&map)?;

    Ok(RhaiManifest {
        id,
        version,
        determinism,
        scalar_fns,
        aggregate_fns,
        procedures,
    })
}

fn parse_scalar_entries(map: &Map) -> Result<Vec<ScalarEntry>, RhaiError> {
    let Some(arr) = map.get("scalar_fns") else {
        return Ok(vec![]);
    };
    let arr = arr
        .clone()
        .try_cast::<rhai::Array>()
        .ok_or_else(|| RhaiError::ManifestInvalid("scalar_fns must be an array of maps".into()))?;
    let mut entries = Vec::with_capacity(arr.len());
    for d in arr {
        let m = d
            .try_cast::<Map>()
            .ok_or_else(|| RhaiError::ManifestInvalid("scalar_fns entry must be a map".into()))?;
        entries.push(ScalarEntry {
            name: required_string(&m, "name")?,
            args: required_string_array(&m, "args")?,
            returns: required_string(&m, "returns")?,
            vectorized: optional_bool(&m, "vectorized").unwrap_or(false),
        });
    }
    Ok(entries)
}

fn parse_aggregate_entries(map: &Map) -> Result<Vec<AggregateEntry>, RhaiError> {
    let Some(arr) = map.get("aggregate_fns") else {
        return Ok(vec![]);
    };
    let arr = arr
        .clone()
        .try_cast::<rhai::Array>()
        .ok_or_else(|| RhaiError::ManifestInvalid("aggregate_fns must be an array".into()))?;
    let mut entries = Vec::with_capacity(arr.len());
    for d in arr {
        let m = d.try_cast::<Map>().ok_or_else(|| {
            RhaiError::ManifestInvalid("aggregate_fns entry must be a map".into())
        })?;
        entries.push(AggregateEntry {
            name: required_string(&m, "name")?,
            args: required_string_array(&m, "args")?,
            returns: required_string(&m, "returns")?,
            state: optional_string(&m, "state").unwrap_or_else(|| "map".into()),
        });
    }
    Ok(entries)
}

fn parse_procedure_entries(map: &Map) -> Result<Vec<ProcedureEntry>, RhaiError> {
    let Some(arr) = map.get("procedures") else {
        return Ok(vec![]);
    };
    let arr = arr
        .clone()
        .try_cast::<rhai::Array>()
        .ok_or_else(|| RhaiError::ManifestInvalid("procedures must be an array".into()))?;
    let mut entries = Vec::with_capacity(arr.len());
    for d in arr {
        let m = d
            .try_cast::<Map>()
            .ok_or_else(|| RhaiError::ManifestInvalid("procedures entry must be a map".into()))?;
        entries.push(ProcedureEntry {
            name: required_string(&m, "name")?,
            args: required_string_array(&m, "args")?,
            yields: required_string_array(&m, "yields")?,
            mode: optional_string(&m, "mode").unwrap_or_else(|| "read".into()),
        });
    }
    Ok(entries)
}

fn required_string(map: &Map, key: &str) -> Result<String, RhaiError> {
    let dyn_val = map
        .get(key)
        .ok_or_else(|| RhaiError::ManifestInvalid(format!("missing required field `{key}`")))?;
    dyn_val
        .clone()
        .into_string()
        .map_err(|t| RhaiError::ManifestInvalid(format!("`{key}` must be a string (got {t})")))
}

fn optional_string(map: &Map, key: &str) -> Option<String> {
    map.get(key).and_then(|d| d.clone().into_string().ok())
}

fn optional_bool(map: &Map, key: &str) -> Option<bool> {
    map.get(key).and_then(|d| d.as_bool().ok())
}

fn required_string_array(map: &Map, key: &str) -> Result<Vec<String>, RhaiError> {
    let dyn_val = map
        .get(key)
        .ok_or_else(|| RhaiError::ManifestInvalid(format!("missing required field `{key}`")))?;
    let arr = dyn_val
        .clone()
        .try_cast::<rhai::Array>()
        .ok_or_else(|| RhaiError::ManifestInvalid(format!("`{key}` must be an array")))?;
    let mut out = Vec::with_capacity(arr.len());
    for (i, d) in arr.into_iter().enumerate() {
        let s = d.into_string().map_err(|t| {
            RhaiError::ManifestInvalid(format!("`{key}`[{i}] must be a string (got {t})"))
        })?;
        out.push(s);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::build_engine;
    use crate::host_fns::RhaiHostFnRegistry;
    use uni_plugin::CapabilitySet;

    fn engine() -> Engine {
        build_engine(&CapabilitySet::new(), &RhaiHostFnRegistry::new())
    }

    #[test]
    fn parses_minimal_manifest() {
        let script = r#"
            fn uni_manifest() {
                #{
                    id: "ai.test.min",
                    version: "0.1.0",
                    scalar_fns: [
                        #{ name: "score", args: ["float","float"], returns: "float" },
                    ],
                }
            }
            fn score(x, y) { x + y }
        "#;
        let eng = engine();
        let ast = compile(&eng, script).expect("compiles");
        let m = parse_manifest(&eng, &ast).expect("parses");
        assert_eq!(m.id, "ai.test.min");
        assert_eq!(m.version, "0.1.0");
        assert_eq!(m.determinism, "pure");
        assert_eq!(m.scalar_fns.len(), 1);
        assert_eq!(m.scalar_fns[0].name, "score");
        assert_eq!(m.scalar_fns[0].args, vec!["float", "float"]);
        assert_eq!(m.scalar_fns[0].returns, "float");
        assert!(!m.scalar_fns[0].vectorized);
    }

    #[test]
    fn missing_id_rejected() {
        let script = r#"
            fn uni_manifest() { #{ version: "0.1.0" } }
        "#;
        let eng = engine();
        let ast = compile(&eng, script).unwrap();
        let err = parse_manifest(&eng, &ast).unwrap_err();
        assert!(matches!(err, RhaiError::ManifestInvalid(_)));
    }

    #[test]
    fn parses_aggregate_and_procedure_entries() {
        let script = r#"
            fn uni_manifest() {
                #{
                    id: "ai.test.agg",
                    version: "0.1.0",
                    aggregate_fns: [
                        #{ name: "stats", args: ["float"], returns: "map", state: "map" },
                    ],
                    procedures: [
                        #{ name: "rows", args: [], yields: ["int","string"], mode: "read" },
                    ],
                }
            }
        "#;
        let eng = engine();
        let ast = compile(&eng, script).unwrap();
        let m = parse_manifest(&eng, &ast).unwrap();
        assert_eq!(m.aggregate_fns.len(), 1);
        assert_eq!(m.aggregate_fns[0].name, "stats");
        assert_eq!(m.procedures.len(), 1);
        assert_eq!(m.procedures[0].yields, vec!["int", "string"]);
    }
}
