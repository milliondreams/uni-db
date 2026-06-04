// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Discriminated union over the three projection sources an algorithm
//! invocation can reference, per proposal §4.10.1.
//!
//! Every algorithm call uses the 2-arg `(graphRef, config)` shape where
//! `graphRef` is a `Map` that [`parse_graph_ref`] decodes into one of the
//! variants below. `Native` materialises immediately from labels + edge
//! types; `Cypher` runs inner queries through `QueryProcedureHost`;
//! `Named` resolves through the per-`Database` `ProjectionStore`.

use serde_json::Value;

/// Source of a graph projection for an algorithm invocation.
///
/// The dispatcher [`parse_graph_ref`] picks a variant based on which keys
/// the user supplied; conflicting key sets are rejected so the call site
/// cannot mix `nodeLabels` with `nodeQuery`.
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionInput {
    /// Build a CSR directly from native labels + edge types.
    Native {
        /// Vertex labels to include.
        node_labels: Vec<String>,
        /// Edge types to traverse.
        edge_types: Vec<String>,
        /// Optional edge property to read as scalar weight.
        weight_property: Option<String>,
        /// When `true`, build the reverse CSR alongside the forward one.
        include_reverse: bool,
    },
    /// Build a CSR from two inner Cypher queries; the node query must
    /// yield an `id` column, the edge query must yield `source`/`target`
    /// (and optionally the column named by `weight_column`).
    Cypher {
        /// Cypher query producing the node rows.
        node_query: String,
        /// Cypher query producing the edge rows.
        edge_query: String,
        /// Optional name of the column in the edge query carrying the
        /// scalar weight.
        weight_column: Option<String>,
        /// When `true`, build the reverse CSR alongside the forward one.
        include_reverse: bool,
    },
    /// Look up a previously materialised projection from the per-
    /// `Database` `ProjectionStore` (resolved by the host crate;
    /// `uni-algo` itself only holds the named-lookup variant).
    Named {
        /// Name the projection was registered under.
        name: String,
    },
}

/// Error returned by [`parse_graph_ref`] when the input map cannot be
/// decoded as exactly one [`ProjectionInput`] variant.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphRefParseError {
    /// Human-readable message — flagged through to the caller's
    /// `FnError::new(0x820, ...)` site.
    pub message: String,
}

impl std::fmt::Display for GraphRefParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for GraphRefParseError {}

fn err(msg: impl Into<String>) -> GraphRefParseError {
    GraphRefParseError {
        message: msg.into(),
    }
}

/// Decode a `graphRef` map into one of the [`ProjectionInput`] variants.
///
/// Selection rules:
/// - Presence of `nodeLabels` or `edgeTypes` → `Native`. `nodeQuery` /
///   `edgeQuery` / `name` must be absent.
/// - Presence of `nodeQuery` or `edgeQuery` → `Cypher`. Both must be
///   supplied. `nodeLabels` / `edgeTypes` / `name` must be absent.
/// - Presence of `name` (with no labels or queries) → `Named`.
/// - Anything else is a parse error.
///
/// Optional keys per variant:
/// - `Native`: `weightProperty: String`, `includeReverse: Bool`.
/// - `Cypher`: `weightColumn: String`, `includeReverse: Bool`.
///
/// # Errors
///
/// Returns [`GraphRefParseError`] when the input is not a map, when key
/// sets conflict (`nodeLabels` + `nodeQuery` together), when a required
/// `Cypher` query is missing, or when the value attached to a key has the
/// wrong shape (e.g. `nodeLabels` not an array of strings).
pub fn parse_graph_ref(v: &Value) -> Result<ProjectionInput, GraphRefParseError> {
    let map = v.as_object().ok_or_else(|| err("graphRef must be a Map"))?;

    let has_native = map.contains_key("nodeLabels") || map.contains_key("edgeTypes");
    let has_cypher = map.contains_key("nodeQuery") || map.contains_key("edgeQuery");
    let has_named = map.contains_key("name");

    let variants = [has_native, has_cypher, has_named];
    let selected = variants.iter().filter(|b| **b).count();
    if selected == 0 {
        return Err(err(
            "graphRef must contain one of: nodeLabels/edgeTypes (Native), \
             nodeQuery/edgeQuery (Cypher), or name (Named)",
        ));
    }
    if selected > 1 {
        return Err(err(
            "graphRef keys conflict: pick exactly one of Native (nodeLabels/edgeTypes), \
             Cypher (nodeQuery/edgeQuery), or Named (name)",
        ));
    }

    if has_native {
        let node_labels = map
            .get("nodeLabels")
            .map(parse_string_array)
            .transpose()?
            .unwrap_or_default();
        let edge_types = map
            .get("edgeTypes")
            .map(parse_string_array)
            .transpose()?
            .unwrap_or_default();
        let weight_property = map
            .get("weightProperty")
            .map(parse_optional_string)
            .transpose()?
            .flatten();
        let include_reverse = map
            .get("includeReverse")
            .map(parse_bool)
            .transpose()?
            .unwrap_or(true);
        Ok(ProjectionInput::Native {
            node_labels,
            edge_types,
            weight_property,
            include_reverse,
        })
    } else if has_cypher {
        let node_query = map
            .get("nodeQuery")
            .ok_or_else(|| err("Cypher graphRef requires nodeQuery"))?
            .as_str()
            .ok_or_else(|| err("nodeQuery must be a String"))?
            .to_owned();
        let edge_query = map
            .get("edgeQuery")
            .ok_or_else(|| err("Cypher graphRef requires edgeQuery"))?
            .as_str()
            .ok_or_else(|| err("edgeQuery must be a String"))?
            .to_owned();
        let weight_column = map
            .get("weightColumn")
            .map(parse_optional_string)
            .transpose()?
            .flatten();
        let include_reverse = map
            .get("includeReverse")
            .map(parse_bool)
            .transpose()?
            .unwrap_or(true);
        Ok(ProjectionInput::Cypher {
            node_query,
            edge_query,
            weight_column,
            include_reverse,
        })
    } else {
        let name = map
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| err("Named graphRef requires a String `name`"))?
            .to_owned();
        Ok(ProjectionInput::Named { name })
    }
}

fn parse_string_array(v: &Value) -> Result<Vec<String>, GraphRefParseError> {
    let arr = v.as_array().ok_or_else(|| err("expected a String array"))?;
    arr.iter()
        .map(|x| {
            x.as_str()
                .map(str::to_owned)
                .ok_or_else(|| err("array element must be a String"))
        })
        .collect()
}

fn parse_optional_string(v: &Value) -> Result<Option<String>, GraphRefParseError> {
    if v.is_null() {
        Ok(None)
    } else {
        v.as_str()
            .map(|s| Some(s.to_owned()))
            .ok_or_else(|| err("expected a String"))
    }
}

fn parse_bool(v: &Value) -> Result<bool, GraphRefParseError> {
    v.as_bool().ok_or_else(|| err("expected a Bool"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn native_minimal() {
        // `include_reverse` defaults to `true` so PageRank /
        // Louvain / WCC don't silently lose in-neighbors when the
        // caller omits the field.
        let v = json!({ "nodeLabels": ["Person"], "edgeTypes": ["KNOWS"] });
        let got = parse_graph_ref(&v).unwrap();
        assert_eq!(
            got,
            ProjectionInput::Native {
                node_labels: vec!["Person".to_owned()],
                edge_types: vec!["KNOWS".to_owned()],
                weight_property: None,
                include_reverse: true,
            }
        );
    }

    #[test]
    fn native_full() {
        let v = json!({
            "nodeLabels": ["Person"],
            "edgeTypes": ["KNOWS"],
            "weightProperty": "weight",
            "includeReverse": true,
        });
        let got = parse_graph_ref(&v).unwrap();
        match got {
            ProjectionInput::Native {
                weight_property,
                include_reverse,
                ..
            } => {
                assert_eq!(weight_property.as_deref(), Some("weight"));
                assert!(include_reverse);
            }
            _ => panic!("expected Native"),
        }
    }

    #[test]
    fn cypher_minimal() {
        let v = json!({
            "nodeQuery": "MATCH (p:Person) RETURN id(p) AS id",
            "edgeQuery": "MATCH (a)-[:KNOWS]->(b) RETURN id(a) AS source, id(b) AS target",
        });
        let got = parse_graph_ref(&v).unwrap();
        match got {
            ProjectionInput::Cypher {
                node_query,
                edge_query,
                weight_column,
                include_reverse,
            } => {
                assert!(node_query.starts_with("MATCH (p:Person)"));
                assert!(edge_query.starts_with("MATCH (a)"));
                assert_eq!(weight_column, None);
                // `include_reverse` defaults to `true` (see
                // `native_minimal` rationale).
                assert!(include_reverse);
            }
            _ => panic!("expected Cypher"),
        }
    }

    #[test]
    fn named() {
        let v = json!({ "name": "myGraph" });
        assert_eq!(
            parse_graph_ref(&v).unwrap(),
            ProjectionInput::Named {
                name: "myGraph".to_owned()
            }
        );
    }

    #[test]
    fn conflicting_keys_rejected() {
        let v = json!({ "nodeLabels": ["Person"], "name": "g" });
        let err = parse_graph_ref(&v).unwrap_err();
        assert!(err.message.contains("conflict"), "{}", err.message);
    }

    #[test]
    fn missing_cypher_partner_rejected() {
        let v = json!({ "nodeQuery": "RETURN 1 AS id" });
        let err = parse_graph_ref(&v).unwrap_err();
        assert!(err.message.contains("edgeQuery"), "{}", err.message);
    }

    #[test]
    fn empty_map_rejected() {
        let v = json!({});
        let err = parse_graph_ref(&v).unwrap_err();
        assert!(err.message.contains("must contain"), "{}", err.message);
    }

    #[test]
    fn non_map_rejected() {
        let v = json!("not a map");
        let err = parse_graph_ref(&v).unwrap_err();
        assert!(err.message.contains("must be a Map"), "{}", err.message);
    }
}
