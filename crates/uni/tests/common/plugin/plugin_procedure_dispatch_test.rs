#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for the M4 plugin-procedure dispatch path.
//!
//! These tests exercise the end-to-end flow: `BuiltinPlugin` /
//! `ApocCorePlugin` are auto-registered at `Uni::build()` time
//! (per `crates/uni/src/api/mod.rs::register_builtin_plugins`); their
//! procedures are reachable from Cypher `CALL` sites via
//! `ProcedureRegistry::resolve_user_procedure` (the namespace resolver
//! that maps `uni.X.Y` → `<plugin>.X.Y`).
//!
//! Per `docs/plans/plugin_framework_implementation.md` §M4, these are
//! the canary tests proving the plugin dispatch path works before more
//! procedures port to the plugin framework.

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn call_uni_system_echo_routes_through_plugin_path() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    // `uni.system.echo` is registered by BuiltinPlugin under
    // `QName::new("builtin", "system.echo")`. The namespace resolver
    // strips the `uni.` prefix and finds it in the `builtin` namespace.
    let result = db
        .session()
        .query("CALL uni.system.echo('hello plugin path') YIELD echo RETURN echo")
        .await?;
    assert_eq!(result.len(), 1);
    let echo: String = result.rows()[0].get("echo")?;
    assert_eq!(echo, "hello plugin path");
    Ok(())
}

#[tokio::test]
async fn call_uni_bitwise_and_routes_through_apoc_core_plugin() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    // `uni.bitwise.and` is registered by ApocCorePlugin under
    // `QName::new("apoc-core", "bitwise.and")`. The namespace resolver
    // strips `uni.` and finds it in the `apoc-core` namespace.
    let result = db
        .session()
        .query("CALL uni.bitwise.and(12, 10) YIELD result RETURN result")
        .await?;
    assert_eq!(result.len(), 1);
    let v: i64 = result.rows()[0].get("result")?;
    assert_eq!(v, 12 & 10);
    Ok(())
}

#[tokio::test]
async fn call_uni_bitwise_or_returns_expected_value() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.bitwise.or(12, 10) YIELD result RETURN result")
        .await?;
    let v: i64 = result.rows()[0].get("result")?;
    assert_eq!(v, 12 | 10);
    Ok(())
}

#[tokio::test]
async fn call_uni_bitwise_xor_returns_expected_value() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.bitwise.xor(12, 10) YIELD result RETURN result")
        .await?;
    let v: i64 = result.rows()[0].get("result")?;
    assert_eq!(v, 12 ^ 10);
    Ok(())
}

#[tokio::test]
async fn call_uni_bitwise_not_returns_expected_value() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.bitwise.not(0) YIELD result RETURN result")
        .await?;
    let v: i64 = result.rows()[0].get("result")?;
    assert_eq!(v, !0i64);
    Ok(())
}

#[tokio::test]
async fn call_uni_bitwise_shift_left_returns_expected_value() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.bitwise.shiftLeft(1, 4) YIELD result RETURN result")
        .await?;
    let v: i64 = result.rows()[0].get("result")?;
    assert_eq!(v, 16);
    Ok(())
}

#[tokio::test]
async fn call_uni_bitwise_shift_right_returns_expected_value() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.bitwise.shiftRight(64, 2) YIELD result RETURN result")
        .await?;
    let v: i64 = result.rows()[0].get("result")?;
    assert_eq!(v, 16);
    Ok(())
}

// ---------------------------------------------------------------------------
// uni.create.* — ApocCorePlugin synthesizers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn call_uni_create_uuid_returns_36_char_string() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.create.uuid() YIELD result RETURN result")
        .await?;
    let s: String = result.rows()[0].get("result")?;
    assert_eq!(s.len(), 36, "UUID should be 36 chars; got {s:?}");
    assert_eq!(s.chars().nth(14).unwrap(), '4');
    Ok(())
}

// ---------------------------------------------------------------------------
// uni.text.* — extended procedures (trim, contains, length)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn call_uni_text_trim() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.text.trim('  hello  ') YIELD result RETURN result")
        .await?;
    let s: String = result.rows()[0].get("result")?;
    assert_eq!(s, "hello");
    Ok(())
}

#[tokio::test]
async fn call_uni_text_contains_true() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.text.contains('hello world', 'world') YIELD result RETURN result")
        .await?;
    let v: bool = result.rows()[0].get("result")?;
    assert!(v);
    Ok(())
}

#[tokio::test]
async fn call_uni_text_repeat() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.text.repeat('ab', 3) YIELD result RETURN result")
        .await?;
    let s: String = result.rows()[0].get("result")?;
    assert_eq!(s, "ababab");
    Ok(())
}

#[tokio::test]
async fn call_uni_text_index_of_found() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.text.indexOf('hello world', 'world') YIELD result RETURN result")
        .await?;
    let v: i64 = result.rows()[0].get("result")?;
    assert_eq!(v, 6);
    Ok(())
}

#[tokio::test]
async fn call_uni_text_index_of_not_found() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.text.indexOf('hello', 'zzz') YIELD result RETURN result")
        .await?;
    let v: i64 = result.rows()[0].get("result")?;
    assert_eq!(v, -1);
    Ok(())
}

#[tokio::test]
async fn call_uni_text_length() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.text.length('café') YIELD result RETURN result")
        .await?;
    let v: i64 = result.rows()[0].get("result")?;
    // 'café' has 4 Unicode scalar values (chars), 5 bytes in UTF-8.
    assert_eq!(v, 4);
    Ok(())
}

// ---------------------------------------------------------------------------
// uni.convert.* — ApocCorePlugin type conversions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn call_uni_convert_to_integer_from_float() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.convert.toInteger(3.9) YIELD result RETURN result")
        .await?;
    let v: i64 = result.rows()[0].get("result")?;
    assert_eq!(v, 3);
    Ok(())
}

#[tokio::test]
async fn call_uni_convert_to_boolean_from_int() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.convert.toBoolean(1) YIELD result RETURN result")
        .await?;
    let v: bool = result.rows()[0].get("result")?;
    assert!(v);
    Ok(())
}

// ---------------------------------------------------------------------------
// uni.number.* — additional ApocCorePlugin procedures
// ---------------------------------------------------------------------------

#[tokio::test]
async fn call_uni_number_parse_int_valid() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.number.parseInt('42') YIELD result RETURN result")
        .await?;
    let v: i64 = result.rows()[0].get("result")?;
    assert_eq!(v, 42);
    Ok(())
}

#[tokio::test]
async fn call_uni_number_to_string_formats() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.number.toString(2.5) YIELD result RETURN result")
        .await?;
    let s: String = result.rows()[0].get("result")?;
    assert_eq!(s, "2.5");
    Ok(())
}

// ---------------------------------------------------------------------------
// uni.math.* — additional ApocCorePlugin procedures
// ---------------------------------------------------------------------------

#[tokio::test]
async fn call_uni_math_sigmoid_at_zero_is_half() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.math.sigmoid(0.0) YIELD result RETURN result")
        .await?;
    let v: f64 = result.rows()[0].get("result")?;
    assert!((v - 0.5).abs() < 1e-12);
    Ok(())
}

#[tokio::test]
async fn call_uni_math_tanh_at_zero_is_zero() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.math.tanh(0.0) YIELD result RETURN result")
        .await?;
    let v: f64 = result.rows()[0].get("result")?;
    assert!(v.abs() < 1e-12);
    Ok(())
}

#[tokio::test]
async fn call_uni_math_cosh_at_zero_is_one() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.math.cosh(0.0) YIELD result RETURN result")
        .await?;
    let v: f64 = result.rows()[0].get("result")?;
    assert!((v - 1.0).abs() < 1e-12);
    Ok(())
}

// ---------------------------------------------------------------------------
// uni.text.* — additional ApocCorePlugin procedures
// ---------------------------------------------------------------------------

#[tokio::test]
async fn call_uni_text_to_upper_uppercases() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.text.toUpper('hello') YIELD result RETURN result")
        .await?;
    let s: String = result.rows()[0].get("result")?;
    assert_eq!(s, "HELLO");
    Ok(())
}

#[tokio::test]
async fn call_uni_text_to_lower_lowercases() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.text.toLower('HELLO') YIELD result RETURN result")
        .await?;
    let s: String = result.rows()[0].get("result")?;
    assert_eq!(s, "hello");
    Ok(())
}

#[tokio::test]
async fn call_uni_text_replace_substitutes() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.text.replace('foo bar foo', 'foo', 'baz') YIELD result RETURN result")
        .await?;
    let s: String = result.rows()[0].get("result")?;
    assert_eq!(s, "baz bar baz");
    Ok(())
}

#[tokio::test]
async fn call_uni_text_reverse_handles_unicode() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.text.reverse('café') YIELD result RETURN result")
        .await?;
    let s: String = result.rows()[0].get("result")?;
    assert_eq!(s, "éfac");
    Ok(())
}

// ---------------------------------------------------------------------------
// uni.plugin.* — meta-plugin (apoc.custom analogue, M9 partial)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn call_uni_plugin_list_declared_returns_empty_initially() -> Result<()> {
    // A fresh Uni has no declared plugins; the listDeclared procedure
    // returns zero rows.
    let db = Uni::in_memory().build().await?;
    let result = db
        .session()
        .query("CALL uni.plugin.listDeclared() YIELD qname, kind, declared_by, active RETURN qname")
        .await?;
    assert_eq!(
        result.len(),
        0,
        "expected zero declared plugins on a fresh DB"
    );
    Ok(())
}

#[tokio::test]
async fn call_unknown_uni_namespace_falls_through_to_legacy_dispatch() -> Result<()> {
    // `uni.schema.labels` is NOT yet ported to a plugin; it still runs
    // via the legacy hardcoded dispatch. The plugin lookup should miss
    // and the legacy path should serve it.
    let db = Uni::in_memory().build().await?;
    db.schema().label("Sentinel").apply().await?;
    let result = db
        .session()
        .query("CALL uni.schema.labels() YIELD label RETURN label")
        .await?;
    let labels: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("label").unwrap())
        .collect();
    assert!(labels.contains(&"Sentinel".to_string()));
    Ok(())
}
