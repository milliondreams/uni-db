//! Built-in logical-type registrations (Arrow extension types).
//!
//! M5g scaffolding: registers `geo.point` (Float64×2) and `uri` (Utf8)
//! placeholder extension types so plugins building against these
//! identifiers find them at load time. Full GIS integration arrives in
//! a follow-up commit.

use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use uni_plugin::traits::types::LogicalTypeProvider;
use uni_plugin::{FnError, PluginError, PluginRegistrar};

/// Register built-in logical types.
///
/// # Errors
///
/// Returns [`PluginError`] on duplicate registration.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.logical_type(Arc::new(UriLogicalType))?;
    r.logical_type(Arc::new(GeoPointLogicalType))?;
    r.logical_type(Arc::new(EmailLogicalType))?;
    r.logical_type(Arc::new(Ipv4LogicalType))?;
    r.logical_type(Arc::new(Ipv6LogicalType))?;
    Ok(())
}

/// URI logical type — Utf8-backed, validates RFC-3986 shape on parse.
#[derive(Debug)]
pub struct UriLogicalType;

impl LogicalTypeProvider for UriLogicalType {
    fn name(&self) -> &str {
        "uri"
    }
    fn arrow_type(&self) -> DataType {
        DataType::Utf8
    }
    fn from_literal(&self, s: &str) -> Result<ScalarValue, FnError> {
        // Minimal validation: must contain `://` or start with a scheme:
        if !looks_like_uri(s) {
            return Err(FnError::new(
                0x900,
                format!("uri: `{s}` is not a recognized URI shape"),
            ));
        }
        Ok(ScalarValue::Utf8(Some(s.to_owned())))
    }
    fn to_display(&self, v: &ScalarValue) -> Result<String, FnError> {
        match v {
            ScalarValue::Utf8(Some(s)) => Ok(s.clone()),
            ScalarValue::Utf8(None) => Ok(String::new()),
            other => Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("uri::to_display: expected Utf8, got {other:?}"),
            )),
        }
    }
    fn cast_to(&self, v: &ColumnarValue, target: &DataType) -> Result<ColumnarValue, FnError> {
        if matches!(target, DataType::Utf8) {
            Ok(v.clone())
        } else {
            Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("uri::cast_to: unsupported target {target:?}"),
            ))
        }
    }
    fn cast_from(&self, v: &ColumnarValue) -> Result<ColumnarValue, FnError> {
        Ok(v.clone())
    }
}

fn looks_like_uri(s: &str) -> bool {
    if let Some(idx) = s.find(':') {
        // Scheme portion must be non-empty alpha.
        let scheme = &s[..idx];
        if !scheme.is_empty()
            && scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.')
        {
            return true;
        }
    }
    false
}

/// `geo.point` logical type — placeholder (full GIS in follow-up).
#[derive(Debug)]
pub struct GeoPointLogicalType;

impl LogicalTypeProvider for GeoPointLogicalType {
    fn name(&self) -> &str {
        "geo.point"
    }
    fn arrow_type(&self) -> DataType {
        // Backed by a Utf8 well-known-text representation in this scaffold.
        // M5g cutover swaps to a FixedSizeBinary or struct of Float64×2.
        DataType::Utf8
    }
    fn from_literal(&self, s: &str) -> Result<ScalarValue, FnError> {
        // Expect "POINT(x y)" — the OGC WKT shape.
        let trimmed = s.trim();
        if !(trimmed.starts_with("POINT(") && trimmed.ends_with(')')) {
            return Err(FnError::new(
                0x901,
                format!("geo.point: expected `POINT(x y)`, got `{s}`"),
            ));
        }
        let body = &trimmed[6..trimmed.len() - 1];
        let mut parts = body.split_whitespace();
        let x: f64 = parts
            .next()
            .ok_or_else(|| FnError::new(0x902, "geo.point: missing x"))?
            .parse()
            .map_err(|e| FnError::new(0x902, format!("geo.point x: {e}")))?;
        let y: f64 = parts
            .next()
            .ok_or_else(|| FnError::new(0x903, "geo.point: missing y"))?
            .parse()
            .map_err(|e| FnError::new(0x903, format!("geo.point y: {e}")))?;
        Ok(ScalarValue::Utf8(Some(format!("POINT({x} {y})"))))
    }
    fn to_display(&self, v: &ScalarValue) -> Result<String, FnError> {
        match v {
            ScalarValue::Utf8(Some(s)) => Ok(s.clone()),
            ScalarValue::Utf8(None) => Ok("POINT EMPTY".to_owned()),
            other => Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("geo.point::to_display: expected Utf8, got {other:?}"),
            )),
        }
    }
    fn cast_to(&self, v: &ColumnarValue, target: &DataType) -> Result<ColumnarValue, FnError> {
        if matches!(target, DataType::Utf8) {
            Ok(v.clone())
        } else {
            Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("geo.point::cast_to: unsupported target {target:?}"),
            ))
        }
    }
    fn cast_from(&self, v: &ColumnarValue) -> Result<ColumnarValue, FnError> {
        Ok(v.clone())
    }
}

// =========================================================================
// email — RFC-5322-ish shape over Utf8.
// =========================================================================

/// `email` logical type — minimal-validation `local@domain` shape over Utf8.
///
/// Validation is intentionally conservative (single `@`, non-empty
/// local + domain, no whitespace). Full RFC-5322 conformance is out
/// of scope; users who need it should ship a stricter logical type
/// as a plugin.
#[derive(Debug)]
pub struct EmailLogicalType;

impl LogicalTypeProvider for EmailLogicalType {
    fn name(&self) -> &str {
        "email"
    }
    fn arrow_type(&self) -> DataType {
        DataType::Utf8
    }
    fn from_literal(&self, s: &str) -> Result<ScalarValue, FnError> {
        if !looks_like_email(s) {
            return Err(FnError::new(
                0x910,
                format!("email: `{s}` is not a `local@domain` shape"),
            ));
        }
        Ok(ScalarValue::Utf8(Some(s.to_owned())))
    }
    fn to_display(&self, v: &ScalarValue) -> Result<String, FnError> {
        match v {
            ScalarValue::Utf8(Some(s)) => Ok(s.clone()),
            ScalarValue::Utf8(None) => Ok(String::new()),
            other => Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("email::to_display: expected Utf8, got {other:?}"),
            )),
        }
    }
    fn cast_to(&self, v: &ColumnarValue, target: &DataType) -> Result<ColumnarValue, FnError> {
        if matches!(target, DataType::Utf8) {
            Ok(v.clone())
        } else {
            Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("email::cast_to: unsupported target {target:?}"),
            ))
        }
    }
    fn cast_from(&self, v: &ColumnarValue) -> Result<ColumnarValue, FnError> {
        Ok(v.clone())
    }
}

fn looks_like_email(s: &str) -> bool {
    let mut parts = s.split('@');
    let local = parts.next();
    let domain = parts.next();
    let extra = parts.next();
    matches!(
        (local, domain, extra),
        (Some(l), Some(d), None) if !l.is_empty() && !d.is_empty()
            && !l.chars().any(char::is_whitespace)
            && !d.chars().any(char::is_whitespace)
            && d.contains('.')
    )
}

// =========================================================================
// ipv4 / ipv6 — parsed via std::net.
// =========================================================================

/// `ipv4` logical type — backed by Utf8, validated via [`std::net::Ipv4Addr`].
#[derive(Debug)]
pub struct Ipv4LogicalType;

impl LogicalTypeProvider for Ipv4LogicalType {
    fn name(&self) -> &str {
        "ipv4"
    }
    fn arrow_type(&self) -> DataType {
        DataType::Utf8
    }
    fn from_literal(&self, s: &str) -> Result<ScalarValue, FnError> {
        s.parse::<std::net::Ipv4Addr>()
            .map(|ip| ScalarValue::Utf8(Some(ip.to_string())))
            .map_err(|e| FnError::new(0x920, format!("ipv4: parse `{s}`: {e}")))
    }
    fn to_display(&self, v: &ScalarValue) -> Result<String, FnError> {
        match v {
            ScalarValue::Utf8(Some(s)) => Ok(s.clone()),
            ScalarValue::Utf8(None) => Ok(String::new()),
            other => Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("ipv4::to_display: expected Utf8, got {other:?}"),
            )),
        }
    }
    fn cast_to(&self, v: &ColumnarValue, target: &DataType) -> Result<ColumnarValue, FnError> {
        if matches!(target, DataType::Utf8) {
            Ok(v.clone())
        } else {
            Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("ipv4::cast_to: unsupported target {target:?}"),
            ))
        }
    }
    fn cast_from(&self, v: &ColumnarValue) -> Result<ColumnarValue, FnError> {
        Ok(v.clone())
    }
}

/// `ipv6` logical type — backed by Utf8, validated via [`std::net::Ipv6Addr`].
#[derive(Debug)]
pub struct Ipv6LogicalType;

impl LogicalTypeProvider for Ipv6LogicalType {
    fn name(&self) -> &str {
        "ipv6"
    }
    fn arrow_type(&self) -> DataType {
        DataType::Utf8
    }
    fn from_literal(&self, s: &str) -> Result<ScalarValue, FnError> {
        s.parse::<std::net::Ipv6Addr>()
            .map(|ip| ScalarValue::Utf8(Some(ip.to_string())))
            .map_err(|e| FnError::new(0x930, format!("ipv6: parse `{s}`: {e}")))
    }
    fn to_display(&self, v: &ScalarValue) -> Result<String, FnError> {
        match v {
            ScalarValue::Utf8(Some(s)) => Ok(s.clone()),
            ScalarValue::Utf8(None) => Ok(String::new()),
            other => Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("ipv6::to_display: expected Utf8, got {other:?}"),
            )),
        }
    }
    fn cast_to(&self, v: &ColumnarValue, target: &DataType) -> Result<ColumnarValue, FnError> {
        if matches!(target, DataType::Utf8) {
            Ok(v.clone())
        } else {
            Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("ipv6::cast_to: unsupported target {target:?}"),
            ))
        }
    }
    fn cast_from(&self, v: &ColumnarValue) -> Result<ColumnarValue, FnError> {
        Ok(v.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn email_from_literal_accepts_simple() {
        let t = EmailLogicalType;
        assert!(t.from_literal("a@example.com").is_ok());
    }

    #[test]
    fn email_from_literal_rejects_no_at() {
        assert!(EmailLogicalType.from_literal("noemail").is_err());
    }

    #[test]
    fn email_from_literal_rejects_no_domain_dot() {
        assert!(EmailLogicalType.from_literal("a@localhost").is_err());
    }

    #[test]
    fn ipv4_from_literal_accepts() {
        match Ipv4LogicalType.from_literal("192.168.1.1").unwrap() {
            ScalarValue::Utf8(Some(s)) => assert_eq!(s, "192.168.1.1"),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn ipv4_from_literal_rejects_garbage() {
        assert!(Ipv4LogicalType.from_literal("not-an-ip").is_err());
        assert!(Ipv4LogicalType.from_literal("256.0.0.1").is_err());
    }

    #[test]
    fn ipv6_from_literal_accepts() {
        assert!(Ipv6LogicalType.from_literal("::1").is_ok());
        assert!(
            Ipv6LogicalType
                .from_literal("2001:0db8:85a3::8a2e:0370:7334")
                .is_ok()
        );
    }

    #[test]
    fn extended_type_names_are_stable() {
        assert_eq!(EmailLogicalType.name(), "email");
        assert_eq!(Ipv4LogicalType.name(), "ipv4");
        assert_eq!(Ipv6LogicalType.name(), "ipv6");
    }

    #[test]
    fn uri_from_literal_accepts_http() {
        let t = UriLogicalType;
        match t.from_literal("https://example.com").unwrap() {
            ScalarValue::Utf8(Some(s)) => assert_eq!(s, "https://example.com"),
            other => panic!("expected Utf8, got {other:?}"),
        }
    }

    #[test]
    fn uri_from_literal_rejects_no_scheme() {
        let t = UriLogicalType;
        assert!(t.from_literal("not a uri").is_err());
    }

    #[test]
    fn geo_point_from_literal_parses_wkt() {
        let t = GeoPointLogicalType;
        match t.from_literal("POINT(1.5 2.5)").unwrap() {
            ScalarValue::Utf8(Some(s)) => assert_eq!(s, "POINT(1.5 2.5)"),
            other => panic!("expected Utf8, got {other:?}"),
        }
    }

    #[test]
    fn geo_point_from_literal_rejects_garbage() {
        let t = GeoPointLogicalType;
        assert!(t.from_literal("LINESTRING(1 2, 3 4)").is_err());
        assert!(t.from_literal("POINT(only-one)").is_err());
    }

    #[test]
    fn type_names_are_stable() {
        assert_eq!(UriLogicalType.name(), "uri");
        assert_eq!(GeoPointLogicalType.name(), "geo.point");
    }
}
