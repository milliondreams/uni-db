//! Example Extism plugin — `geo.haversine` great-circle distance.
//!
//! Demonstrates the full M6a wire contract end-to-end:
//!
//! - `manifest` export — returns the canonical JSON
//!   `ExtismPluginManifest` describing this plugin.
//! - `register` export — returns the canonical JSON
//!   `RegistrationManifest` declaring `geo.haversine` as a scalar
//!   fn taking four `float64`s and returning one `float64`.
//! - `invoke_geo.haversine` export — Arrow IPC in, Arrow IPC out.
//!   Reads a `RecordBatch` with four `Float64` columns, returns a
//!   `RecordBatch` with one `Float64` column.
//!
//! Build:
//!     cargo build --target wasm32-unknown-unknown --release
//!
//! The resulting
//! `target/wasm32-unknown-unknown/release/example_extism_geo.wasm`
//! is loadable via `Uni::load_wasm_extism` (feature `extism-plugins`).

use std::sync::Arc;

use arrow::array::{Float64Array, RecordBatch};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use extism_pdk::*;

const EARTH_RADIUS_KM: f64 = 6371.0;

#[plugin_fn]
pub fn manifest(_: ()) -> FnResult<String> {
    Ok(serde_json::json!({
        "id": "ai.example.geo",
        "version": "0.1.0",
        "abi-extism": "^1",
        "capabilities": [],
        "determinism": "pure",
        "description": "Great-circle distance via the haversine formula."
    })
    .to_string())
}

#[plugin_fn]
pub fn register(_: ()) -> FnResult<String> {
    Ok(serde_json::json!({
        "entries": [{
            "kind": "scalar",
            "qname": "ai.example.geo.haversine",
            "signature": {
                "args": [
                    {"kind": "primitive", "arrow": "float64"},
                    {"kind": "primitive", "arrow": "float64"},
                    {"kind": "primitive", "arrow": "float64"},
                    {"kind": "primitive", "arrow": "float64"}
                ],
                "returns": {"kind": "primitive", "arrow": "float64"},
                "volatility": "immutable",
                "null_handling": "propagate"
            }
        }]
    })
    .to_string())
}

/// Compute great-circle distance per the haversine formula.
fn haversine_km(lat1_deg: f64, lon1_deg: f64, lat2_deg: f64, lon2_deg: f64) -> f64 {
    let lat1 = lat1_deg.to_radians();
    let lat2 = lat2_deg.to_radians();
    let dlat = (lat2_deg - lat1_deg).to_radians();
    let dlon = (lon2_deg - lon1_deg).to_radians();
    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    EARTH_RADIUS_KM * c
}

/// Plugin-side `invoke_ai_example_geo_haversine` export.
///
/// The host computes the export name as
/// `invoke_<qname-with-dots-replaced-by-underscores>` per
/// `uni-plugin-extism/src/adapter.rs::scalar_export_name`. For qname
/// `ai.example.geo.haversine` that produces the symbol
/// `invoke_ai_example_geo_haversine`.
#[plugin_fn]
pub fn invoke_ai_example_geo_haversine(input: Vec<u8>) -> FnResult<Vec<u8>> {
    let batch = decode_input(&input).map_err(|e| WithReturnCode::new(Error::msg(e), 2))?;
    let out_batch = compute_haversine_batch(&batch)
        .map_err(|e| WithReturnCode::new(Error::msg(e), 2))?;
    let out_bytes =
        encode_output(&out_batch).map_err(|e| WithReturnCode::new(Error::msg(e), 2))?;
    Ok(out_bytes)
}

fn decode_input(bytes: &[u8]) -> Result<RecordBatch, String> {
    let reader =
        StreamReader::try_new(bytes, None).map_err(|e| format!("reader setup: {e}"))?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    for r in reader {
        batches.push(r.map_err(|e| format!("read batch: {e}"))?);
    }
    batches
        .into_iter()
        .next()
        .ok_or_else(|| "empty IPC stream".to_owned())
}

fn compute_haversine_batch(batch: &RecordBatch) -> Result<RecordBatch, String> {
    if batch.num_columns() != 4 {
        return Err(format!("expected 4 columns; got {}", batch.num_columns()));
    }
    let cols: Vec<&Float64Array> = (0..4)
        .map(|i| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| format!("column {i} is not Float64"))
        })
        .collect::<Result<_, _>>()?;

    let rows = batch.num_rows();
    let mut out: Vec<f64> = Vec::with_capacity(rows);
    for r in 0..rows {
        out.push(haversine_km(
            cols[0].value(r),
            cols[1].value(r),
            cols[2].value(r),
            cols[3].value(r),
        ));
    }
    let out_arr = Arc::new(Float64Array::from(out));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "distance_km",
        DataType::Float64,
        true,
    )]));
    RecordBatch::try_new(schema, vec![out_arr]).map_err(|e| format!("RecordBatch: {e}"))
}

fn encode_output(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    let schema = batch.schema();
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    {
        let mut w = StreamWriter::try_new(&mut buf, schema.as_ref())
            .map_err(|e| format!("writer: {e}"))?;
        w.write(batch).map_err(|e| format!("write: {e}"))?;
        w.finish().map_err(|e| format!("finish: {e}"))?;
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn haversine_paris_to_london_matches_known_value() {
        let d = haversine_km(48.8566, 2.3522, 51.5074, -0.1278);
        assert!((d - 343.557).abs() < 0.01, "got {d}");
    }

    #[test]
    fn haversine_identical_points_is_zero() {
        let d = haversine_km(0.0, 0.0, 0.0, 0.0);
        assert!(d.abs() < f64::EPSILON);
    }

    #[test]
    fn haversine_antipodes_is_half_circumference() {
        let d = haversine_km(0.0, 0.0, 0.0, 180.0);
        let expected = std::f64::consts::PI * EARTH_RADIUS_KM;
        assert!((d - expected).abs() < 1.0, "got {d}, expected ~{expected}");
    }
}
