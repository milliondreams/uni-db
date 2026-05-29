//! Example Component Model plugin — `ai.example.geo.haversine`.
//!
//! Mirrors `example-extism-geo` but targets the Component Model
//! wire (typed WIT contracts via `wit-bindgen`'s guest macro). The
//! same Arrow-IPC payload format crosses both ABIs, so the
//! cross-ABI parity test can byte-compare outputs.

use std::sync::Arc;

use arrow::array::{Float64Array, RecordBatch};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};

wit_bindgen::generate!({
    world: "scalar-plugin",
    path: "wit",
});

const EARTH_RADIUS_KM: f64 = 6371.0;

struct GeoPlugin;

impl Guest for GeoPlugin {
    fn manifest() -> String {
        r#"{
            "id": "ai.example.geo",
            "version": "0.1.0",
            "capabilities": [],
            "determinism": "pure",
            "description": "Great-circle distance via the haversine formula (CM)."
        }"#
            .to_owned()
    }

    fn register() -> String {
        r#"{
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
        }"#
            .to_owned()
    }

    fn invoke_scalar(qname: String, ipc_bytes: Vec<u8>) -> Result<Vec<u8>, FnError> {
        if qname != "ai.example.geo.haversine" {
            return Err(FnError {
                code: 1,
                message: format!("unknown qname: {qname}"),
                retryable: false,
            });
        }
        compute_and_encode(&ipc_bytes).map_err(|e| FnError {
            code: 2,
            message: e,
            retryable: false,
        })
    }
}

fn compute_and_encode(input: &[u8]) -> Result<Vec<u8>, String> {
    let batch = decode_input(input)?;
    let out_batch = compute_haversine_batch(&batch)?;
    encode_output(&out_batch)
}

fn decode_input(bytes: &[u8]) -> Result<RecordBatch, String> {
    let reader = StreamReader::try_new(bytes, None).map_err(|e| format!("reader: {e}"))?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    for r in reader {
        batches.push(r.map_err(|e| format!("read: {e}"))?);
    }
    batches
        .into_iter()
        .next()
        .ok_or_else(|| "empty IPC stream".to_owned())
}

fn compute_haversine_batch(batch: &RecordBatch) -> Result<RecordBatch, String> {
    if batch.num_columns() != 4 {
        return Err(format!("expected 4 cols; got {}", batch.num_columns()));
    }
    let cols: Vec<&Float64Array> = (0..4)
        .map(|i| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| format!("col {i} not Float64"))
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
    let arr = Arc::new(Float64Array::from(out));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "distance_km",
        DataType::Float64,
        true,
    )]));
    RecordBatch::try_new(schema, vec![arr]).map_err(|e| format!("batch: {e}"))
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

fn haversine_km(lat1_deg: f64, lon1_deg: f64, lat2_deg: f64, lon2_deg: f64) -> f64 {
    let lat1 = lat1_deg.to_radians();
    let lat2 = lat2_deg.to_radians();
    let dlat = (lat2_deg - lat1_deg).to_radians();
    let dlon = (lon2_deg - lon1_deg).to_radians();
    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    EARTH_RADIUS_KM * c
}

export!(GeoPlugin);
