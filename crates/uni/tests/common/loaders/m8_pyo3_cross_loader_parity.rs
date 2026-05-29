#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Proposal §19 acceptance criterion #4 — PyO3 joins the cross-loader
//! parity matrix.
//!
//! Python's `math.*` functions delegate to the host libm (CPython's
//! `mathmodule.c` calls the C library trig functions directly). On
//! glibc / musl this is the same libm path that Rust uses; the result
//! is byte-identical for non-singular inputs. We require ≤ 4 ULPs to
//! tolerate the off-by-one rounding mode that some libms produce
//! through the asin-form haversine.

#![cfg(feature = "pyo3-plugins")]

use std::sync::Arc;

use arrow_array::Float64Array;
use datafusion::logical_expr::ColumnarValue;
use pyo3::Python;
use uni_db::Uni;
use uni_plugin::{Capability, CapabilitySet, QName};
use uni_plugin_pyo3::PythonPluginLoader;

const PYTHON_SRC: &str = r#"
import math

db.set_plugin_id("ai.example.geo")
db.set_version("0.3.1")

R = 6371.0

@db.scalar_fn("haversine", args=["float","float","float","float"], returns="float", determinism="pure")
def haversine(lat1, lon1, lat2, lon2):
    rlat1 = math.radians(lat1)
    rlat2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2.0) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2.0) ** 2
    return R * 2.0 * math.asin(math.sqrt(a))
"#;

fn native_haversine(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r = 6371.0_f64;
    let rlat1 = lat1.to_radians();
    let rlat2 = lat2.to_radians();
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2) + rlat1.cos() * rlat2.cos() * (dlon / 2.0).sin().powi(2);
    r * 2.0 * a.sqrt().asin()
}

fn ulp_distance(a: f64, b: f64) -> u64 {
    if a == b {
        return 0;
    }
    let ai = a.to_bits() as i64;
    let bi = b.to_bits() as i64;
    (ai - bi).unsigned_abs()
}

#[tokio::test]
async fn pyo3_haversine_matches_native_within_4_ulp() -> anyhow::Result<()> {
    Python::initialize();
    let uni = Uni::in_memory().build().await?;
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let loader = PythonPluginLoader::with_default_plugin_id("ai.example.geo");

    let outcome = Python::attach(|py| {
        uni.load_python_plugin(py, &loader, PYTHON_SRC, "ai.example.geo", &caps)
            .expect("load_python_plugin")
    });
    assert_eq!(outcome.plugin_id.as_str(), "ai.example.geo");

    let qname = QName::new("ai.example.geo", "haversine");
    let entry = uni.plugin_registry().scalar_fn(&qname).expect("registered");

    // Same 5-row matrix as the CM/Extism + Rhai parity tests.
    let lat1 = vec![48.8566, 0.0, 0.0, 90.0, 35.6762];
    let lon1 = vec![2.3522, 0.0, 0.0, 0.0, 139.6503];
    let lat2 = vec![51.5074, 0.0, 0.0, -90.0, 40.7128];
    let lon2 = vec![-0.1278, 0.0, 180.0, 0.0, -74.0060];
    let n = lat1.len();

    let args = vec![
        ColumnarValue::Array(Arc::new(Float64Array::from(lat1.clone()))),
        ColumnarValue::Array(Arc::new(Float64Array::from(lon1.clone()))),
        ColumnarValue::Array(Arc::new(Float64Array::from(lat2.clone()))),
        ColumnarValue::Array(Arc::new(Float64Array::from(lon2.clone()))),
    ];
    let out = entry.function.invoke(&args, n)?;
    let arr = match out {
        ColumnarValue::Array(a) => a,
        _ => panic!("expected Array"),
    };
    let f = arr.as_any().downcast_ref::<Float64Array>().unwrap();

    for i in 0..n {
        let py_val = f.value(i);
        let native = native_haversine(lat1[i], lon1[i], lat2[i], lon2[i]);
        // Antipodal singularity (row 3) — asin(1.0) is exactly π/2 in
        // libm but the chain through the user's expression can drift
        // slightly. Use generous absolute tolerance there.
        if i == 3 {
            assert!(
                (py_val - native).abs() < 1e-3,
                "row {i}: antipodal: py={py_val} native={native}"
            );
            continue;
        }
        let ulps = ulp_distance(py_val, native);
        assert!(
            ulps <= 4,
            "row {i}: py={py_val} native={native} ulps={ulps} > 4"
        );
    }
    Ok(())
}

#[tokio::test]
async fn pyo3_haversine_vectorized_matches_native_within_4_ulp() -> anyhow::Result<()> {
    Python::initialize();
    let uni = Uni::in_memory().build().await?;
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let loader = PythonPluginLoader::with_default_plugin_id("ai.example.geo_v");

    let module_src = r#"
import pyarrow as pa
import pyarrow.compute as pc

db.set_plugin_id("ai.example.geo_v")
db.set_version("0.3.1")

@db.scalar_fn("haversine_v", args=["float","float","float","float"], returns="float", vectorized=True, determinism="pure")
def haversine_v(lat1, lon1, lat2, lon2):
    # pyarrow.compute kernels run in C against Arrow buffers.
    deg2rad = 0.017453292519943295
    rlat1 = pc.multiply(lat1, deg2rad)
    rlat2 = pc.multiply(lat2, deg2rad)
    dlat = pc.multiply(pc.subtract(lat2, lat1), deg2rad)
    dlon = pc.multiply(pc.subtract(lon2, lon1), deg2rad)
    s1 = pc.sin(pc.divide(dlat, 2.0))
    s2 = pc.sin(pc.divide(dlon, 2.0))
    c1 = pc.cos(rlat1)
    c2 = pc.cos(rlat2)
    a = pc.add(pc.power(s1, 2), pc.multiply(c1, pc.multiply(c2, pc.power(s2, 2))))
    return pc.multiply(6371.0 * 2.0, pc.asin(pc.sqrt(a)))
"#;

    let outcome = Python::attach(|py| {
        uni.load_python_plugin(py, &loader, module_src, "ai.example.geo_v", &caps)
            .expect("load_python_plugin")
    });
    assert_eq!(outcome.scalars_registered.len(), 1);

    let qname = QName::new("ai.example.geo_v", "haversine_v");
    let entry = uni.plugin_registry().scalar_fn(&qname).expect("registered");

    let lat1 = vec![48.8566, 0.0, 0.0, 90.0, 35.6762];
    let lon1 = vec![2.3522, 0.0, 0.0, 0.0, 139.6503];
    let lat2 = vec![51.5074, 0.0, 0.0, -90.0, 40.7128];
    let lon2 = vec![-0.1278, 0.0, 180.0, 0.0, -74.0060];
    let n = lat1.len();

    let args = vec![
        ColumnarValue::Array(Arc::new(Float64Array::from(lat1.clone()))),
        ColumnarValue::Array(Arc::new(Float64Array::from(lon1.clone()))),
        ColumnarValue::Array(Arc::new(Float64Array::from(lat2.clone()))),
        ColumnarValue::Array(Arc::new(Float64Array::from(lon2.clone()))),
    ];
    let out = entry.function.invoke(&args, n)?;
    let arr = match out {
        ColumnarValue::Array(a) => a,
        _ => panic!("expected Array"),
    };
    let f = arr.as_any().downcast_ref::<Float64Array>().unwrap();

    for i in 0..n {
        let py_val = f.value(i);
        let native = native_haversine(lat1[i], lon1[i], lat2[i], lon2[i]);
        if i == 3 {
            assert!(
                (py_val - native).abs() < 1e-3,
                "row {i}: antipodal vec: py={py_val} native={native}"
            );
            continue;
        }
        let ulps = ulp_distance(py_val, native);
        assert!(
            ulps <= 4,
            "row {i}: vec py={py_val} native={native} ulps={ulps} > 4"
        );
    }
    Ok(())
}
