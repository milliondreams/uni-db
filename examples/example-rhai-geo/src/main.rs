//! Example: load `geo.rhai` into an in-process Uni and call its
//! `haversine` scalar against a small fixture of well-known city pairs.

use std::sync::Arc;

use arrow_array::{Array, Float64Array};
use datafusion::logical_expr::ColumnarValue;
use uni_db::Uni;
use uni_plugin::{Capability, CapabilitySet, QName};
use uni_plugin_rhai::RhaiLoader;

const SCRIPT: &str = include_str!("../geo.rhai");

const CITIES: &[((&str, f64, f64), (&str, f64, f64), f64)] = &[
    // (origin, destination, expected_km ± a few km).
    (
        ("New York", 40.7128, -74.0060),
        ("San Francisco", 37.7749, -122.4194),
        4_129.0,
    ),
    (
        ("London", 51.5074, -0.1278),
        ("Paris", 48.8566, 2.3522),
        344.0,
    ),
    (
        ("Tokyo", 35.6762, 139.6503),
        ("Sydney", -33.8688, 151.2093),
        7_823.0,
    ),
];

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let loader = RhaiLoader::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let outcome = db.load_rhai_plugin(&loader, SCRIPT, &caps)?;
    println!(
        "loaded `{}` v{} ({} scalar fn(s))",
        outcome.plugin_id.as_str(),
        outcome.version,
        outcome.scalars_registered.len()
    );

    let qn = QName::new("ai.dragonscale.geo", "haversine");
    let entry = db
        .plugin_registry()
        .scalar_fn(&qn)
        .ok_or_else(|| anyhow::anyhow!("haversine not registered"))?;

    let lat1: Vec<f64> = CITIES.iter().map(|(a, _, _)| a.1).collect();
    let lon1: Vec<f64> = CITIES.iter().map(|(a, _, _)| a.2).collect();
    let lat2: Vec<f64> = CITIES.iter().map(|(_, b, _)| b.1).collect();
    let lon2: Vec<f64> = CITIES.iter().map(|(_, b, _)| b.2).collect();
    let n = CITIES.len();

    let args = vec![
        ColumnarValue::Array(Arc::new(Float64Array::from(lat1))),
        ColumnarValue::Array(Arc::new(Float64Array::from(lon1))),
        ColumnarValue::Array(Arc::new(Float64Array::from(lat2))),
        ColumnarValue::Array(Arc::new(Float64Array::from(lon2))),
    ];
    let out = entry.function.invoke(&args, n)?;
    let arr = match out {
        ColumnarValue::Array(a) => a,
        _ => anyhow::bail!("expected array"),
    };
    let out = arr
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow::anyhow!("expected Float64"))?;

    println!();
    println!(
        "  {:<14}  →  {:<14}   km     (expected)",
        "from", "to"
    );
    println!("  {}", "-".repeat(58));
    for (i, ((from, _, _), (to, _, _), expected)) in CITIES.iter().enumerate() {
        let got = out.value(i);
        println!(
            "  {from:<14}  →  {to:<14}  {got:>7.1}  ({expected:>7.1})"
        );
    }
    Ok(())
}
