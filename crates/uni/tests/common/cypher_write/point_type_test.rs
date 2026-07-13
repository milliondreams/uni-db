// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end round-trip for the first-party geospatial `Point` type.
//!
//! A property declared as `DataType::Point` is now persisted as an Arrow struct
//! and decoded back to the `Value::Map` shape `point(...)` produces. Before this
//! wiring the typed column errored on flush and decoded to `Null`.

use anyhow::Result;
use uni_common::DataType;
use uni_common::core::schema::PointType;
use uni_db::Uni;

/// A declared geographic `Point` property survives write → flush → read.
#[tokio::test]
async fn geopoint_property_round_trips_through_flush() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Place")
        .property("loc", DataType::Point(PointType::Geographic))
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(
        "CREATE (:Place {name: 'London', \
         loc: point({latitude: 51.5, longitude: -0.12})})",
    )
    .await?;
    tx.commit().await?;

    // Flush so the read is served from the persisted Arrow struct column, not L0.
    db.flush().await?;

    let rows = session
        .query_with(
            "MATCH (p:Place) \
             RETURN p.loc.latitude AS lat, p.loc.longitude AS lon, p.loc.crs AS crs",
        )
        .fetch_all()
        .await?;

    assert_eq!(rows.iter().count(), 1, "exactly one Place");
    let row = rows.iter().next().expect("one Place");
    let lat: f64 = row.get("lat")?;
    let lon: f64 = row.get("lon")?;
    let crs: String = row.get("crs")?;
    assert!((lat - 51.5).abs() < 1e-9, "latitude round-trips: {lat}");
    assert!((lon - -0.12).abs() < 1e-9, "longitude round-trips: {lon}");
    assert_eq!(crs, "WGS84", "crs preserved");
    Ok(())
}

/// `distance()` between two persisted geographic points computes meters.
#[tokio::test]
async fn geopoint_distance_over_persisted_points() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("City")
        .property("loc", DataType::Point(PointType::Geographic))
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(
        "CREATE (:City {name: 'A', loc: point({latitude: 0.0, longitude: 0.0})}), \
         (:City {name: 'B', loc: point({latitude: 0.0, longitude: 1.0})})",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    let rows = session
        .query_with(
            "MATCH (a:City {name: 'A'}), (b:City {name: 'B'}) \
             RETURN distance(a.loc, b.loc) AS d",
        )
        .fetch_all()
        .await?;
    assert_eq!(rows.iter().count(), 1);
    let d: f64 = rows.iter().next().expect("one row").get("d")?;
    // ~111 km per degree of longitude at the equator.
    assert!(
        (d - 111_195.0).abs() < 2_000.0,
        "one degree at equator ≈ 111 km, got {d} m"
    );
    Ok(())
}
