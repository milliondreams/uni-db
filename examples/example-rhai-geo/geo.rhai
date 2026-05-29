// Example Rhai plugin — proposal §15.3.
//
// Exports a single scalar function `haversine(lat1, lon1, lat2, lon2)`
// computing the great-circle distance between two points in kilometres.
// Uses `asin` instead of `atan2` because the latter isn't in Rhai's
// BasicMathPackage.

fn uni_manifest() {
    #{
        id: "ai.dragonscale.geo",
        version: "0.3.1",
        determinism: "pure",
        scalar_fns: [
            #{ name: "haversine",
               args: ["float", "float", "float", "float"],
               returns: "float" },
        ],
    }
}

const R = 6371.0;  // Earth radius in km

fn haversine(lat1, lon1, lat2, lon2) {
    let rlat1 = lat1.to_radians();
    let rlat2 = lat2.to_radians();
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin() ** 2
          + rlat1.cos() * rlat2.cos() * (dlon / 2.0).sin() ** 2;
    global::R * 2.0 * a.sqrt().asin()
}
