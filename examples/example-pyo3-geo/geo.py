"""Example PyO3 plugin for uni-db — great-circle distance via haversine.

This module is loaded into an in-process Uni by `Uni::load_python_plugin`.
The `db` global is the host's decorator sink: every `@db.scalar_fn(...)`
records a Python callable into the plugin's manifest builder, and the
loader drains the builder into the host's `PluginRegistry`.
"""

import math

db.set_plugin_id("ai.dragonscale.geo")
db.set_version("0.3.1")

R = 6371.0  # Earth radius in km


@db.scalar_fn(
    "haversine",
    args=["float", "float", "float", "float"],
    returns="float",
    determinism="pure",
)
def haversine(lat1, lon1, lat2, lon2):
    """Great-circle distance in km using the asin-form haversine."""
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None
    rlat1 = math.radians(lat1)
    rlat2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2.0) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2.0) ** 2
    return R * 2.0 * math.asin(math.sqrt(a))
