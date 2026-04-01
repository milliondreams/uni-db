"""
Transitive risk propagation in a supply chain network using Locy + MNOR.

Models a multi-tier supply chain where component failures propagate risk
through DEPENDS_ON relationships. MNOR (monotonic noisy-OR) combines
independent failure probabilities at each node so that multiple upstream
risk sources are aggregated correctly: P(any cause) = 1 - prod(1 - p_i).

The recursive rule propagates risk transitively: if Tier-2 supplier S has
a failure probability, every downstream component that transitively depends
on S inherits a combined risk.
"""

import os
import shutil
import tempfile

import uni_db

# ---------------------------------------------------------------------------
# 1. Open an ephemeral database
# ---------------------------------------------------------------------------
db_path = os.path.join(tempfile.gettempdir(), "supply_risk_db")
if os.path.exists(db_path):
    shutil.rmtree(db_path)

db = uni_db.Uni.open(db_path)
session = db.session()
print(f"Database opened at {db_path}")

# ---------------------------------------------------------------------------
# 2. Define schema
# ---------------------------------------------------------------------------
(
    db.schema()
    # Components / sub-assemblies in the supply chain
    .label("Component")
    .property("name", "string")
    .property("tier", "int64")               # tier 0 = final product
    .property_nullable("base_risk", "float64")  # intrinsic failure probability
    .done()
    # Directed dependency: (downstream)-[:DEPENDS_ON {risk: ...}]->(upstream)
    .edge_type("DEPENDS_ON", ["Component"], ["Component"])
    .property("risk", "float64")  # probability that upstream failure causes downstream failure
    .done()
    .apply()
)
print("Schema created")

# ---------------------------------------------------------------------------
# 3. Seed data via Cypher  (a realistic 3-tier supply chain)
#
#   Tier 2 (raw materials / sub-suppliers):
#     ChipFab      base_risk=0.05   (semiconductor fab)
#     RareMinerals  base_risk=0.08   (rare earth supplier)
#     Resin         base_risk=0.03   (plastics supplier)
#
#   Tier 1 (module assemblers):
#     PCB_Asm   depends on ChipFab (0.7) and RareMinerals (0.4)
#     Casing    depends on Resin (0.6)
#     Battery   depends on RareMinerals (0.5)
#
#   Tier 0 (final products):
#     Smartphone  depends on PCB_Asm (0.9), Casing (0.5), Battery (0.8)
#     Laptop      depends on PCB_Asm (0.85), Battery (0.7)
# ---------------------------------------------------------------------------
tx = session.tx()
with tx.bulk_writer().build() as bw:
    # Tier 2 — raw material suppliers with intrinsic failure risk
    tier2 = bw.insert_vertices(
        "Component",
        [
            {"name": "ChipFab",      "tier": 2, "base_risk": 0.05},
            {"name": "RareMinerals", "tier": 2, "base_risk": 0.08},
            {"name": "Resin",        "tier": 2, "base_risk": 0.03},
        ],
    )
    chip_fab, rare_minerals, resin = tier2

    # Tier 1 — module assemblers (no intrinsic risk; risk comes from upstream)
    tier1 = bw.insert_vertices(
        "Component",
        [
            {"name": "PCB_Asm",  "tier": 1},
            {"name": "Casing",   "tier": 1},
            {"name": "Battery",  "tier": 1},
        ],
    )
    pcb_asm, casing, battery = tier1

    # Tier 0 — final products
    tier0 = bw.insert_vertices(
        "Component",
        [
            {"name": "Smartphone", "tier": 0},
            {"name": "Laptop",     "tier": 0},
        ],
    )
    smartphone, laptop = tier0

    # Tier 1 -> Tier 2 dependencies
    bw.insert_edges(
        "DEPENDS_ON",
        [
            (pcb_asm, chip_fab,      {"risk": 0.7}),
            (pcb_asm, rare_minerals, {"risk": 0.4}),
            (casing,  resin,         {"risk": 0.6}),
            (battery, rare_minerals, {"risk": 0.5}),
        ],
    )

    # Tier 0 -> Tier 1 dependencies
    bw.insert_edges(
        "DEPENDS_ON",
        [
            (smartphone, pcb_asm, {"risk": 0.9}),
            (smartphone, casing,  {"risk": 0.5}),
            (smartphone, battery, {"risk": 0.8}),
            (laptop,     pcb_asm, {"risk": 0.85}),
            (laptop,     battery, {"risk": 0.7}),
        ],
    )

    bw.commit()
tx.commit()
print("Supply chain data ingested (3 tiers, 8 components, 9 edges)")

# ---------------------------------------------------------------------------
# 4. Locy program: transitive risk propagation with MNOR
#
#   Rule "supply_risk" has two clauses (same rule name = union/recursion):
#
#   Base clause (non-recursive):
#     For every direct dependency edge (a)-[:DEPENDS_ON {risk: r}]->(b),
#     emit supply_risk(a, b) with combined probability via MNOR(e.risk).
#     MNOR computes: P = 1 - prod(1 - p_i) for independent causes,
#     handling the case where multiple parallel edges exist between
#     the same pair.
#
#   Recursive clause:
#     If (a)-[:DEPENDS_ON {risk: r}]->(mid) and mid IS supply_risk TO b,
#     then a also has transitive risk to b.  MNOR combines all independent
#     paths (direct + transitive) into a single probability.
#
#   After evaluation, derived facts contain VID-keyed rows with the
#   computed `prob` values.  We resolve VIDs to names via Cypher.
# ---------------------------------------------------------------------------
LOCY_PROGRAM = """
CREATE RULE supply_risk AS
  MATCH (a:Component)-[e:DEPENDS_ON]->(b:Component)
  FOLD prob = MNOR(e.risk)
  YIELD KEY a, KEY b, prob

CREATE RULE supply_risk AS
  MATCH (a:Component)-[e:DEPENDS_ON]->(mid:Component)
  WHERE mid IS supply_risk TO b
  FOLD prob = MNOR(e.risk)
  YIELD KEY a, KEY b, prob
"""

print("\n--- Evaluating Locy program ---")
result = session.locy(LOCY_PROGRAM)
print(f"Strata evaluated : {result.stats.strata_evaluated}")
print(f"Fixpoint iters   : {result.stats.total_iterations}")
print(f"Evaluation time  : {result.stats.evaluation_time_secs:.4f}s")

# ---------------------------------------------------------------------------
# 5. Extract derived facts and resolve names from KEY node objects
#
#    Each derived fact row has KEY columns as full Node objects
#    (with .properties dict) and value columns as scalars.
# ---------------------------------------------------------------------------
derived = result.derived.get("supply_risk", [])
print(f"\n--- Derived supply_risk relation ({len(derived)} facts) ---")

# Build a list of resolved rows from the Node KEY objects
resolved_rows: list[dict[str, object]] = []
for fact in derived:
    a_node = fact["a"]
    b_node = fact["b"]
    prob = fact["prob"]
    # Node objects have a .properties dict with the graph properties
    a_props = a_node.properties if hasattr(a_node, "properties") else a_node
    b_props = b_node.properties if hasattr(b_node, "properties") else b_node
    resolved_rows.append({
        "component": a_props["name"],
        "upstream": b_props["name"],
        "risk": prob,
        "component_tier": a_props["tier"],
    })

# Sort by risk descending
resolved_rows.sort(key=lambda r: r["risk"], reverse=True)

print(f"\n{'Component':<14} {'Upstream Source':<16} {'Risk':>8}")
print("-" * 40)
for row in resolved_rows:
    print(f"{row['component']:<14} {row['upstream']:<16} {row['risk']:>8.4f}")

# ---------------------------------------------------------------------------
# 7. Highlight highest-risk final products (tier 0)
# ---------------------------------------------------------------------------
print("\n--- Final product risk summary ---")
product_risks: dict[str, list[tuple[str, float]]] = {}
for row in resolved_rows:
    if row["component_tier"] == 0:
        product_risks.setdefault(row["component"], []).append(
            (row["upstream"], row["risk"])
        )

for product in sorted(product_risks.keys()):
    risks = product_risks[product]
    risks.sort(key=lambda x: x[1], reverse=True)
    print(f"\n  {product}:")
    for upstream, risk in risks:
        bar = "#" * int(risk * 40)
        print(f"    -> {upstream:<16} risk={risk:.4f}  {bar}")

# ---------------------------------------------------------------------------
# 8. Validate results
# ---------------------------------------------------------------------------
print("\n--- Validation ---")

# There must be derived facts for all transitive pairs
assert len(derived) > 0, "Expected derived supply_risk facts"
print(f"  Total transitive risk pairs: {len(derived)}")

# Smartphone should have risk to ChipFab (transitive via PCB_Asm)
sm_chipfab = [
    r for r in resolved_rows
    if r["component"] == "Smartphone" and r["upstream"] == "ChipFab"
]
assert len(sm_chipfab) == 1, f"Expected Smartphone->ChipFab risk, got {sm_chipfab}"
print(f"  Smartphone -> ChipFab transitive risk: {sm_chipfab[0]['risk']:.4f}")

# Laptop should have risk to RareMinerals (transitive via PCB_Asm and Battery)
lp_rare = [
    r for r in resolved_rows
    if r["component"] == "Laptop" and r["upstream"] == "RareMinerals"
]
assert len(lp_rare) == 1, f"Expected Laptop->RareMinerals risk, got {lp_rare}"
print(f"  Laptop -> RareMinerals transitive risk: {lp_rare[0]['risk']:.4f}")

# All risk values should be in (0, 1]
for row in resolved_rows:
    assert 0 < row["risk"] <= 1.0, f"Risk out of range: {row}"
print(f"  All {len(resolved_rows)} risk values in (0, 1] range")

print("\nAll validations passed.")

# ---------------------------------------------------------------------------
# 9. Cleanup
# ---------------------------------------------------------------------------
del session
del db
shutil.rmtree(db_path, ignore_errors=True)
print(f"\nCleaned up {db_path}")
