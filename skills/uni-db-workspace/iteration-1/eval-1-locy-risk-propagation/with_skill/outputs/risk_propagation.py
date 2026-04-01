"""
Transitive Risk Propagation in a Supply Chain Network
=====================================================

Models a supply chain where:
  - Suppliers have risk signals (e.g., geopolitical instability, financial distress).
  - Suppliers supply raw materials to manufacturers.
  - Manufacturers assemble products for distributors.
  - Distributors deliver to retailers.

Risk propagates transitively through the chain using MNOR (noisy-OR):
  P(risk) = 1 - prod(1 - p_i)

This means "the probability that at least one upstream risk source affects
this node." The deeper into the chain, the more paths can contribute risk.
"""

from uni_db import Uni, DataType

# ---------------------------------------------------------------------------
# 1. Database & Schema Setup
# ---------------------------------------------------------------------------

db = Uni.in_memory()

db.schema() \
    .label("Supplier") \
        .property("name", DataType.STRING()) \
    .label("Manufacturer") \
        .property("name", DataType.STRING()) \
    .label("Distributor") \
        .property("name", DataType.STRING()) \
    .label("Retailer") \
        .property("name", DataType.STRING()) \
    .label("RiskSignal") \
        .property("name", DataType.STRING()) \
        .property("probability", DataType.FLOAT64()) \
    .edge_type("HAS_SIGNAL", ["Supplier"], ["RiskSignal"]) \
    .edge_type("SUPPLIES", ["Supplier"], ["Manufacturer"]) \
        .property("dependency", DataType.FLOAT64()) \
    .edge_type("ASSEMBLES_FOR", ["Manufacturer"], ["Distributor"]) \
        .property("dependency", DataType.FLOAT64()) \
    .edge_type("DELIVERS_TO", ["Distributor"], ["Retailer"]) \
        .property("dependency", DataType.FLOAT64()) \
    .apply()

# ---------------------------------------------------------------------------
# 2. Seed Data via Cypher
# ---------------------------------------------------------------------------

session = db.session()

with session.tx() as tx:
    # --- Suppliers ---
    tx.execute("CREATE (:Supplier {name: 'ChipCo'})")
    tx.execute("CREATE (:Supplier {name: 'SteelWorks'})")
    tx.execute("CREATE (:Supplier {name: 'RareMinerals'})")

    # --- Risk signals attached to suppliers ---
    # ChipCo: geopolitical tension (0.4) + shipping disruption (0.3)
    tx.execute("""
        CREATE (:RiskSignal {name: 'Geopolitical Tension', probability: 0.4})
    """)
    tx.execute("""
        CREATE (:RiskSignal {name: 'Shipping Disruption', probability: 0.3})
    """)
    tx.execute("""
        MATCH (s:Supplier {name: 'ChipCo'}), (r:RiskSignal {name: 'Geopolitical Tension'})
        CREATE (s)-[:HAS_SIGNAL]->(r)
    """)
    tx.execute("""
        MATCH (s:Supplier {name: 'ChipCo'}), (r:RiskSignal {name: 'Shipping Disruption'})
        CREATE (s)-[:HAS_SIGNAL]->(r)
    """)

    # SteelWorks: financial distress (0.5)
    tx.execute("""
        CREATE (:RiskSignal {name: 'Financial Distress', probability: 0.5})
    """)
    tx.execute("""
        MATCH (s:Supplier {name: 'SteelWorks'}), (r:RiskSignal {name: 'Financial Distress'})
        CREATE (s)-[:HAS_SIGNAL]->(r)
    """)

    # RareMinerals: export ban (0.6) + mine collapse (0.2)
    tx.execute("""
        CREATE (:RiskSignal {name: 'Export Ban', probability: 0.6})
    """)
    tx.execute("""
        CREATE (:RiskSignal {name: 'Mine Collapse', probability: 0.2})
    """)
    tx.execute("""
        MATCH (s:Supplier {name: 'RareMinerals'}), (r:RiskSignal {name: 'Export Ban'})
        CREATE (s)-[:HAS_SIGNAL]->(r)
    """)
    tx.execute("""
        MATCH (s:Supplier {name: 'RareMinerals'}), (r:RiskSignal {name: 'Mine Collapse'})
        CREATE (s)-[:HAS_SIGNAL]->(r)
    """)

    # --- Manufacturers ---
    tx.execute("CREATE (:Manufacturer {name: 'ElectroAssembly'})")
    tx.execute("CREATE (:Manufacturer {name: 'HeavyBuild'})")

    # Supply links (with dependency weight = how much the mfr relies on this supplier)
    tx.execute("""
        MATCH (s:Supplier {name: 'ChipCo'}), (m:Manufacturer {name: 'ElectroAssembly'})
        CREATE (s)-[:SUPPLIES {dependency: 0.9}]->(m)
    """)
    tx.execute("""
        MATCH (s:Supplier {name: 'RareMinerals'}), (m:Manufacturer {name: 'ElectroAssembly'})
        CREATE (s)-[:SUPPLIES {dependency: 0.7}]->(m)
    """)
    tx.execute("""
        MATCH (s:Supplier {name: 'SteelWorks'}), (m:Manufacturer {name: 'HeavyBuild'})
        CREATE (s)-[:SUPPLIES {dependency: 0.8}]->(m)
    """)
    tx.execute("""
        MATCH (s:Supplier {name: 'RareMinerals'}), (m:Manufacturer {name: 'HeavyBuild'})
        CREATE (s)-[:SUPPLIES {dependency: 0.5}]->(m)
    """)

    # --- Distributors ---
    tx.execute("CREATE (:Distributor {name: 'GlobalLogistics'})")
    tx.execute("CREATE (:Distributor {name: 'RegionalShip'})")

    tx.execute("""
        MATCH (m:Manufacturer {name: 'ElectroAssembly'}), (d:Distributor {name: 'GlobalLogistics'})
        CREATE (m)-[:ASSEMBLES_FOR {dependency: 0.85}]->(d)
    """)
    tx.execute("""
        MATCH (m:Manufacturer {name: 'HeavyBuild'}), (d:Distributor {name: 'RegionalShip'})
        CREATE (m)-[:ASSEMBLES_FOR {dependency: 0.75}]->(d)
    """)
    tx.execute("""
        MATCH (m:Manufacturer {name: 'HeavyBuild'}), (d:Distributor {name: 'GlobalLogistics'})
        CREATE (m)-[:ASSEMBLES_FOR {dependency: 0.4}]->(d)
    """)

    # --- Retailers ---
    tx.execute("CREATE (:Retailer {name: 'MegaMart'})")
    tx.execute("CREATE (:Retailer {name: 'LocalShop'})")

    tx.execute("""
        MATCH (d:Distributor {name: 'GlobalLogistics'}), (r:Retailer {name: 'MegaMart'})
        CREATE (d)-[:DELIVERS_TO {dependency: 0.95}]->(r)
    """)
    tx.execute("""
        MATCH (d:Distributor {name: 'RegionalShip'}), (r:Retailer {name: 'LocalShop'})
        CREATE (d)-[:DELIVERS_TO {dependency: 0.9}]->(r)
    """)
    tx.execute("""
        MATCH (d:Distributor {name: 'GlobalLogistics'}), (r:Retailer {name: 'LocalShop'})
        CREATE (d)-[:DELIVERS_TO {dependency: 0.3}]->(r)
    """)

    tx.commit()

# ---------------------------------------------------------------------------
# 3. Locy Rules: Transitive Risk Propagation with MNOR
# ---------------------------------------------------------------------------
#
# Layer 1 (supplier_risk):
#   Aggregate each supplier's raw risk signals with MNOR.
#   e.g. ChipCo: 1 - (1-0.4)*(1-0.3) = 0.58
#
# Layer 2 (chain_risk) - base case:
#   For each edge in the supply chain (SUPPLIES / ASSEMBLES_FOR / DELIVERS_TO),
#   combine the upstream node's risk with the edge dependency weight.
#   Effective contribution = upstream_risk * dependency.
#   Then MNOR across all incoming edges per downstream node.
#
# Layer 2 (chain_risk) - recursive case:
#   A downstream node also inherits risk from nodes further upstream via
#   an IS reference to chain_risk, allowing transitive propagation.
#   MNOR naturally combines all contributing paths.
#
# Layer 3 (safe_node):
#   Complement -- nodes NOT exposed to chain risk get safety = 1 - risk.
#   Uses IS NOT with PROB complement semantics.

LOCY_PROGRAM = """
    -- Layer 1: Aggregate raw risk signals per supplier
    CREATE RULE supplier_risk AS
        MATCH (s:Supplier)-[:HAS_SIGNAL]->(sig:RiskSignal)
        FOLD risk = MNOR(sig.probability)
        YIELD KEY s, risk PROB

    -- Layer 2 base: Direct supply-chain links propagate risk.
    -- Risk contribution = supplier_risk * edge dependency weight.

    -- Supplier -> Manufacturer
    CREATE RULE chain_risk AS
        MATCH (s:Supplier)-[e:SUPPLIES]->(m:Manufacturer)
        WHERE s IS supplier_risk
        FOLD exposure = MNOR(risk * e.dependency)
        YIELD KEY m, exposure PROB

    -- Manufacturer -> Distributor
    CREATE RULE chain_risk AS
        MATCH (m:Manufacturer)-[e:ASSEMBLES_FOR]->(d:Distributor)
        WHERE m IS chain_risk
        FOLD exposure = MNOR(risk * e.dependency)
        YIELD KEY d, exposure PROB

    -- Distributor -> Retailer
    CREATE RULE chain_risk AS
        MATCH (d:Distributor)-[e:DELIVERS_TO]->(r:Retailer)
        WHERE d IS chain_risk
        FOLD exposure = MNOR(risk * e.dependency)
        YIELD KEY r, exposure PROB

    -- Layer 3: Safety complement -- how safe is a node?
    CREATE RULE safe_node AS
        MATCH (m:Manufacturer)
        WHERE m IS NOT chain_risk
        YIELD KEY m, 1.0 AS safety PROB

    CREATE RULE safe_node AS
        MATCH (d:Distributor)
        WHERE d IS NOT chain_risk
        YIELD KEY d, 1.0 AS safety PROB

    CREATE RULE safe_node AS
        MATCH (r:Retailer)
        WHERE r IS NOT chain_risk
        YIELD KEY r, 1.0 AS safety PROB

    -- Queries
    QUERY supplier_risk
    QUERY chain_risk
    QUERY safe_node
"""

# ---------------------------------------------------------------------------
# 4. Execute and Print Results
# ---------------------------------------------------------------------------

result = session.locy(LOCY_PROGRAM)

# -- Print execution stats --
stats = result.stats
print("=" * 65)
print("SUPPLY CHAIN RISK PROPAGATION RESULTS")
print("=" * 65)
print(f"Strata evaluated : {stats.strata_evaluated}")
print(f"Total iterations : {stats.total_iterations}")
print(f"Evaluation time  : {stats.evaluation_time_secs:.4f}s")
print()

# -- Supplier base risk --
print("-" * 65)
print("SUPPLIER BASE RISK  (MNOR of raw signals)")
print("-" * 65)
supplier_rows = result.derived_facts("supplier_risk")
if supplier_rows:
    for row in sorted(supplier_rows, key=lambda r: r.get("risk", 0), reverse=True):
        name = row.get("s.name") or row.get("s", {}).get("name", "???")
        risk = row.get("risk", 0)
        print(f"  {name:<20s}  risk = {risk:.4f}")
print()

# -- Transitive chain risk --
print("-" * 65)
print("CHAIN RISK  (transitive MNOR propagation)")
print("-" * 65)
chain_rows = result.derived_facts("chain_risk")
if chain_rows:
    for row in sorted(chain_rows, key=lambda r: r.get("exposure", 0), reverse=True):
        # The KEY column is a node; extract name from whichever label matched
        node = None
        for key in ("m", "d", "r"):
            if key in row:
                node = row[key]
                break
        name = "???"
        if node is not None:
            name = node.get("name", "???") if isinstance(node, dict) else str(node)
        exposure = row.get("exposure", 0)
        print(f"  {name:<25s}  exposure = {exposure:.4f}")
print()

# -- Safety complement --
print("-" * 65)
print("SAFE NODES  (1 - chain_risk via IS NOT complement)")
print("-" * 65)
safe_rows = result.derived_facts("safe_node")
if safe_rows:
    for row in sorted(safe_rows, key=lambda r: r.get("safety", 0), reverse=True):
        node = None
        for key in ("m", "d", "r"):
            if key in row:
                node = row[key]
                break
        name = "???"
        if node is not None:
            name = node.get("name", "???") if isinstance(node, dict) else str(node)
        safety = row.get("safety", 0)
        print(f"  {name:<25s}  safety = {safety:.4f}")
print()

# -- Warnings --
if result.warnings:
    print("-" * 65)
    print("WARNINGS")
    print("-" * 65)
    for w in result.warnings:
        print(f"  {w}")
    print()

print("=" * 65)
print("Done.")

# ---------------------------------------------------------------------------
# 5. Cleanup
# ---------------------------------------------------------------------------

db.shutdown()
