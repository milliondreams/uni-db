import json
import os
import uuid


def generate_cell_id():
    """Generate a unique cell ID."""
    return str(uuid.uuid4()).replace("-", "")[:32]


def create_notebook(cells):
    # Add IDs to all cells if missing
    for cell in cells:
        if "id" not in cell:
            cell["id"] = generate_cell_id()
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "codemirror_mode": {"name": "ipython", "version": 3},
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.10.0",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def md_cell(source):
    return {
        "id": generate_cell_id(),
        "cell_type": "markdown",
        "metadata": {},
        "source": [line + "\n" for line in source],
    }


def code_cell(source):
    return {
        "id": generate_cell_id(),
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [line + "\n" for line in source],
    }


common_setup = [
    "import os",
    "import shutil",
    "import tempfile",
    "",
    "import uni_db",
]


def db_setup(name):
    return [
        f'db_path = os.path.join(tempfile.gettempdir(), "{name}_db")',
        "if os.path.exists(db_path):",
        "    shutil.rmtree(db_path)",
        "db = uni_db.Database(db_path)",
        'print(f"Opened database at {db_path}")',
    ]


# ===========================================================================
# 1. Supply Chain
# ===========================================================================
supply_chain_nb = create_notebook(
    [
        md_cell(
            [
                "# Supply Chain Management with Uni",
                "",
                "This notebook demonstrates how to model a supply chain graph to perform",
                "BOM (Bill of Materials) explosion, cost rollup, and supplier risk analysis.",
            ]
        ),
        code_cell(common_setup),
        code_cell(db_setup("supply_chain")),
        md_cell(
            [
                "## 1. Define Schema",
                "Parts, Suppliers, and Products with assembly and supply relationships.",
            ]
        ),
        code_cell(
            [
                "(",
                "    db.schema()",
                '    .label("Part")',
                '        .property("name", "string")',
                '        .property("sku", "string")',
                '        .property("cost", "float64")',
                '        .index("sku", "hash")',
                "    .done()",
                '    .label("Supplier")',
                '        .property("name", "string")',
                "    .done()",
                '    .label("Product")',
                '        .property("name", "string")',
                '        .property("price", "float64")',
                "    .done()",
                '    .edge_type("ASSEMBLED_FROM", ["Product", "Part"], ["Part"])',
                "    .done()",
                '    .edge_type("SUPPLIED_BY", ["Part"], ["Supplier"])',
                "    .done()",
                "    .apply()",
                ")",
                "",
                'print("Schema created")',
            ]
        ),
        md_cell(
            [
                "## 2. Ingest Data",
                "Two products sharing common parts, supplied by multiple vendors.",
            ]
        ),
        code_cell(
            [
                "# Parts: 7 components with different costs",
                "part_vids = db.bulk_insert_vertices('Part', [",
                "    {'name': 'Resistor 10K',   'sku': 'RES-10K',   'cost': 0.05},",
                "    {'name': 'Capacitor 100uF', 'sku': 'CAP-100UF', 'cost': 0.08},",
                "    {'name': 'Motherboard X1',  'sku': 'MB-X1',     'cost': 50.0},",
                "    {'name': 'OLED Screen',     'sku': 'SCR-OLED',  'cost': 30.0},",
                "    {'name': 'Battery 4000mAh', 'sku': 'BAT-4000',  'cost': 15.0},",
                "    {'name': 'ARM Processor',   'sku': 'PROC-ARM',  'cost': 80.0},",
                "    {'name': 'LCD Screen',      'sku': 'SCR-LCD',   'cost': 20.0},",
                "])",
                "res10k, cap100uf, mbx1, scr_oled, bat4000, proc_arm, scr_lcd = part_vids",
                "",
                "# Suppliers",
                "sup_vids = db.bulk_insert_vertices('Supplier', [",
                "    {'name': 'ResistorWorld'},",
                "    {'name': 'ScreenTech'},",
                "    {'name': 'CoreComponents'},",
                "])",
                "resistor_world, screen_tech, core_components = sup_vids",
                "",
                "# Products",
                "prod_vids = db.bulk_insert_vertices('Product', [",
                "    {'name': 'Smartphone X',  'price': 599.0},",
                "    {'name': 'TabletPro 10',  'price': 799.0},",
                "])",
                "smartphone, tablet = prod_vids",
                "",
                "# Assembly: Smartphone X -> OLED screen, shared parts",
                "db.bulk_insert_edges('ASSEMBLED_FROM', [",
                "    (smartphone, mbx1,     {}),",
                "    (smartphone, scr_oled, {}),",
                "    (smartphone, bat4000,  {}),",
                "    (smartphone, proc_arm, {}),",
                "    (mbx1,       res10k,   {}),",
                "    (mbx1,       cap100uf, {}),",
                "])",
                "",
                "# Assembly: TabletPro 10 -> LCD screen, shared parts",
                "db.bulk_insert_edges('ASSEMBLED_FROM', [",
                "    (tablet, mbx1,    {}),",
                "    (tablet, scr_lcd, {}),",
                "    (tablet, bat4000, {}),",
                "    (tablet, proc_arm, {}),",
                "])",
                "",
                "# Supply relationships",
                "db.bulk_insert_edges('SUPPLIED_BY', [",
                "    (res10k,   resistor_world,  {}),",
                "    (cap100uf, resistor_world,  {}),",
                "    (scr_oled, screen_tech,     {}),",
                "    (scr_lcd,  screen_tech,     {}),",
                "    (mbx1,     core_components, {}),",
                "    (bat4000,  core_components, {}),",
                "    (proc_arm, core_components, {}),",
                "])",
                "",
                "db.flush()",
                'print("Data ingested")',
            ]
        ),
        md_cell(
            [
                "## 3. BOM Explosion",
                "Which products are affected if RES-10K is defective?",
                "Traverses the assembly hierarchy upward.",
            ]
        ),
        code_cell(
            [
                'query_bom = """',
                "    MATCH (defective:Part {sku: 'RES-10K'})",
                "    MATCH (product:Product)-[:ASSEMBLED_FROM*1..5]->(defective)",
                "    RETURN product.name AS name, product.price AS price",
                "    ORDER BY product.price DESC",
                '"""',
                "results = db.query(query_bom)",
                "print('Products affected by defective RES-10K:')",
                "for r in results:",
                "    print(f\"  {r['name']} (${r['price']})\")",
                "assert len(results) == 2, f'Expected 2 affected products, got {len(results)}'",
            ]
        ),
        md_cell(
            [
                "## 4. Full BOM Listing",
                "Every part in Smartphone X with its cost, ordered by cost descending.",
            ]
        ),
        code_cell(
            [
                'query_parts = """',
                "    MATCH (p:Product {name: 'Smartphone X'})-[:ASSEMBLED_FROM*1..5]->(part:Part)",
                "    RETURN part.name AS part_name, part.sku AS sku, part.cost AS cost",
                "    ORDER BY cost DESC",
                '"""',
                "results = db.query(query_parts)",
                "print('Smartphone X BOM:')",
                "for r in results:",
                "    print(f\"  {r['part_name']} ({r['sku']}): ${r['cost']}\")",
            ]
        ),
        md_cell(
            [
                "## 5. Cost Rollup",
                "Total BOM cost per product — GROUP BY product with SUM of part costs.",
            ]
        ),
        code_cell(
            [
                'query_rollup = """',
                "    MATCH (p:Product)-[:ASSEMBLED_FROM*1..5]->(part:Part)",
                "    RETURN p.name AS product, SUM(part.cost) AS total_bom_cost",
                "    ORDER BY total_bom_cost DESC",
                '"""',
                "results = db.query(query_rollup)",
                "print('BOM cost rollup per product:')",
                "for r in results:",
                "    print(f\"  {r['product']}: ${r['total_bom_cost']:.2f}\")",
                "assert len(results) == 2, f'Expected 2 rows, got {len(results)}'",
            ]
        ),
        md_cell(
            [
                "## 6. Supply Chain Risk",
                "Which supplier is critical to the most products?",
            ]
        ),
        code_cell(
            [
                'query_risk = """',
                "    MATCH (p:Product)-[:ASSEMBLED_FROM*1..5]->(part:Part)-[:SUPPLIED_BY]->(s:Supplier)",
                "    RETURN s.name AS supplier, COUNT(DISTINCT p) AS products_at_risk",
                "    ORDER BY products_at_risk DESC",
                '"""',
                "results = db.query(query_risk)",
                "print('Supplier risk analysis:')",
                "for r in results:",
                "    print(f\"  {r['supplier']}: {r['products_at_risk']} product(s) at risk\")",
                "top = results[0]",
                "assert top['supplier'] == 'CoreComponents', f\"Expected CoreComponents, got {top['supplier']}\"",
                "assert top['products_at_risk'] == 2, f\"Expected 2, got {top['products_at_risk']}\"",
            ]
        ),
    ]
)

# ===========================================================================
# 2. Recommendation Engine
# ===========================================================================
rec_nb = create_notebook(
    [
        md_cell(
            [
                "# Recommendation Engine",
                "",
                "Collaborative filtering via graph traversal combined with semantic",
                "vector search for book recommendations.",
            ]
        ),
        code_cell(common_setup),
        code_cell(db_setup("recommendation")),
        md_cell(
            [
                "## 1. Schema",
                "Books with 4D semantic embeddings; users linked via PURCHASED edges.",
            ]
        ),
        code_cell(
            [
                "(",
                "    db.schema()",
                '    .label("User")',
                '        .property("name", "string")',
                "    .done()",
                '    .label("Book")',
                '        .property("name", "string")',
                '        .property("genre", "string")',
                '        .vector("embedding", 4)',
                "    .done()",
                '    .edge_type("PURCHASED", ["User"], ["Book"])',
                "    .done()",
                "    .apply()",
                ")",
                "",
                'print("Schema created")',
            ]
        ),
        md_cell(["## 2. Ingest Data"]),
        code_cell(
            [
                "# 4D embeddings: [tech, fiction, history, science]",
                "book_vids = db.bulk_insert_vertices('Book', [",
                "    {'name': 'Clean Code',                        'genre': 'tech',    'embedding': [0.95, 0.05, 0.0,  0.0 ]},",
                "    {'name': 'The Pragmatic Programmer',          'genre': 'tech',    'embedding': [0.90, 0.10, 0.0,  0.0 ]},",
                "    {'name': 'Designing Data-Intensive Apps',     'genre': 'tech',    'embedding': [0.85, 0.0,  0.0,  0.15]},",
                "    {'name': 'Dune',                              'genre': 'fiction', 'embedding': [0.0,  0.95, 0.0,  0.05]},",
                "    {'name': 'Foundation',                        'genre': 'fiction', 'embedding': [0.0,  0.85, 0.0,  0.15]},",
                "    {'name': 'Sapiens',                           'genre': 'history', 'embedding': [0.0,  0.05, 0.7,  0.25]},",
                "])",
                "clean_code, pragmatic, ddia, dune, foundation, sapiens = book_vids",
                "",
                "user_vids = db.bulk_insert_vertices('User', [",
                "    {'name': 'Alice'},",
                "    {'name': 'Bob'},",
                "    {'name': 'Carol'},",
                "    {'name': 'Dave'},",
                "])",
                "alice, bob, carol, dave = user_vids",
                "",
                "# Purchase history",
                "db.bulk_insert_edges('PURCHASED', [",
                "    (alice, clean_code,  {}),",
                "    (alice, pragmatic,   {}),",
                "    (bob,   clean_code,  {}),",
                "    (bob,   dune,        {}),",
                "    (carol, pragmatic,   {}),",
                "    (carol, foundation,  {}),",
                "    (dave,  dune,        {}),",
                "    (dave,  foundation,  {}),",
                "    (dave,  sapiens,     {}),",
                "])",
                "",
                "db.flush()",
                "",
                "# Create vector index AFTER flush",
                'db.create_vector_index("Book", "embedding", "l2")',
                'print("Data ingested and vector index created")',
            ]
        ),
        md_cell(
            [
                "## 3. Collaborative Filtering",
                "Books that users-who-bought-Alice's-books also bought (that Alice hasn't read).",
            ]
        ),
        code_cell(
            [
                'query_collab = """',
                "    MATCH (alice:User {name: 'Alice'})-[:PURCHASED]->(b:Book)<-[:PURCHASED]-(other:User)",
                "    WHERE other._vid <> alice._vid",
                "    MATCH (other)-[:PURCHASED]->(rec:Book)",
                "    WHERE NOT (alice)-[:PURCHASED]->(rec)",
                "    RETURN rec.name AS recommendation, COUNT(DISTINCT other) AS buyers",
                "    ORDER BY buyers DESC",
                '"""',
                "results = db.query(query_collab)",
                "print('Collaborative recommendations for Alice:')",
                "for r in results:",
                "    print(f\"  {r['recommendation']} (bought by {r['buyers']} similar user(s))\")",
            ]
        ),
        md_cell(
            [
                "## 4. Semantic Vector Search",
                "Find the 3 books most similar to a 'tech' query vector.",
            ]
        ),
        code_cell(
            [
                "tech_query = [0.95, 0.05, 0.0, 0.0]",
                "",
                'results = db.query("""',
                "    CALL uni.vector.query('Book', 'embedding', $vec, 3)",
                "    YIELD node, distance",
                "    RETURN node.name AS title, node.genre AS genre, distance",
                "    ORDER BY distance",
                '""", {\'vec\': tech_query})',
                "",
                "print('Top 3 books semantically similar to tech query:')",
                "for r in results:",
                "    print(f\"  [{r['distance']:.4f}] {r['title']} ({r['genre']})\")",
                "",
                "# All 3 results should be tech books",
                "genres = [r['genre'] for r in results]",
                "assert all(g == 'tech' for g in genres), f'Expected all tech, got {genres}'",
            ]
        ),
        md_cell(
            [
                "## 5. Hybrid: Vector + Graph",
                "Vector search for fiction books, then find which users bought them.",
            ]
        ),
        code_cell(
            [
                "fiction_query = [0.0, 0.95, 0.0, 0.05]",
                "",
                'results = db.query("""',
                "    CALL uni.vector.query('Book', 'embedding', $vec, 3)",
                "    YIELD node, distance",
                "    MATCH (u:User)-[:PURCHASED]->(node)",
                "    RETURN node.name AS book, u.name AS buyer, distance",
                "    ORDER BY distance, buyer",
                '""", {\'vec\': fiction_query})',
                "",
                "print('Fiction book buyers (via vector + graph):')",
                "for r in results:",
                "    print(f\"  {r['buyer']} bought '{r['book']}' (distance={r['distance']:.4f})\")",
            ]
        ),
        md_cell(
            [
                "## 6. Discovery: Popular Books Outside Alice's Profile",
                "Books Alice hasn't bought, ranked by how many users bought them.",
            ]
        ),
        code_cell(
            [
                'query_discovery = """',
                "    MATCH (alice:User {name: 'Alice'})",
                "    MATCH (u:User)-[:PURCHASED]->(b:Book)",
                "    WHERE NOT (alice)-[:PURCHASED]->(b) AND u._vid <> alice._vid",
                "    RETURN b.name AS book, COUNT(DISTINCT u) AS buyers",
                "    ORDER BY buyers DESC",
                '"""',
                "results = db.query(query_discovery)",
                "print('Popular books Alice has not read:')",
                "for r in results:",
                "    print(f\"  {r['book']}: {r['buyers']} buyer(s)\")",
            ]
        ),
    ]
)

# ===========================================================================
# 3. RAG (Retrieval-Augmented Generation)
# ===========================================================================
rag_nb = create_notebook(
    [
        md_cell(
            [
                "# Retrieval-Augmented Generation (RAG)",
                "",
                "Combining vector search with knowledge graph traversal for hybrid",
                "retrieval over Python web framework documentation.",
            ]
        ),
        code_cell(common_setup),
        code_cell(db_setup("rag")),
        md_cell(
            [
                "## 1. Schema",
                "Text chunks with embeddings, linked to named entities via MENTIONS edges.",
            ]
        ),
        code_cell(
            [
                "(",
                "    db.schema()",
                '    .label("Chunk")',
                '        .property("chunk_id", "string")',
                '        .property("text", "string")',
                '        .vector("embedding", 4)',
                "    .done()",
                '    .label("Entity")',
                '        .property("name", "string")',
                '        .property("type", "string")',
                "    .done()",
                '    .edge_type("MENTIONS", ["Chunk"], ["Entity"])',
                "    .done()",
                "    .apply()",
                ")",
                "",
                'print("Schema created")',
            ]
        ),
        md_cell(
            [
                "## 2. Ingest Data",
                "8 documentation chunks across 4 topics, with 6 entities.",
            ]
        ),
        code_cell(
            [
                "# 4D embeddings: [auth, routing, database, testing]",
                "chunk_vids = db.bulk_insert_vertices('Chunk', [",
                "    {'chunk_id': 'c1', 'text': 'JWT tokens issued by /auth/login endpoint. Tokens expire after 1 hour.',",
                "     'embedding': [1.0,  0.0,  0.0,  0.0 ]},",
                "    {'chunk_id': 'c2', 'text': 'Token refresh via /auth/refresh. Send expired token, receive new one.',",
                "     'embedding': [0.95, 0.05, 0.0,  0.0 ]},",
                "    {'chunk_id': 'c3', 'text': 'Password hashing uses bcrypt with cost factor 12.',",
                "     'embedding': [0.85, 0.0,  0.0,  0.15]},",
                "    {'chunk_id': 'c4', 'text': 'Routes defined with @app.route decorator. Supports GET, POST, PUT, DELETE.',",
                "     'embedding': [0.0,  1.0,  0.0,  0.0 ]},",
                "    {'chunk_id': 'c5', 'text': 'Middleware intercepts requests before handlers. Register with app.use().',",
                "     'embedding': [0.05, 0.9,  0.05, 0.0 ]},",
                "    {'chunk_id': 'c6', 'text': 'ConnectionPool manages DB connections. Max pool size defaults to 10.',",
                "     'embedding': [0.0,  0.0,  1.0,  0.0 ]},",
                "    {'chunk_id': 'c7', 'text': 'ORM models inherit from BaseModel. Columns map to database fields.',",
                "     'embedding': [0.0,  0.1,  0.9,  0.0 ]},",
                "    {'chunk_id': 'c8', 'text': 'TestClient simulates HTTP requests without starting a server.',",
                "     'embedding': [0.0,  0.2,  0.0,  0.8 ]},",
                "])",
                "c1, c2, c3, c4, c5, c6, c7, c8 = chunk_vids",
                "",
                "# Entities",
                "entity_vids = db.bulk_insert_vertices('Entity', [",
                "    {'name': 'JWT',            'type': 'technology'},",
                "    {'name': 'authentication', 'type': 'concept'},",
                "    {'name': 'routing',        'type': 'concept'},",
                "    {'name': 'database',       'type': 'concept'},",
                "    {'name': 'bcrypt',         'type': 'technology'},",
                "    {'name': 'ConnectionPool', 'type': 'class'},",
                "])",
                "jwt, auth_entity, routing_entity, db_entity, bcrypt_entity, pool_entity = entity_vids",
                "",
                "# MENTIONS edges",
                "db.bulk_insert_edges('MENTIONS', [",
                "    (c1, jwt,          {}),",
                "    (c1, auth_entity,  {}),",
                "    (c2, jwt,          {}),",
                "    (c2, auth_entity,  {}),",
                "    (c3, bcrypt_entity,{}),",
                "    (c3, auth_entity,  {}),",
                "    (c4, routing_entity, {}),",
                "    (c5, routing_entity, {}),",
                "    (c6, db_entity,    {}),",
                "    (c6, pool_entity,  {}),",
                "    (c7, db_entity,    {}),",
                "])",
                "",
                "db.flush()",
                "",
                "# Create vector index AFTER flush",
                'db.create_vector_index("Chunk", "embedding", "l2")',
                'print("Data ingested and vector index created")',
            ]
        ),
        md_cell(
            [
                "## 3. Pure Vector Search",
                "Find the 3 chunks most similar to an authentication query.",
            ]
        ),
        code_cell(
            [
                "auth_query = [1.0, 0.0, 0.0, 0.0]",
                "",
                'results = db.query("""',
                "    CALL uni.vector.query('Chunk', 'embedding', $vec, 3)",
                "    YIELD node, distance",
                "    RETURN node.chunk_id AS chunk_id, node.text AS text, distance",
                "    ORDER BY distance",
                '""", {\'vec\': auth_query})',
                "",
                "print('Top 3 chunks for auth query:')",
                "for r in results:",
                "    print(f\"  [{r['distance']:.4f}] {r['chunk_id']}: {r['text'][:60]}...\")",
                "",
                "chunk_ids = [r['chunk_id'] for r in results]",
                "assert set(chunk_ids) == {'c1', 'c2', 'c3'}, f'Expected auth chunks c1/c2/c3, got {chunk_ids}'",
            ]
        ),
        md_cell(
            [
                "## 4. Graph Expansion",
                "Same vector seeds — now also show which entities each chunk mentions.",
            ]
        ),
        code_cell(
            [
                'results = db.query("""',
                "    CALL uni.vector.query('Chunk', 'embedding', $vec, 3)",
                "    YIELD node, distance",
                "    MATCH (node)-[:MENTIONS]->(e:Entity)",
                "    RETURN node.chunk_id AS chunk_id, e.name AS entity, distance",
                "    ORDER BY distance, entity",
                '""", {\'vec\': auth_query})',
                "",
                "print('Entities mentioned by top auth chunks:')",
                "for r in results:",
                "    print(f\"  {r['chunk_id']} -> {r['entity']}\")",
            ]
        ),
        md_cell(
            [
                "## 5. Entity Bridging",
                "Find all chunks related to the auth seeds via shared entity mentions.",
                "This is the graph RAG technique: expand context through shared concepts.",
            ]
        ),
        code_cell(
            [
                'results = db.query("""',
                "    CALL uni.vector.query('Chunk', 'embedding', $vec, 3)",
                "    YIELD node AS anchor, distance",
                "    MATCH (anchor)-[:MENTIONS]->(e:Entity)<-[:MENTIONS]-(related:Chunk)",
                "    WHERE related._vid <> anchor._vid",
                "    RETURN anchor.chunk_id AS anchor_id, e.name AS bridge_entity,",
                "           related.chunk_id AS related_id",
                "    ORDER BY anchor_id, bridge_entity",
                '""", {\'vec\': auth_query})',
                "",
                "print('Entity bridges between auth chunks:')",
                "for r in results:",
                "    print(f\"  {r['anchor_id']} <-> {r['related_id']} (via {r['bridge_entity']})\")",
            ]
        ),
        md_cell(
            [
                "## 6. Context Assembly",
                "Full hybrid pipeline: vector seeds + graph bridging -> collect unique chunks",
                "for the LLM context window.",
            ]
        ),
        code_cell(
            [
                'results = db.query("""',
                "    CALL uni.vector.query('Chunk', 'embedding', $vec, 3)",
                "    YIELD node AS seed, distance",
                "    MATCH (seed)-[:MENTIONS]->(e:Entity)<-[:MENTIONS]-(related:Chunk)",
                "    RETURN seed.chunk_id AS seed_id, seed.text AS seed_text,",
                "           related.chunk_id AS related_id, related.text AS related_text,",
                "           e.name AS shared_entity",
                "    ORDER BY seed_id, shared_entity",
                '""", {\'vec\': auth_query})',
                "",
                "# Collect all unique chunk texts for LLM context",
                "context_chunks = {}",
                "for r in results:",
                "    context_chunks[r['seed_id']]    = r['seed_text']",
                "    context_chunks[r['related_id']] = r['related_text']",
                "",
                "print(f'Assembled {len(context_chunks)} unique chunks for LLM context:')",
                "for cid, text in sorted(context_chunks.items()):",
                "    print(f'  [{cid}] {text[:70]}...')",
            ]
        ),
    ]
)

# ===========================================================================
# 4. Fraud Detection
# ===========================================================================
fraud_nb = create_notebook(
    [
        md_cell(
            [
                "# Fraud Detection",
                "",
                "Detecting money laundering rings (3-cycles) and shared device anomalies",
                "using graph pattern matching.",
            ]
        ),
        code_cell(common_setup),
        code_cell(db_setup("fraud")),
        md_cell(["## 1. Schema"]),
        code_cell(
            [
                "(",
                "    db.schema()",
                '    .label("User")',
                '        .property("name", "string")',
                '        .property("email", "string")',
                '        .property_nullable("risk_score", "float32")',
                "    .done()",
                '    .label("Device")',
                '        .property("device_id", "string")',
                "    .done()",
                '    .edge_type("SENT_MONEY", ["User"], ["User"])',
                '        .property("amount", "float64")',
                "    .done()",
                '    .edge_type("USED_DEVICE", ["User"], ["Device"])',
                "    .done()",
                "    .apply()",
                ")",
                "",
                'print("Schema created")',
            ]
        ),
        md_cell(
            [
                "## 2. Ingestion",
                "5 named users, 3 devices, a money ring, and suspicious cross-device links.",
            ]
        ),
        code_cell(
            [
                "# 5 users: 3 in a ring, 2 high-risk fraudsters",
                "u_vids = db.bulk_insert_vertices('User', [",
                "    {'name': 'Alice',  'email': 'alice@example.com',  'risk_score': 0.10},",
                "    {'name': 'Bob',    'email': 'bob@example.com',    'risk_score': 0.15},",
                "    {'name': 'Carlos', 'email': 'carlos@example.com', 'risk_score': 0.20},",
                "    {'name': 'Dana',   'email': 'dana@example.com',   'risk_score': 0.92},",
                "    {'name': 'Eve',    'email': 'eve@example.com',    'risk_score': 0.88},",
                "])",
                "alice, bob, carlos, dana, eve = u_vids",
                "",
                "# 3 devices",
                "d_vids = db.bulk_insert_vertices('Device', [",
                "    {'device_id': 'device_A'},",
                "    {'device_id': 'device_B'},",
                "    {'device_id': 'device_C'},",
                "])",
                "device_a, device_b, device_c = d_vids",
                "",
                "# Money ring: Alice -> Bob -> Carlos -> Alice",
                "db.bulk_insert_edges('SENT_MONEY', [",
                "    (alice,  bob,    {'amount': 9500.0}),",
                "    (bob,    carlos, {'amount': 9000.0}),",
                "    (carlos, alice,  {'amount': 8750.0}),",
                "    (dana,   eve,    {'amount': 15000.0}),  # Suspicious transfer",
                "])",
                "",
                "# Device sharing: Alice+Dana on device_A, Bob+Eve on device_B, Carlos alone on device_C",
                "db.bulk_insert_edges('USED_DEVICE', [",
                "    (alice,  device_a, {}),",
                "    (dana,   device_a, {}),",
                "    (bob,    device_b, {}),",
                "    (eve,    device_b, {}),",
                "    (carlos, device_c, {}),",
                "])",
                "",
                "db.flush()",
                'print("Data ingested")',
            ]
        ),
        md_cell(
            [
                "## 3. Ring Detection",
                "Find 3-cycles in the money transfer graph.",
                "Deduplication: `a._vid < b._vid AND a._vid < c._vid` prevents each ring",
                "appearing 3 times (once per starting node).",
            ]
        ),
        code_cell(
            [
                'query_ring = """',
                "    MATCH (a:User)-[:SENT_MONEY]->(b:User)-[:SENT_MONEY]->(c:User)-[:SENT_MONEY]->(a)",
                "    WHERE a._vid < b._vid AND a._vid < c._vid",
                "    RETURN a.name AS user_a, b.name AS user_b, c.name AS user_c,",
                "           COUNT(*) AS rings",
                '"""',
                "results = db.query(query_ring)",
                "print('Money laundering rings detected:')",
                "for r in results:",
                "    print(f\"  Ring: {r['user_a']} | {r['user_b']} | {r['user_c']} ({r['rings']} ring(s))\")",
                "assert len(results) == 1, f'Expected 1 ring, got {len(results)}'",
            ]
        ),
        md_cell(
            [
                "## 4. Ring with Transfer Amounts",
                "Same pattern, but also retrieve edge properties to show total cycled money.",
            ]
        ),
        code_cell(
            [
                'query_amounts = """',
                "    MATCH (a:User)-[r1:SENT_MONEY]->(b:User)-[r2:SENT_MONEY]->(c:User)-[r3:SENT_MONEY]->(a)",
                "    WHERE a._vid < b._vid AND a._vid < c._vid",
                "    RETURN a.name AS user_a, b.name AS user_b, c.name AS user_c,",
                "           r1.amount AS leg1, r2.amount AS leg2, r3.amount AS leg3,",
                "           r1.amount + r2.amount + r3.amount AS total_cycled",
                '"""',
                "results = db.query(query_amounts)",
                "for r in results:",
                "    print(f\"Ring: {r['user_a']} -> {r['user_b']} -> {r['user_c']} -> {r['user_a']}\")",
                "    print(f\"  Leg amounts: ${r['leg1']:.0f}, ${r['leg2']:.0f}, ${r['leg3']:.0f}\")",
                "    print(f\"  Total cycled: ${r['total_cycled']:,.0f}\")",
            ]
        ),
        md_cell(
            [
                "## 5. Shared Device Risk",
                "Find users who share a device with a high-risk user (risk > 0.8).",
                "Carlos should NOT appear — he only uses device_C alone.",
            ]
        ),
        code_cell(
            [
                'query_shared = """',
                "    MATCH (u:User)-[:USED_DEVICE]->(d:Device)<-[:USED_DEVICE]-(fraudster:User)",
                "    WHERE fraudster.risk_score > 0.8 AND u._vid <> fraudster._vid",
                "    RETURN u.name AS user, d.device_id AS device, fraudster.name AS flagged_contact",
                "    ORDER BY user",
                '"""',
                "results = db.query(query_shared)",
                "print('Users sharing device with high-risk account:')",
                "for r in results:",
                "    print(f\"  {r['user']} shares {r['device']} with {r['flagged_contact']}\")",
                "",
                "names = [r['user'] for r in results]",
                "assert 'Carlos' not in names, f'Carlos should not appear, got {names}'",
            ]
        ),
        md_cell(
            [
                "## 6. Combined Alert: Ring + Device Sharing",
                "Users appearing in BOTH a money ring AND sharing a device with a fraudster",
                "are the highest-priority investigation targets.",
            ]
        ),
        code_cell(
            [
                "# Ring members",
                'ring_query = """',
                "    MATCH (a:User)-[:SENT_MONEY]->(b:User)-[:SENT_MONEY]->(c:User)-[:SENT_MONEY]->(a)",
                "    WHERE a._vid < b._vid AND a._vid < c._vid",
                "    RETURN a.name AS n UNION",
                "    MATCH (a:User)-[:SENT_MONEY]->(b:User)-[:SENT_MONEY]->(c:User)-[:SENT_MONEY]->(a)",
                "    WHERE a._vid < b._vid AND a._vid < c._vid",
                "    RETURN b.name AS n UNION",
                "    MATCH (a:User)-[:SENT_MONEY]->(b:User)-[:SENT_MONEY]->(c:User)-[:SENT_MONEY]->(a)",
                "    WHERE a._vid < b._vid AND a._vid < c._vid",
                "    RETURN c.name AS n",
                '"""',
                "ring_members = {r['n'] for r in db.query(ring_query)}",
                "",
                "# Device-sharing users",
                'device_query = """',
                "    MATCH (u:User)-[:USED_DEVICE]->(d:Device)<-[:USED_DEVICE]-(fraudster:User)",
                "    WHERE fraudster.risk_score > 0.8 AND u._vid <> fraudster._vid",
                "    RETURN u.name AS n",
                '"""',
                "device_risk = {r['n'] for r in db.query(device_query)}",
                "",
                "combined = ring_members & device_risk",
                "print(f'Ring members: {sorted(ring_members)}')",
                "print(f'Device-sharing users: {sorted(device_risk)}')",
                "print(f'HIGH PRIORITY (both signals): {sorted(combined)}')",
                "assert 'Alice' in combined, f'Alice should be in combined alert, got {combined}'",
            ]
        ),
    ]
)

# ===========================================================================
# 5. Sales Analytics
# ===========================================================================
sales_nb = create_notebook(
    [
        md_cell(
            [
                "# Regional Sales Analytics",
                "",
                "Combining graph traversal with columnar aggregation across multiple",
                "regions and product categories.",
            ]
        ),
        code_cell(common_setup),
        code_cell(db_setup("sales")),
        md_cell(
            ["## 1. Schema", "Regions, Categories, and Orders with two edge types."]
        ),
        code_cell(
            [
                "(",
                "    db.schema()",
                '    .label("Region")',
                '        .property("name", "string")',
                "    .done()",
                '    .label("Category")',
                '        .property("name", "string")',
                "    .done()",
                '    .label("Order")',
                '        .property("amount", "float64")',
                "    .done()",
                '    .edge_type("SHIPPED_TO", ["Order"], ["Region"])',
                "    .done()",
                '    .edge_type("IN_CATEGORY", ["Order"], ["Category"])',
                "    .done()",
                "    .apply()",
                ")",
                "",
                'print("Schema created")',
            ]
        ),
        md_cell(
            [
                "## 2. Ingest Data",
                "4 regions, 3 categories, ~38 orders distributed non-uniformly.",
            ]
        ),
        code_cell(
            [
                "# Regions",
                "region_vids = db.bulk_insert_vertices('Region', [",
                "    {'name': 'North'},",
                "    {'name': 'South'},",
                "    {'name': 'East'},",
                "    {'name': 'West'},",
                "])",
                "north, south, east, west = region_vids",
                "",
                "# Categories",
                "cat_vids = db.bulk_insert_vertices('Category', [",
                "    {'name': 'Electronics'},",
                "    {'name': 'Apparel'},",
                "    {'name': 'Home & Garden'},",
                "])",
                "electronics, apparel, home_garden = cat_vids",
                "",
                "# Orders: (amount, region_vid, category_vid)",
                "orders_data = [",
                "    # East: high-value electronics",
                "    (1200.0, east,  electronics),",
                "    (980.0,  east,  electronics),",
                "    (450.0,  east,  apparel),",
                "    (120.0,  east,  apparel),",
                "    (85.0,   east,  apparel),",
                "    (60.0,   east,  home_garden),",
                "    # West: strong Home & Garden",
                "    (890.0,  west,  home_garden),",
                "    (620.0,  west,  home_garden),",
                "    (450.0,  west,  home_garden),",
                "    (340.0,  west,  home_garden),",
                "    (500.0,  west,  electronics),",
                "    (210.0,  west,  apparel),",
                "    (180.0,  west,  apparel),",
                "    # South: most electronics orders",
                "    (750.0,  south, electronics),",
                "    (680.0,  south, electronics),",
                "    (590.0,  south, electronics),",
                "    (520.0,  south, electronics),",
                "    (480.0,  south, electronics),",
                "    (300.0,  south, apparel),",
                "    (250.0,  south, home_garden),",
                "    # North: balanced mix",
                "    (400.0,  north, electronics),",
                "    (350.0,  north, electronics),",
                "    (280.0,  north, apparel),",
                "    (260.0,  north, apparel),",
                "    (240.0,  north, apparel),",
                "    (320.0,  north, home_garden),",
                "    (290.0,  north, home_garden),",
                "]",
                "",
                "order_props = [{'amount': amt} for amt, _, _ in orders_data]",
                "order_vids = db.bulk_insert_vertices('Order', order_props)",
                "",
                "shipped_edges = [(ov, rv, {}) for ov, (_, rv, _) in zip(order_vids, orders_data)]",
                "category_edges = [(ov, cv, {}) for ov, (_, _, cv) in zip(order_vids, orders_data)]",
                "",
                "db.bulk_insert_edges('SHIPPED_TO',   shipped_edges)",
                "db.bulk_insert_edges('IN_CATEGORY',  category_edges)",
                "db.flush()",
                'print("Data ingested")',
            ]
        ),
        md_cell(
            [
                "## 3. Revenue by Region",
                "Total revenue and order count per region, ordered by revenue.",
            ]
        ),
        code_cell(
            [
                'query_region = """',
                "    MATCH (r:Region)<-[:SHIPPED_TO]-(o:Order)",
                "    RETURN r.name AS region, COUNT(o) AS order_count, SUM(o.amount) AS total_revenue",
                "    ORDER BY total_revenue DESC",
                '"""',
                "results = db.query(query_region)",
                "print('Revenue by region:')",
                "for r in results:",
                "    print(f\"  {r['region']:10s}: {r['order_count']:3d} orders, ${r['total_revenue']:8.2f}\")",
                "assert len(results) == 4, f'Expected 4 regions, got {len(results)}'",
            ]
        ),
        md_cell(
            [
                "## 4. Region × Category Breakdown",
                "12-row breakdown showing revenue for every region/category pair.",
            ]
        ),
        code_cell(
            [
                'query_breakdown = """',
                "    MATCH (r:Region)<-[:SHIPPED_TO]-(o:Order)-[:IN_CATEGORY]->(c:Category)",
                "    RETURN r.name AS region, c.name AS category,",
                "           COUNT(o) AS orders, SUM(o.amount) AS revenue",
                "    ORDER BY region, revenue DESC",
                '"""',
                "results = db.query(query_breakdown)",
                "print('Region x Category breakdown:')",
                "current_region = None",
                "for r in results:",
                "    if r['region'] != current_region:",
                "        current_region = r['region']",
                '        print(f"  {current_region}:")',
                "    print(f\"    {r['category']:15s}: {r['orders']} orders, ${r['revenue']:.2f}\")",
                "assert len(results) == 12, f'Expected 12 rows (4 regions x 3 categories), got {len(results)}'",
            ]
        ),
        md_cell(
            ["## 5. Top Orders per Region", "Highest-value orders in each region."]
        ),
        code_cell(
            [
                "from collections import defaultdict",
                "",
                "# Query top 2 orders per region by fetching all and processing",
                'query_top = """',
                "    MATCH (r:Region)<-[:SHIPPED_TO]-(o:Order)",
                "    RETURN r.name AS region, o.amount AS amount",
                "    ORDER BY region, amount DESC",
                '"""',
                "all_orders = db.query(query_top)",
                "",
                "# Group by region, take top 2",
                "region_orders = defaultdict(list)",
                "for row in all_orders:",
                "    region_orders[row['region']].append(row['amount'])",
                "",
                "print('Top 2 orders per region:')",
                "for region in sorted(region_orders):",
                "    top2 = region_orders[region][:2]",
                "    print(f'  {region}: {[f\"${v:.0f}\" for v in top2]}')",
                "",
                "assert region_orders['East'][0] == 1200.0, 'East should lead with $1200'",
            ]
        ),
        md_cell(
            [
                "## 6. Best Category per Region",
                "The highest-revenue category for each region.",
            ]
        ),
        code_cell(
            [
                'query_best_cat = """',
                "    MATCH (r:Region)<-[:SHIPPED_TO]-(o:Order)-[:IN_CATEGORY]->(c:Category)",
                "    RETURN r.name AS region, c.name AS category, SUM(o.amount) AS revenue",
                "    ORDER BY region, revenue DESC",
                '"""',
                "results = db.query(query_best_cat)",
                "",
                "# Take first (highest revenue) category per region",
                "best = {}",
                "for r in results:",
                "    if r['region'] not in best:",
                "        best[r['region']] = (r['category'], r['revenue'])",
                "",
                "print('Best category per region:')",
                "for region, (cat, rev) in sorted(best.items()):",
                "    print(f'  {region:10s}: {cat} (${rev:.2f})')",
                "",
                "# Each region should have a different best category",
                "best_cats = [cat for cat, _ in best.values()]",
                "assert len(set(best_cats)) > 1, f'Expected variance across regions, got {best_cats}'",
            ]
        ),
    ]
)

# Write notebooks to the same directory as this script
script_dir = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(script_dir, "supply_chain.ipynb"), "w") as f:
    json.dump(supply_chain_nb, f, indent=2)

with open(os.path.join(script_dir, "recommendation.ipynb"), "w") as f:
    json.dump(rec_nb, f, indent=2)

with open(os.path.join(script_dir, "rag.ipynb"), "w") as f:
    json.dump(rag_nb, f, indent=2)

with open(os.path.join(script_dir, "fraud_detection.ipynb"), "w") as f:
    json.dump(fraud_nb, f, indent=2)

with open(os.path.join(script_dir, "sales_analytics.ipynb"), "w") as f:
    json.dump(sales_nb, f, indent=2)

print("Notebooks created successfully.")
