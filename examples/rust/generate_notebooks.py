#!/usr/bin/env python3
"""Generate Rust Jupyter notebooks for Uni examples.

These notebooks use evcxr_jupyter as the Rust kernel for Jupyter.
Install with: cargo install evcxr_jupyter && evcxr_jupyter --install
"""

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
                "display_name": "Rust",
                "language": "rust",
                "name": "rust",
            },
            "language_info": {
                "codemirror_mode": "rust",
                "file_extension": ".rs",
                "mimetype": "text/rust",
                "name": "Rust",
                "pygment_lexer": "rust",
                "version": "",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def md_cell(source):
    if isinstance(source, list):
        source = "\n".join(source)
    return {
        "id": generate_cell_id(),
        "cell_type": "markdown",
        "metadata": {},
        "source": source,
    }


def code_cell(source):
    if isinstance(source, list):
        source = "\n".join(source)
    return {
        "id": generate_cell_id(),
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source,
    }


# Common setup for all notebooks - load the uni crate
common_deps = """:dep uni-db = { path = "../../../crates/uni" }
:dep tokio = { version = "1", features = ["full"] }
:dep serde_json = "1"
"""

common_imports = """use uni::{Uni, DataType, IndexType, ScalarType, VectorMetric, VectorAlgo, VectorIndexCfg};
use std::collections::HashMap;
use serde_json::json;

// Helper macro to run async code in evcxr
macro_rules! run {
    ($e:expr) => {
        tokio::runtime::Runtime::new().unwrap().block_on($e)
    };
}
"""


def db_setup(name):
    return f"""let db_path = "./{name}_db";

// Clean up any existing database
if std::path::Path::new(db_path).exists() {{
    std::fs::remove_dir_all(db_path).unwrap();
}}

let db = run!(Uni::open(db_path).build()).unwrap();
println!("Opened database at {{}}", db_path);
"""


# =============================================================================
# 1. Supply Chain
# =============================================================================
supply_chain_nb = create_notebook(
    [
        md_cell(
            [
                "# Supply Chain Management with Uni (Rust)",
                "",
                "BOM explosion, cost rollup, and supplier risk analysis using Uni's native Rust API.",
            ]
        ),
        code_cell(common_deps),
        code_cell(common_imports),
        code_cell(db_setup("supply_chain")),
        md_cell(
            [
                "## 1. Define Schema",
                "",
                "Parts (with name), Suppliers, and Products, along with ASSEMBLED_FROM and SUPPLIED_BY relationships.",
            ]
        ),
        code_cell(
            """run!(async {
    db.schema()
        .label("Part")
            .property("name", DataType::String)
            .property("sku",  DataType::String)
            .property("cost", DataType::Float64)
            .index("sku", IndexType::Scalar(ScalarType::Hash))
        .label("Supplier")
            .property("name", DataType::String)
        .label("Product")
            .property("name",  DataType::String)
            .property("price", DataType::Float64)
        .edge_type("ASSEMBLED_FROM", &["Product", "Part"], &["Part"])
        .edge_type("SUPPLIED_BY",    &["Part"],             &["Supplier"])
        .apply()
        .await
}).unwrap();

println!("Schema created successfully");"""
        ),
        md_cell(
            [
                "## 2. Ingest Data",
                "",
                "7 parts, 3 suppliers, 2 products — same topology as the Python notebook.",
            ]
        ),
        code_cell(
            """// 7 parts
let part_props = vec![
    HashMap::from([("name".to_string(), json!("Resistor 10K")),    ("sku".to_string(), json!("RES-10K")),   ("cost".to_string(), json!(0.05))]),
    HashMap::from([("name".to_string(), json!("Capacitor 100uF")), ("sku".to_string(), json!("CAP-100UF")), ("cost".to_string(), json!(0.08))]),
    HashMap::from([("name".to_string(), json!("Motherboard X1")),  ("sku".to_string(), json!("MB-X1")),     ("cost".to_string(), json!(50.0))]),
    HashMap::from([("name".to_string(), json!("OLED Screen")),     ("sku".to_string(), json!("SCR-OLED")),  ("cost".to_string(), json!(30.0))]),
    HashMap::from([("name".to_string(), json!("Battery 4000mAh")), ("sku".to_string(), json!("BAT-4000")),  ("cost".to_string(), json!(15.0))]),
    HashMap::from([("name".to_string(), json!("ARM Processor")),   ("sku".to_string(), json!("PROC-ARM")),  ("cost".to_string(), json!(80.0))]),
    HashMap::from([("name".to_string(), json!("LCD Screen")),      ("sku".to_string(), json!("SCR-LCD")),   ("cost".to_string(), json!(20.0))]),
];

let part_vids = run!(db.bulk_insert_vertices("Part", part_props)).unwrap();
let (res10k, cap100uf, mbx1, scr_oled, bat4000, proc_arm, scr_lcd) =
    (part_vids[0], part_vids[1], part_vids[2], part_vids[3], part_vids[4], part_vids[5], part_vids[6]);

// 3 suppliers
let sup_props = vec![
    HashMap::from([("name".to_string(), json!("ResistorWorld"))]),
    HashMap::from([("name".to_string(), json!("ScreenTech"))]),
    HashMap::from([("name".to_string(), json!("CoreComponents"))]),
];
let sup_vids = run!(db.bulk_insert_vertices("Supplier", sup_props)).unwrap();
let (resistor_world, screen_tech, core_components) = (sup_vids[0], sup_vids[1], sup_vids[2]);

// 2 products
let prod_props = vec![
    HashMap::from([("name".to_string(), json!("Smartphone X")), ("price".to_string(), json!(599.0))]),
    HashMap::from([("name".to_string(), json!("TabletPro 10")), ("price".to_string(), json!(799.0))]),
];
let prod_vids = run!(db.bulk_insert_vertices("Product", prod_props)).unwrap();
let (smartphone, tablet) = (prod_vids[0], prod_vids[1]);

// Smartphone X assembly
run!(db.bulk_insert_edges("ASSEMBLED_FROM", vec![
    (smartphone, mbx1,     HashMap::new()),
    (smartphone, scr_oled, HashMap::new()),
    (smartphone, bat4000,  HashMap::new()),
    (smartphone, proc_arm, HashMap::new()),
    (mbx1,       res10k,   HashMap::new()),
    (mbx1,       cap100uf, HashMap::new()),
])).unwrap();

// TabletPro 10 assembly
run!(db.bulk_insert_edges("ASSEMBLED_FROM", vec![
    (tablet, mbx1,    HashMap::new()),
    (tablet, scr_lcd, HashMap::new()),
    (tablet, bat4000, HashMap::new()),
    (tablet, proc_arm, HashMap::new()),
])).unwrap();

// Supply relationships
run!(db.bulk_insert_edges("SUPPLIED_BY", vec![
    (res10k,   resistor_world,  HashMap::new()),
    (cap100uf, resistor_world,  HashMap::new()),
    (scr_oled, screen_tech,     HashMap::new()),
    (scr_lcd,  screen_tech,     HashMap::new()),
    (mbx1,     core_components, HashMap::new()),
    (bat4000,  core_components, HashMap::new()),
    (proc_arm, core_components, HashMap::new()),
])).unwrap();

run!(db.flush()).unwrap();
println!("Data ingested and flushed");"""
        ),
        md_cell(
            [
                "## 3. BOM Explosion",
                "",
                "Which products are affected if RES-10K is defective? Traverses the assembly hierarchy upward.",
            ]
        ),
        code_cell(
            """let query = r#"
    MATCH (defective:Part {sku: 'RES-10K'})
    MATCH (product:Product)-[:ASSEMBLED_FROM*1..5]->(defective)
    RETURN product.name AS name, product.price AS price
    ORDER BY product.price DESC
"#;

let results = run!(db.query(query)).unwrap();
println!("Products affected by defective RES-10K:");
for row in &results.rows {
    println!("  {:?}", row);
}
assert!(results.rows.len() == 2, "Expected 2 affected products, got {}", results.rows.len());"""
        ),
        md_cell(
            [
                "## 4. Full BOM Listing",
                "",
                "Every part in Smartphone X with its cost, ordered by cost descending.",
            ]
        ),
        code_cell(
            """let query_parts = r#"
    MATCH (p:Product {name: 'Smartphone X'})-[:ASSEMBLED_FROM*1..5]->(part:Part)
    RETURN part.name AS part_name, part.sku AS sku, part.cost AS cost
    ORDER BY cost DESC
"#;

let results = run!(db.query(query_parts)).unwrap();
println!("Smartphone X BOM:");
for row in &results.rows {
    println!("  {:?}", row);
}"""
        ),
        md_cell(
            [
                "## 5. Cost Rollup",
                "",
                "Total BOM cost per product — GROUP BY product with SUM of part costs.",
            ]
        ),
        code_cell(
            """let query_rollup = r#"
    MATCH (p:Product)-[:ASSEMBLED_FROM*1..5]->(part:Part)
    RETURN p.name AS product, SUM(part.cost) AS total_bom_cost
    ORDER BY total_bom_cost DESC
"#;

let results = run!(db.query(query_rollup)).unwrap();
println!("BOM cost rollup per product:");
for row in &results.rows {
    println!("  {:?}", row);
}
assert!(results.rows.len() == 2, "Expected 2 rows, got {}", results.rows.len());"""
        ),
        md_cell(
            [
                "## 6. Supply Chain Risk",
                "",
                "Which supplier is critical to the most products?",
            ]
        ),
        code_cell(
            """let query_risk = r#"
    MATCH (p:Product)-[:ASSEMBLED_FROM*1..5]->(part:Part)-[:SUPPLIED_BY]->(s:Supplier)
    RETURN s.name AS supplier, COUNT(DISTINCT p) AS products_at_risk
    ORDER BY products_at_risk DESC
"#;

let results = run!(db.query(query_risk)).unwrap();
println!("Supplier risk analysis:");
for row in &results.rows {
    println!("  {:?}", row);
}
// CoreComponents supplies mbx1, bat4000, proc_arm — used by both products
println!("Top supplier: {:?}", results.rows[0]);"""
        ),
    ]
)


# =============================================================================
# 2. Recommendation Engine
# =============================================================================
recommendation_nb = create_notebook(
    [
        md_cell(
            [
                "# Recommendation Engine (Rust)",
                "",
                "Collaborative filtering via graph traversal combined with semantic vector search for book recommendations.",
            ]
        ),
        code_cell(common_deps),
        code_cell(common_imports),
        code_cell(db_setup("recommendation")),
        md_cell(
            [
                "## 1. Schema",
                "",
                "Books with 4D semantic embeddings (L2 metric); users linked via PURCHASED edges.",
            ]
        ),
        code_cell(
            """run!(async {
    db.schema()
        .label("User")
            .property("name", DataType::String)
        .label("Book")
            .property("name",      DataType::String)
            .property("genre",     DataType::String)
            .property("embedding", DataType::Vector { dimensions: 4 })
            .index("embedding", IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::L2,
            }))
        .edge_type("PURCHASED", &["User"], &["Book"])
        .apply()
        .await
}).unwrap();

println!("Schema created");"""
        ),
        md_cell("## 2. Ingest Data"),
        code_cell(
            """// 4D embeddings: [tech, fiction, history, science]
let books = vec![
    HashMap::from([
        ("name".to_string(),      json!("Clean Code")),
        ("genre".to_string(),     json!("tech")),
        ("embedding".to_string(), json!([0.95, 0.05, 0.0,  0.0 ])),
    ]),
    HashMap::from([
        ("name".to_string(),      json!("The Pragmatic Programmer")),
        ("genre".to_string(),     json!("tech")),
        ("embedding".to_string(), json!([0.90, 0.10, 0.0,  0.0 ])),
    ]),
    HashMap::from([
        ("name".to_string(),      json!("Designing Data-Intensive Apps")),
        ("genre".to_string(),     json!("tech")),
        ("embedding".to_string(), json!([0.85, 0.0,  0.0,  0.15])),
    ]),
    HashMap::from([
        ("name".to_string(),      json!("Dune")),
        ("genre".to_string(),     json!("fiction")),
        ("embedding".to_string(), json!([0.0,  0.95, 0.0,  0.05])),
    ]),
    HashMap::from([
        ("name".to_string(),      json!("Foundation")),
        ("genre".to_string(),     json!("fiction")),
        ("embedding".to_string(), json!([0.0,  0.85, 0.0,  0.15])),
    ]),
    HashMap::from([
        ("name".to_string(),      json!("Sapiens")),
        ("genre".to_string(),     json!("history")),
        ("embedding".to_string(), json!([0.0,  0.05, 0.7,  0.25])),
    ]),
];

let book_vids = run!(db.bulk_insert_vertices("Book", books)).unwrap();
let (clean_code, pragmatic, ddia, dune, foundation, sapiens) =
    (book_vids[0], book_vids[1], book_vids[2], book_vids[3], book_vids[4], book_vids[5]);

// 4 users
let users = vec![
    HashMap::from([("name".to_string(), json!("Alice"))]),
    HashMap::from([("name".to_string(), json!("Bob"))]),
    HashMap::from([("name".to_string(), json!("Carol"))]),
    HashMap::from([("name".to_string(), json!("Dave"))]),
];
let user_vids = run!(db.bulk_insert_vertices("User", users)).unwrap();
let (alice, bob, carol, dave) = (user_vids[0], user_vids[1], user_vids[2], user_vids[3]);

// Purchase history
run!(db.bulk_insert_edges("PURCHASED", vec![
    (alice, clean_code,  HashMap::new()),
    (alice, pragmatic,   HashMap::new()),
    (bob,   clean_code,  HashMap::new()),
    (bob,   dune,        HashMap::new()),
    (carol, pragmatic,   HashMap::new()),
    (carol, foundation,  HashMap::new()),
    (dave,  dune,        HashMap::new()),
    (dave,  foundation,  HashMap::new()),
    (dave,  sapiens,     HashMap::new()),
])).unwrap();

run!(db.flush()).unwrap();
println!("Data ingested");"""
        ),
        md_cell(
            [
                "## 3. Collaborative Filtering",
                "",
                "Books that users-who-bought-Alice's-books also bought (that Alice hasn't read).",
            ]
        ),
        code_cell(
            """let query_collab = r#"
    MATCH (alice:User {name: 'Alice'})-[:PURCHASED]->(b:Book)<-[:PURCHASED]-(other:User)
    WHERE other._vid <> alice._vid
    MATCH (other)-[:PURCHASED]->(rec:Book)
    WHERE NOT (alice)-[:PURCHASED]->(rec)
    RETURN rec.name AS recommendation, COUNT(DISTINCT other) AS buyers
    ORDER BY buyers DESC
"#;

let results = run!(db.query(query_collab)).unwrap();
println!("Collaborative recommendations for Alice:");
for row in &results.rows {
    println!("  {:?}", row);
}"""
        ),
        md_cell(
            [
                "## 4. Semantic Vector Search",
                "",
                "Find the 3 books most similar to a 'tech' query vector using `CALL uni.vector.query`.",
            ]
        ),
        code_cell(
            """let query_vec = r#"
    CALL uni.vector.query('Book', 'embedding', [0.95, 0.05, 0.0, 0.0], 3)
    YIELD node, distance
    RETURN node.name AS title, node.genre AS genre, distance
    ORDER BY distance
"#;

let results = run!(db.query(query_vec)).unwrap();
println!("Top 3 books semantically similar to tech query:");
for row in &results.rows {
    println!("  {:?}", row);
}
// All 3 results should be tech books
assert!(results.rows.len() == 3, "Expected 3 results, got {}", results.rows.len());"""
        ),
        md_cell(
            [
                "## 5. Hybrid: Vector + Graph",
                "",
                "Vector search for fiction books, then find which users bought them.",
            ]
        ),
        code_cell(
            """let query_hybrid = r#"
    CALL uni.vector.query('Book', 'embedding', [0.0, 0.95, 0.0, 0.05], 3)
    YIELD node, distance
    MATCH (u:User)-[:PURCHASED]->(node)
    RETURN node.name AS book, u.name AS buyer, distance
    ORDER BY distance, buyer
"#;

let results = run!(db.query(query_hybrid)).unwrap();
println!("Fiction book buyers (via vector + graph):");
for row in &results.rows {
    println!("  {:?}", row);
}"""
        ),
        md_cell(
            [
                "## 6. Discovery: Popular Books Alice Hasn't Read",
                "",
                "Books Alice hasn't bought, ranked by how many users bought them.",
            ]
        ),
        code_cell(
            """let query_discovery = r#"
    MATCH (alice:User {name: 'Alice'})
    MATCH (u:User)-[:PURCHASED]->(b:Book)
    WHERE NOT (alice)-[:PURCHASED]->(b) AND u._vid <> alice._vid
    RETURN b.name AS book, COUNT(DISTINCT u) AS buyers
    ORDER BY buyers DESC
"#;

let results = run!(db.query(query_discovery)).unwrap();
println!("Popular books Alice has not read:");
for row in &results.rows {
    println!("  {:?}", row);
}"""
        ),
    ]
)


# =============================================================================
# 3. RAG (Retrieval-Augmented Generation)
# =============================================================================
rag_nb = create_notebook(
    [
        md_cell(
            [
                "# Retrieval-Augmented Generation (RAG) with Uni (Rust)",
                "",
                "Combining vector search with knowledge graph traversal for hybrid retrieval over Python web framework documentation.",
            ]
        ),
        code_cell(common_deps),
        code_cell(common_imports),
        code_cell(db_setup("rag")),
        md_cell(
            [
                "## 1. Schema",
                "",
                "Chunks of text with embeddings, linked to named Entities via MENTIONS edges.",
            ]
        ),
        code_cell(
            """run!(async {
    db.schema()
        .label("Chunk")
            .property("chunk_id",  DataType::String)
            .property("text",      DataType::String)
            .property("embedding", DataType::Vector { dimensions: 4 })
            .index("embedding", IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::L2,
            }))
        .label("Entity")
            .property("name", DataType::String)
            .property("type", DataType::String)
        .edge_type("MENTIONS", &["Chunk"], &["Entity"])
        .apply()
        .await
}).unwrap();

println!("RAG schema created");"""
        ),
        md_cell("## 2. Ingest Data"),
        code_cell(
            """// 4D embeddings: [auth, routing, database, testing]
let chunks = vec![
    HashMap::from([("chunk_id".to_string(), json!("c1")), ("text".to_string(), json!("JWT tokens issued by /auth/login endpoint. Tokens expire after 1 hour.")),        ("embedding".to_string(), json!([1.0,  0.0,  0.0,  0.0 ]))]),
    HashMap::from([("chunk_id".to_string(), json!("c2")), ("text".to_string(), json!("Token refresh via /auth/refresh. Send expired token, receive new one.")),         ("embedding".to_string(), json!([0.95, 0.05, 0.0,  0.0 ]))]),
    HashMap::from([("chunk_id".to_string(), json!("c3")), ("text".to_string(), json!("Password hashing uses bcrypt with cost factor 12.")),                             ("embedding".to_string(), json!([0.85, 0.0,  0.0,  0.15]))]),
    HashMap::from([("chunk_id".to_string(), json!("c4")), ("text".to_string(), json!("Routes defined with @app.route decorator. Supports GET, POST, PUT, DELETE.")),   ("embedding".to_string(), json!([0.0,  1.0,  0.0,  0.0 ]))]),
    HashMap::from([("chunk_id".to_string(), json!("c5")), ("text".to_string(), json!("Middleware intercepts requests before handlers. Register with app.use().")),      ("embedding".to_string(), json!([0.05, 0.9,  0.05, 0.0 ]))]),
    HashMap::from([("chunk_id".to_string(), json!("c6")), ("text".to_string(), json!("ConnectionPool manages DB connections. Max pool size defaults to 10.")),          ("embedding".to_string(), json!([0.0,  0.0,  1.0,  0.0 ]))]),
    HashMap::from([("chunk_id".to_string(), json!("c7")), ("text".to_string(), json!("ORM models inherit from BaseModel. Columns map to database fields.")),            ("embedding".to_string(), json!([0.0,  0.1,  0.9,  0.0 ]))]),
    HashMap::from([("chunk_id".to_string(), json!("c8")), ("text".to_string(), json!("TestClient simulates HTTP requests without starting a server.")),                 ("embedding".to_string(), json!([0.0,  0.2,  0.0,  0.8 ]))]),
];

let chunk_vids = run!(db.bulk_insert_vertices("Chunk", chunks)).unwrap();
let (c1, c2, c3, c4, c5, c6, c7, c8) =
    (chunk_vids[0], chunk_vids[1], chunk_vids[2], chunk_vids[3],
     chunk_vids[4], chunk_vids[5], chunk_vids[6], chunk_vids[7]);

// 6 entities
let entities = vec![
    HashMap::from([("name".to_string(), json!("JWT")),            ("type".to_string(), json!("technology"))]),
    HashMap::from([("name".to_string(), json!("authentication")), ("type".to_string(), json!("concept"))]),
    HashMap::from([("name".to_string(), json!("routing")),        ("type".to_string(), json!("concept"))]),
    HashMap::from([("name".to_string(), json!("database")),       ("type".to_string(), json!("concept"))]),
    HashMap::from([("name".to_string(), json!("bcrypt")),         ("type".to_string(), json!("technology"))]),
    HashMap::from([("name".to_string(), json!("ConnectionPool")), ("type".to_string(), json!("class"))]),
];

let entity_vids = run!(db.bulk_insert_vertices("Entity", entities)).unwrap();
let (jwt, auth_entity, routing_entity, db_entity, bcrypt_entity, pool_entity) =
    (entity_vids[0], entity_vids[1], entity_vids[2], entity_vids[3], entity_vids[4], entity_vids[5]);

// MENTIONS edges
run!(db.bulk_insert_edges("MENTIONS", vec![
    (c1, jwt,            HashMap::new()),
    (c1, auth_entity,    HashMap::new()),
    (c2, jwt,            HashMap::new()),
    (c2, auth_entity,    HashMap::new()),
    (c3, bcrypt_entity,  HashMap::new()),
    (c3, auth_entity,    HashMap::new()),
    (c4, routing_entity, HashMap::new()),
    (c5, routing_entity, HashMap::new()),
    (c6, db_entity,      HashMap::new()),
    (c6, pool_entity,    HashMap::new()),
    (c7, db_entity,      HashMap::new()),
])).unwrap();

run!(db.flush()).unwrap();
println!("RAG data ingested");"""
        ),
        md_cell(
            [
                "## 3. Pure Vector Search",
                "",
                "Find the 3 chunks most similar to an authentication query.",
            ]
        ),
        code_cell(
            """let query_vec = r#"
    CALL uni.vector.query('Chunk', 'embedding', [1.0, 0.0, 0.0, 0.0], 3)
    YIELD node, distance
    RETURN node.chunk_id AS chunk_id, node.text AS text, distance
    ORDER BY distance
"#;

let results = run!(db.query(query_vec)).unwrap();
println!("Top 3 chunks for auth query:");
for row in &results.rows {
    println!("  {:?}", row);
}
// Expected: c1, c2, c3 (auth chunks)
assert!(results.rows.len() == 3, "Expected 3 results, got {}", results.rows.len());"""
        ),
        md_cell(
            [
                "## 4. Graph Expansion",
                "",
                "Same vector seeds — also show which entities each chunk mentions.",
            ]
        ),
        code_cell(
            """let query_expand = r#"
    CALL uni.vector.query('Chunk', 'embedding', [1.0, 0.0, 0.0, 0.0], 3)
    YIELD node, distance
    MATCH (node)-[:MENTIONS]->(e:Entity)
    RETURN node.chunk_id AS chunk_id, e.name AS entity, distance
    ORDER BY distance, entity
"#;

let results = run!(db.query(query_expand)).unwrap();
println!("Entities mentioned by top auth chunks:");
for row in &results.rows {
    println!("  {:?}", row);
}"""
        ),
        md_cell(
            [
                "## 5. Entity Bridging",
                "",
                "Find all chunks related to the auth seeds via shared entity mentions — the core graph RAG technique.",
            ]
        ),
        code_cell(
            """let query_bridge = r#"
    CALL uni.vector.query('Chunk', 'embedding', [1.0, 0.0, 0.0, 0.0], 3)
    YIELD node AS anchor, distance
    MATCH (anchor)-[:MENTIONS]->(e:Entity)<-[:MENTIONS]-(related:Chunk)
    WHERE related._vid <> anchor._vid
    RETURN anchor.chunk_id AS anchor_id, e.name AS bridge_entity,
           related.chunk_id AS related_id
    ORDER BY anchor_id, bridge_entity
"#;

let results = run!(db.query(query_bridge)).unwrap();
println!("Entity bridges between auth chunks:");
for row in &results.rows {
    println!("  {:?}", row);
}"""
        ),
        md_cell(
            [
                "## 6. Context Assembly",
                "",
                "Full hybrid pipeline: vector seeds + graph bridging → collect unique chunks for the LLM context window.",
            ]
        ),
        code_cell(
            """use std::collections::HashSet;

let query_ctx = r#"
    CALL uni.vector.query('Chunk', 'embedding', [1.0, 0.0, 0.0, 0.0], 3)
    YIELD node AS seed, distance
    MATCH (seed)-[:MENTIONS]->(e:Entity)<-[:MENTIONS]-(related:Chunk)
    RETURN seed.chunk_id AS seed_id, seed.text AS seed_text,
           related.chunk_id AS related_id, related.text AS related_text,
           e.name AS shared_entity
    ORDER BY seed_id, shared_entity
"#;

let results = run!(db.query(query_ctx)).unwrap();
println!("Context assembly result:");
println!("  {} rows retrieved for LLM context window", results.rows.len());
for row in &results.rows {
    println!("  {:?}", row);
}"""
        ),
    ]
)


# =============================================================================
# 4. Fraud Detection
# =============================================================================
fraud_nb = create_notebook(
    [
        md_cell(
            [
                "# Fraud Detection with Uni (Rust)",
                "",
                "Detecting money laundering rings (3-cycles) and shared device anomalies using Uni's native Rust API.",
            ]
        ),
        code_cell(common_deps),
        code_cell(common_imports),
        code_cell(db_setup("fraud")),
        md_cell("## 1. Schema"),
        code_cell(
            """run!(async {
    db.schema()
        .label("User")
            .property("name",  DataType::String)
            .property("email", DataType::String)
            .property_nullable("risk_score", DataType::Float32)
        .label("Device")
            .property("device_id", DataType::String)
        .edge_type("SENT_MONEY", &["User"], &["User"])
            .property("amount", DataType::Float64)
        .edge_type("USED_DEVICE", &["User"], &["Device"])
        .apply()
        .await
}).unwrap();

println!("Fraud detection schema created");"""
        ),
        md_cell(
            [
                "## 2. Ingestion",
                "",
                "5 named users, 3 devices, a money ring, and suspicious cross-device links.",
            ]
        ),
        code_cell(
            """// 5 users: 3 in a ring, 2 high-risk fraudsters
let users = vec![
    HashMap::from([("name".to_string(), json!("Alice")),  ("email".to_string(), json!("alice@example.com")),  ("risk_score".to_string(), json!(0.10))]),
    HashMap::from([("name".to_string(), json!("Bob")),    ("email".to_string(), json!("bob@example.com")),    ("risk_score".to_string(), json!(0.15))]),
    HashMap::from([("name".to_string(), json!("Carlos")), ("email".to_string(), json!("carlos@example.com")), ("risk_score".to_string(), json!(0.20))]),
    HashMap::from([("name".to_string(), json!("Dana")),   ("email".to_string(), json!("dana@example.com")),   ("risk_score".to_string(), json!(0.92))]),
    HashMap::from([("name".to_string(), json!("Eve")),    ("email".to_string(), json!("eve@example.com")),    ("risk_score".to_string(), json!(0.88))]),
];

let user_vids = run!(db.bulk_insert_vertices("User", users)).unwrap();
let (alice, bob, carlos, dana, eve) =
    (user_vids[0], user_vids[1], user_vids[2], user_vids[3], user_vids[4]);

// 3 devices
let devices = vec![
    HashMap::from([("device_id".to_string(), json!("device_A"))]),
    HashMap::from([("device_id".to_string(), json!("device_B"))]),
    HashMap::from([("device_id".to_string(), json!("device_C"))]),
];
let device_vids = run!(db.bulk_insert_vertices("Device", devices)).unwrap();
let (device_a, device_b, device_c) = (device_vids[0], device_vids[1], device_vids[2]);

// Money ring: Alice -> Bob -> Carlos -> Alice
run!(db.bulk_insert_edges("SENT_MONEY", vec![
    (alice,  bob,    HashMap::from([("amount".to_string(), json!(9500.0))])),
    (bob,    carlos, HashMap::from([("amount".to_string(), json!(9000.0))])),
    (carlos, alice,  HashMap::from([("amount".to_string(), json!(8750.0))])),
    (dana,   eve,    HashMap::from([("amount".to_string(), json!(15000.0))])),  // Suspicious
])).unwrap();

// Device sharing: Alice+Dana on device_A, Bob+Eve on device_B, Carlos alone on device_C
run!(db.bulk_insert_edges("USED_DEVICE", vec![
    (alice,  device_a, HashMap::new()),
    (dana,   device_a, HashMap::new()),
    (bob,    device_b, HashMap::new()),
    (eve,    device_b, HashMap::new()),
    (carlos, device_c, HashMap::new()),
])).unwrap();

run!(db.flush()).unwrap();
println!("Fraud data ingested");"""
        ),
        md_cell(
            [
                "## 3. Ring Detection",
                "",
                "Find 3-cycles in the money transfer graph. Deduplication: `a._vid < b._vid AND a._vid < c._vid`.",
            ]
        ),
        code_cell(
            """let query_ring = r#"
    MATCH (a:User)-[:SENT_MONEY]->(b:User)-[:SENT_MONEY]->(c:User)-[:SENT_MONEY]->(a)
    WHERE a._vid < b._vid AND a._vid < c._vid
    RETURN a.name AS user_a, b.name AS user_b, c.name AS user_c,
           COUNT(*) AS rings
"#;

let results = run!(db.query(query_ring)).unwrap();
println!("Money laundering rings detected:");
for row in &results.rows {
    println!("  {:?}", row);
}
assert!(results.rows.len() == 1, "Expected 1 ring, got {}", results.rows.len());"""
        ),
        md_cell(
            [
                "## 4. Ring with Transfer Amounts",
                "",
                "Same pattern, but also retrieve edge properties to show total cycled money.",
            ]
        ),
        code_cell(
            """let query_amounts = r#"
    MATCH (a:User)-[r1:SENT_MONEY]->(b:User)-[r2:SENT_MONEY]->(c:User)-[r3:SENT_MONEY]->(a)
    WHERE a._vid < b._vid AND a._vid < c._vid
    RETURN a.name AS user_a, b.name AS user_b, c.name AS user_c,
           r1.amount AS leg1, r2.amount AS leg2, r3.amount AS leg3,
           r1.amount + r2.amount + r3.amount AS total_cycled
"#;

let results = run!(db.query(query_amounts)).unwrap();
println!("Ring with transfer amounts:");
for row in &results.rows {
    println!("  {:?}", row);
}"""
        ),
        md_cell(
            [
                "## 5. Shared Device Risk",
                "",
                "Find users who share a device with a high-risk user (risk > 0.8). Carlos should NOT appear.",
            ]
        ),
        code_cell(
            """let query_shared = r#"
    MATCH (u:User)-[:USED_DEVICE]->(d:Device)<-[:USED_DEVICE]-(fraudster:User)
    WHERE fraudster.risk_score > 0.8 AND u._vid <> fraudster._vid
    RETURN u.name AS user, d.device_id AS device, fraudster.name AS flagged_contact
    ORDER BY user
"#;

let results = run!(db.query(query_shared)).unwrap();
println!("Users sharing device with high-risk account:");
for row in &results.rows {
    println!("  {:?}", row);
}
// Carlos should not appear - he only uses device_C alone
println!("Note: Carlos uses device_C alone and should not appear above");"""
        ),
        md_cell(
            [
                "## 6. Combined Alert: Ring + Device Sharing",
                "",
                "Users appearing in BOTH a money ring AND sharing a device with a fraudster.",
            ]
        ),
        code_cell(
            """let query_combined = r#"
    MATCH (a:User)-[:SENT_MONEY]->(b:User)-[:SENT_MONEY]->(c:User)-[:SENT_MONEY]->(a)
    WHERE a._vid < b._vid AND a._vid < c._vid
    MATCH (a)-[:USED_DEVICE]->(d:Device)<-[:USED_DEVICE]-(fraudster:User)
    WHERE fraudster.risk_score > 0.8
    RETURN DISTINCT a.name AS high_priority_user
"#;

let results = run!(db.query(query_combined)).unwrap();
println!("HIGH PRIORITY targets (ring + device-sharing):");
for row in &results.rows {
    println!("  {:?}", row);
}
// Alice is in the ring AND shares device_A with Dana (high risk)
assert!(!results.rows.is_empty(), "Expected at least one combined alert");"""
        ),
    ]
)


# =============================================================================
# 5. Sales Analytics
# =============================================================================
sales_nb = create_notebook(
    [
        md_cell(
            [
                "# Regional Sales Analytics with Uni (Rust)",
                "",
                "Combining graph traversal with columnar aggregation across multiple regions and product categories.",
            ]
        ),
        code_cell(common_deps),
        code_cell(common_imports),
        code_cell(db_setup("sales")),
        md_cell("## 1. Schema"),
        code_cell(
            """run!(async {
    db.schema()
        .label("Region")
            .property("name", DataType::String)
        .label("Category")
            .property("name", DataType::String)
        .label("Order")
            .property("amount", DataType::Float64)
        .edge_type("SHIPPED_TO",  &["Order"], &["Region"])
        .edge_type("IN_CATEGORY", &["Order"], &["Category"])
        .apply()
        .await
}).unwrap();

println!("Sales analytics schema created");"""
        ),
        md_cell(["## 2. Ingest Data", "", "4 regions, 3 categories, 27 orders distributed non-uniformly."]),
        code_cell(
            """// Regions
let region_props = vec![
    HashMap::from([("name".to_string(), json!("North"))]),
    HashMap::from([("name".to_string(), json!("South"))]),
    HashMap::from([("name".to_string(), json!("East"))]),
    HashMap::from([("name".to_string(), json!("West"))]),
];
let region_vids = run!(db.bulk_insert_vertices("Region", region_props)).unwrap();
let (north, south, east, west) = (region_vids[0], region_vids[1], region_vids[2], region_vids[3]);

// Categories
let cat_props = vec![
    HashMap::from([("name".to_string(), json!("Electronics"))]),
    HashMap::from([("name".to_string(), json!("Apparel"))]),
    HashMap::from([("name".to_string(), json!("Home & Garden"))]),
];
let cat_vids = run!(db.bulk_insert_vertices("Category", cat_props)).unwrap();
let (electronics, apparel, home_garden) = (cat_vids[0], cat_vids[1], cat_vids[2]);

// Orders: (amount, region_vid, category_vid)
let orders_raw = vec![
    // East: high-value electronics
    (1200.0, east, electronics), (980.0, east, electronics),
    (450.0,  east, apparel),     (120.0, east, apparel),
    (85.0,   east, apparel),     (60.0,  east, home_garden),
    // West: strong Home & Garden
    (890.0, west, home_garden), (620.0, west, home_garden),
    (450.0, west, home_garden), (340.0, west, home_garden),
    (500.0, west, electronics), (210.0, west, apparel),
    (180.0, west, apparel),
    // South: most electronics
    (750.0, south, electronics), (680.0, south, electronics),
    (590.0, south, electronics), (520.0, south, electronics),
    (480.0, south, electronics), (300.0, south, apparel),
    (250.0, south, home_garden),
    // North: balanced mix
    (400.0, north, electronics), (350.0, north, electronics),
    (280.0, north, apparel),     (260.0, north, apparel),
    (240.0, north, apparel),     (320.0, north, home_garden),
    (290.0, north, home_garden),
];

let order_props: Vec<_> = orders_raw.iter()
    .map(|(amt, _, _)| HashMap::from([("amount".to_string(), json!(*amt))]))
    .collect();

let order_vids = run!(db.bulk_insert_vertices("Order", order_props)).unwrap();

let shipped_edges: Vec<_> = order_vids.iter().zip(orders_raw.iter())
    .map(|(&ov, &(_, rv, _))| (ov, rv, HashMap::new()))
    .collect();
run!(db.bulk_insert_edges("SHIPPED_TO", shipped_edges)).unwrap();

let category_edges: Vec<_> = order_vids.iter().zip(orders_raw.iter())
    .map(|(&ov, &(_, _, cv))| (ov, cv, HashMap::new()))
    .collect();
run!(db.bulk_insert_edges("IN_CATEGORY", category_edges)).unwrap();

run!(db.flush()).unwrap();
println!("Inserted {} orders", order_vids.len());"""
        ),
        md_cell(
            [
                "## 3. Revenue by Region",
                "",
                "Total revenue and order count per region, ordered by revenue.",
            ]
        ),
        code_cell(
            """let query_region = r#"
    MATCH (r:Region)<-[:SHIPPED_TO]-(o:Order)
    RETURN r.name AS region, COUNT(o) AS order_count, SUM(o.amount) AS total_revenue
    ORDER BY total_revenue DESC
"#;

let results = run!(db.query(query_region)).unwrap();
println!("Revenue by region:");
for row in &results.rows {
    println!("  {:?}", row);
}
assert!(results.rows.len() == 4, "Expected 4 regions, got {}", results.rows.len());"""
        ),
        md_cell(
            [
                "## 4. Region \u00d7 Category Breakdown",
                "",
                "12-row breakdown showing revenue for every region/category pair.",
            ]
        ),
        code_cell(
            """let query_breakdown = r#"
    MATCH (r:Region)<-[:SHIPPED_TO]-(o:Order)-[:IN_CATEGORY]->(c:Category)
    RETURN r.name AS region, c.name AS category,
           COUNT(o) AS orders, SUM(o.amount) AS revenue
    ORDER BY region, revenue DESC
"#;

let results = run!(db.query(query_breakdown)).unwrap();
println!("Region x Category breakdown ({} rows):", results.rows.len());
for row in &results.rows {
    println!("  {:?}", row);
}
assert!(results.rows.len() == 12, "Expected 12 rows (4x3), got {}", results.rows.len());"""
        ),
        md_cell(
            [
                "## 5. Top Orders per Region",
                "",
                "Highest-value orders in each region.",
            ]
        ),
        code_cell(
            """let query_top = r#"
    MATCH (r:Region)<-[:SHIPPED_TO]-(o:Order)
    RETURN r.name AS region, o.amount AS amount
    ORDER BY region, amount DESC
"#;

let results = run!(db.query(query_top)).unwrap();
println!("All orders by region (DESC):");
println!("  {} total order rows", results.rows.len());
// East should lead with $1200
println!("  First row (should be East $1200): {:?}", results.rows.first());"""
        ),
        md_cell(
            [
                "## 6. Best Category per Region",
                "",
                "The highest-revenue category for each region.",
            ]
        ),
        code_cell(
            """let query_best = r#"
    MATCH (r:Region)<-[:SHIPPED_TO]-(o:Order)-[:IN_CATEGORY]->(c:Category)
    RETURN r.name AS region, c.name AS category, SUM(o.amount) AS revenue
    ORDER BY region, revenue DESC
"#;

let results = run!(db.query(query_best)).unwrap();
println!("Best category per region (first result per region):");
let mut seen_regions = std::collections::HashSet::new();
for row in &results.rows {
    // rows are sorted by region, revenue DESC — take first per region
    if let Some(region_val) = row.values.first() {
        let region_str = format!("{:?}", region_val);
        if seen_regions.insert(region_str.clone()) {
            println!("  {}: {:?}", region_str, row);
        }
    }
}
// Each region has a different best category (electronics, home & garden, etc.)"""
        ),
    ]
)


# Write notebooks
script_dir = os.path.dirname(os.path.abspath(__file__))

notebooks = [
    ("supply_chain.ipynb", supply_chain_nb),
    ("recommendation.ipynb", recommendation_nb),
    ("rag.ipynb", rag_nb),
    ("fraud_detection.ipynb", fraud_nb),
    ("sales_analytics.ipynb", sales_nb),
]

for filename, nb in notebooks:
    path = os.path.join(script_dir, filename)
    with open(path, "w") as f:
        json.dump(nb, f, indent=2)
    print(f"Created {filename}")

print("\nAll Rust notebooks created successfully!")
print("\nTo use these notebooks:")
print("1. Install evcxr_jupyter: cargo install evcxr_jupyter && evcxr_jupyter --install")
print("2. Run: jupyter notebook")
print("3. Open any .ipynb file and select the Rust kernel")
