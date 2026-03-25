#!/usr/bin/env python3
"""Generate example Jupyter notebooks for uni-pydantic."""

import json
import os
import uuid


def generate_cell_id():
    """Generate a unique cell ID."""
    return str(uuid.uuid4()).replace("-", "")[:32]


def create_notebook(cells):
    """Create a notebook structure with cells."""
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
    """Create a markdown cell."""
    return {
        "id": generate_cell_id(),
        "cell_type": "markdown",
        "metadata": {},
        "source": [line + "\n" for line in source],
    }


def code_cell(source):
    """Create a code cell."""
    return {
        "id": generate_cell_id(),
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [line + "\n" for line in source],
    }


# Common imports for all pydantic notebooks
common_imports = [
    "import os",
    "import shutil",
    "import tempfile",
    "",
    "import uni_db",
    "from uni_pydantic import UniNode, UniEdge, UniSession, Field, Relationship",
]

common_imports_with_vector = [
    "import os",
    "import shutil",
    "import tempfile",
    "",
    "import uni_db",
    "from uni_pydantic import UniNode, UniEdge, UniSession, Field, Relationship, Vector",
]


def db_setup(name):
    """Generate database setup code."""
    return [
        f'db_path = os.path.join(tempfile.gettempdir(), "{name}_pydantic_db")',
        "if os.path.exists(db_path):",
        "    shutil.rmtree(db_path)",
        "db = uni_db.Database.open(db_path)",
        "",
        "# Create session and register models",
        "session = UniSession(db)",
    ]


# =============================================================================
# 1. Supply Chain Notebook
# =============================================================================
supply_chain_nb = create_notebook(
    [
        md_cell(
            [
                "# Supply Chain Management with uni-pydantic",
                "",
                "BOM explosion, cost rollup, and supplier risk analysis using Pydantic models.",
            ]
        ),
        code_cell(common_imports),
        md_cell(
            [
                "## 1. Define Models",
                "",
                "Parts, Suppliers, and Products with assembly relationships using type-safe Pydantic models.",
            ]
        ),
        code_cell(
            [
                "class Part(UniNode):",
                '    """A component part in the supply chain."""',
                '    __label__ = "Part"',
                "    ",
                "    name: str",
                '    sku: str = Field(index="hash", unique=True)',
                "    cost: float",
                "    ",
                "    # Relationships",
                '    used_in: list["Part"] = Relationship("ASSEMBLED_FROM", direction="incoming")',
                '    components: list["Part"] = Relationship("ASSEMBLED_FROM", direction="outgoing")',
                '    suppliers: list["Supplier"] = Relationship("SUPPLIED_BY", direction="outgoing")',
                "",
                "",
                "class Supplier(UniNode):",
                '    """A supplier of parts."""',
                '    __label__ = "Supplier"',
                "    ",
                '    name: str = Field(index="btree")',
                "    ",
                "    # Relationships",
                '    supplies: list[Part] = Relationship("SUPPLIED_BY", direction="incoming")',
                "",
                "",
                "class Product(UniNode):",
                '    """A finished product assembled from parts."""',
                '    __label__ = "Product"',
                "    ",
                '    name: str = Field(index="btree")',
                "    price: float",
                "    ",
                "    # Relationships",
                '    components: list[Part] = Relationship("ASSEMBLED_FROM", direction="outgoing")',
                "",
                "",
                "class AssembledFrom(UniEdge):",
                '    """Edge representing assembly relationship."""',
                '    __edge_type__ = "ASSEMBLED_FROM"',
                "    __from__ = (Product, Part)",
                "    __to__ = Part",
                "",
                "",
                "class SuppliedBy(UniEdge):",
                '    """Edge representing supplier relationship."""',
                '    __edge_type__ = "SUPPLIED_BY"',
                "    __from__ = Part",
                "    __to__ = Supplier",
            ]
        ),
        md_cell(["## 2. Setup Database and Session"]),
        code_cell(
            db_setup("supply_chain")
            + [
                "session.register(Part, Supplier, Product, AssembledFrom, SuppliedBy)",
                "session.sync_schema()",
                "",
                'print(f"Opened database at {db_path}")',
            ]
        ),
        md_cell(
            [
                "## 3. Create Data",
                "",
                "Two products sharing common parts, supplied by multiple vendors.",
            ]
        ),
        code_cell(
            [
                "# 7 parts: resistors, capacitors, boards, screens, battery, processor",
                'res10k   = Part(name="Resistor 10K",    sku="RES-10K",   cost=0.05)',
                'cap100uf = Part(name="Capacitor 100uF", sku="CAP-100UF", cost=0.08)',
                'mbx1     = Part(name="Motherboard X1",  sku="MB-X1",     cost=50.0)',
                'scr_oled = Part(name="OLED Screen",     sku="SCR-OLED",  cost=30.0)',
                'bat4000  = Part(name="Battery 4000mAh", sku="BAT-4000",  cost=15.0)',
                'proc_arm = Part(name="ARM Processor",   sku="PROC-ARM",  cost=80.0)',
                'scr_lcd  = Part(name="LCD Screen",      sku="SCR-LCD",   cost=20.0)',
                "",
                "# 3 suppliers",
                'resistor_world  = Supplier(name="ResistorWorld")',
                'screen_tech     = Supplier(name="ScreenTech")',
                'core_components = Supplier(name="CoreComponents")',
                "",
                "# 2 products",
                'smartphone = Product(name="Smartphone X",  price=599.0)',
                'tablet     = Product(name="TabletPro 10",  price=799.0)',
                "",
                "session.add_all([",
                "    res10k, cap100uf, mbx1, scr_oled, bat4000, proc_arm, scr_lcd,",
                "    resistor_world, screen_tech, core_components,",
                "    smartphone, tablet,",
                "])",
                "session.commit()",
                'print("Nodes created")',
            ]
        ),
        code_cell(
            [
                "# Smartphone X assembly",
                'session.create_edge(smartphone, "ASSEMBLED_FROM", mbx1)',
                'session.create_edge(smartphone, "ASSEMBLED_FROM", scr_oled)',
                'session.create_edge(smartphone, "ASSEMBLED_FROM", bat4000)',
                'session.create_edge(smartphone, "ASSEMBLED_FROM", proc_arm)',
                'session.create_edge(mbx1, "ASSEMBLED_FROM", res10k)',
                'session.create_edge(mbx1, "ASSEMBLED_FROM", cap100uf)',
                "",
                "# TabletPro 10 assembly (shares mbx1, bat4000, proc_arm)",
                'session.create_edge(tablet, "ASSEMBLED_FROM", mbx1)',
                'session.create_edge(tablet, "ASSEMBLED_FROM", scr_lcd)',
                'session.create_edge(tablet, "ASSEMBLED_FROM", bat4000)',
                'session.create_edge(tablet, "ASSEMBLED_FROM", proc_arm)',
                "",
                "# Supply relationships",
                'session.create_edge(res10k,   "SUPPLIED_BY", resistor_world)',
                'session.create_edge(cap100uf, "SUPPLIED_BY", resistor_world)',
                'session.create_edge(scr_oled, "SUPPLIED_BY", screen_tech)',
                'session.create_edge(scr_lcd,  "SUPPLIED_BY", screen_tech)',
                'session.create_edge(mbx1,     "SUPPLIED_BY", core_components)',
                'session.create_edge(bat4000,  "SUPPLIED_BY", core_components)',
                'session.create_edge(proc_arm, "SUPPLIED_BY", core_components)',
                "",
                "session.commit()",
                'print("Data ingested")',
            ]
        ),
        md_cell(
            [
                "## 4. BOM Explosion",
                "",
                "Which products are affected if RES-10K is defective? Traverses the assembly hierarchy upward.",
            ]
        ),
        code_cell(
            [
                'query_bom = """',
                "    MATCH (defective:Part {sku: 'RES-10K'})",
                "    MATCH (product:Product)-[:ASSEMBLED_FROM*]->(defective)",
                "    RETURN product.name AS name, product.price AS price",
                "    ORDER BY product.price DESC",
                '"""',
                "results = session.cypher(query_bom)",
                "print('Products affected by defective RES-10K:')",
                "for r in results:",
                "    print(f\"  {r['name']} (${r['price']})\")",
                "assert len(results) == 2, f'Expected 2 affected products, got {len(results)}'",
            ]
        ),
        md_cell(
            [
                "> **Bounded vs Unbounded Paths**: `[*]` performs unbounded traversal",
                "> (defaults to 100 hops max), ideal for BOM explosion where you want *every*",
                "> affected product regardless of depth. Use `[*1..5]` to cap traversal",
                "> at a known depth, as shown in the queries below.",
            ]
        ),
        md_cell(
            [
                "## 5. Full BOM Listing",
                "",
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
                "results = session.cypher(query_parts)",
                "print('Smartphone X BOM:')",
                "for r in results:",
                "    print(f\"  {r['part_name']} ({r['sku']}): ${r['cost']}\")",
            ]
        ),
        md_cell(
            [
                "## 6. Cost Rollup",
                "",
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
                "results = session.cypher(query_rollup)",
                "print('BOM cost rollup per product:')",
                "for r in results:",
                "    print(f\"  {r['product']}: ${r['total_bom_cost']:.2f}\")",
                "assert len(results) == 2, f'Expected 2 rows, got {len(results)}'",
            ]
        ),
        md_cell(
            [
                "## 7. Supply Chain Risk",
                "",
                "Which supplier is critical to the most products?",
            ]
        ),
        code_cell(
            [
                'query_risk = """',
                "    MATCH (p:Product)-[:ASSEMBLED_FROM*1..5]->(part:Part)-[:SUPPLIED_BY]->(s:Supplier)",
                "    RETURN s.name AS supplier, COUNT(DISTINCT p) AS products_at_risk,",
                "           COUNT(DISTINCT part) AS parts_supplied",
                "    ORDER BY products_at_risk DESC, parts_supplied DESC",
                '"""',
                "results = session.cypher(query_risk)",
                "print('Supplier risk analysis:')",
                "for r in results:",
                "    print(f\"  {r['supplier']}: {r['products_at_risk']} product(s), {r['parts_supplied']} part(s)\")",
                "top = results[0]",
                "assert top['supplier'] == 'CoreComponents', f\"Expected CoreComponents, got {top['supplier']}\"",
                "assert top['products_at_risk'] == 2, f\"Expected 2, got {top['products_at_risk']}\"",
            ]
        ),
        md_cell(
            [
                "## 8. Query Builder Demo",
                "",
                "Using the type-safe query builder to find expensive parts.",
            ]
        ),
        code_cell(
            [
                "# Find all expensive parts using query builder",
                "expensive_parts = (",
                "    session.query(Part)",
                "    .filter(Part.cost >= 20.0)",
                "    .order_by(Part.cost, descending=True)",
                "    .all()",
                ")",
                "",
                'print("Expensive Parts (>=$20):")',
                "for part in expensive_parts:",
                '    print(f"  - {part.name} ({part.sku}): ${part.cost:,.2f}")',
            ]
        ),
    ]
)


# =============================================================================
# 2. Recommendation Notebook
# =============================================================================
rec_nb = create_notebook(
    [
        md_cell(
            [
                "# Recommendation Engine with uni-pydantic",
                "",
                "Collaborative filtering via graph traversal combined with semantic vector search for book recommendations.",
            ]
        ),
        code_cell(common_imports_with_vector),
        md_cell(
            [
                "## 1. Define Models",
                "",
                "Books with 4D semantic embeddings; users linked via PURCHASED edges.",
            ]
        ),
        code_cell(
            [
                "class User(UniNode):",
                '    """A user who purchases books."""',
                '    __label__ = "User"',
                "    ",
                "    name: str",
                "    ",
                "    # Relationships",
                '    purchased: list["Book"] = Relationship("PURCHASED", direction="outgoing")',
                "",
                "",
                "class Book(UniNode):",
                '    """A book with semantic embedding."""',
                '    __label__ = "Book"',
                "    ",
                "    name: str",
                "    genre: str",
                '    embedding: Vector[4] = Field(metric="l2")  # 4D: [tech, fiction, history, science]',
                "    ",
                "    # Relationships",
                '    purchased_by: list[User] = Relationship("PURCHASED", direction="incoming")',
                "",
                "",
                "class Purchased(UniEdge):",
                '    """Edge representing a user purchasing a book."""',
                '    __edge_type__ = "PURCHASED"',
                "    __from__ = User",
                "    __to__ = Book",
            ]
        ),
        md_cell(["## 2. Setup Database and Session"]),
        code_cell(
            db_setup("recommendation")
            + [
                "session.register(User, Book, Purchased)",
                "session.sync_schema()",
                "",
                'print(f"Opened database at {db_path}")',
            ]
        ),
        md_cell(
            [
                "## 3. Create Data",
                "",
                "6 books in 3 genre clusters, 4 users with purchase history.",
            ]
        ),
        code_cell(
            [
                "# 4D embeddings: [tech, fiction, history, science]",
                'clean_code = Book(name="Clean Code",                    genre="tech",    embedding=[0.95, 0.05, 0.0,  0.0 ])',
                'pragmatic  = Book(name="The Pragmatic Programmer",      genre="tech",    embedding=[0.90, 0.10, 0.0,  0.0 ])',
                'ddia       = Book(name="Designing Data-Intensive Apps", genre="tech",    embedding=[0.85, 0.0,  0.0,  0.15])',
                'dune       = Book(name="Dune",                          genre="fiction", embedding=[0.0,  0.95, 0.0,  0.05])',
                'foundation = Book(name="Foundation",                    genre="fiction", embedding=[0.0,  0.85, 0.0,  0.15])',
                'sapiens    = Book(name="Sapiens",                       genre="history", embedding=[0.0,  0.05, 0.7,  0.25])',
                "",
                'alice = User(name="Alice")',
                'bob   = User(name="Bob")',
                'carol = User(name="Carol")',
                'dave  = User(name="Dave")',
                "",
                "session.add_all([clean_code, pragmatic, ddia, dune, foundation, sapiens,",
                "                 alice, bob, carol, dave])",
                "session.commit()",
                "",
                "# Create vector index AFTER commit",
                'db.create_vector_index("Book", "embedding", "l2")',
                'print("Data ingested and vector index created")',
            ]
        ),
        code_cell(
            [
                "# Purchase history",
                'session.create_edge(alice, "PURCHASED", clean_code)',
                'session.create_edge(alice, "PURCHASED", pragmatic)',
                'session.create_edge(bob,   "PURCHASED", clean_code)',
                'session.create_edge(bob,   "PURCHASED", dune)',
                'session.create_edge(carol, "PURCHASED", pragmatic)',
                'session.create_edge(carol, "PURCHASED", foundation)',
                'session.create_edge(dave,  "PURCHASED", dune)',
                'session.create_edge(dave,  "PURCHASED", foundation)',
                'session.create_edge(dave,  "PURCHASED", sapiens)',
                "session.commit()",
                'print("Purchase edges created")',
            ]
        ),
        md_cell(
            [
                "## 4. Collaborative Filtering",
                "",
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
                "results = session.cypher(query_collab)",
                "print('Collaborative recommendations for Alice:')",
                "for r in results:",
                "    print(f\"  {r['recommendation']} (bought by {r['buyers']} similar user(s))\")",
            ]
        ),
        md_cell(
            [
                "## 5. Semantic Vector Search",
                "",
                "Find the 3 books most similar to a 'tech' query vector.",
            ]
        ),
        code_cell(
            [
                "tech_query = [0.95, 0.05, 0.0, 0.0]",
                "",
                'query_vec = """',
                "    CALL uni.vector.query('Book', 'embedding', $vec, 3)",
                "    YIELD node, distance",
                "    RETURN node.name AS title, node.genre AS genre, distance",
                "    ORDER BY distance",
                '"""',
                'results = session.cypher(query_vec, {"vec": tech_query})',
                "print('Top 3 books semantically similar to tech query:')",
                "for r in results:",
                "    print(f\"  [{r['distance']:.4f}] {r['title']} ({r['genre']})\")",
                "",
                "genres = [r['genre'] for r in results]",
                "assert all(g == 'tech' for g in genres), f'Expected all tech, got {genres}'",
            ]
        ),
        md_cell(
            [
                "## 6. Hybrid: Vector + Graph",
                "",
                "Vector search for fiction books, then find which users bought them.",
            ]
        ),
        code_cell(
            [
                "fiction_query = [0.0, 0.95, 0.0, 0.05]",
                "",
                'query_hybrid = """',
                "    CALL uni.vector.query('Book', 'embedding', $vec, 3)",
                "    YIELD node, distance",
                "    MATCH (u:User)-[:PURCHASED]->(node)",
                "    RETURN node.name AS book, u.name AS buyer, distance",
                "    ORDER BY distance, buyer",
                '"""',
                'results = session.cypher(query_hybrid, {"vec": fiction_query})',
                "print('Fiction book buyers (via vector + graph):')",
                "for r in results:",
                "    print(f\"  {r['buyer']} bought '{r['book']}' (distance={r['distance']:.4f})\")",
            ]
        ),
        md_cell(
            [
                "## 7. Discovery: Popular Books Alice Hasn't Read",
                "",
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
                "results = session.cypher(query_discovery)",
                "print('Popular books Alice has not read:')",
                "for r in results:",
                "    print(f\"  {r['book']}: {r['buyers']} buyer(s)\")",
            ]
        ),
        md_cell(
            [
                "## 8. Query Builder Demo",
                "",
                "Using the type-safe query builder to browse books by genre.",
            ]
        ),
        code_cell(
            [
                "# Find all tech books using query builder",
                "tech_books = (",
                "    session.query(Book)",
                '    .filter(Book.genre == "tech")',
                "    .all()",
                ")",
                "",
                'print("Tech books:")',
                "for book in tech_books:",
                '    print(f"  - {book.name}")',
            ]
        ),
    ]
)


# =============================================================================
# 3. RAG Notebook
# =============================================================================
rag_nb = create_notebook(
    [
        md_cell(
            [
                "# Retrieval-Augmented Generation (RAG) with uni-pydantic",
                "",
                "Combining vector search with knowledge graph traversal for hybrid retrieval over Python web framework documentation.",
            ]
        ),
        code_cell(common_imports_with_vector),
        md_cell(
            [
                "## 1. Define Models",
                "",
                "Text chunks with embeddings, linked to named entities via MENTIONS edges.",
            ]
        ),
        code_cell(
            [
                "class Chunk(UniNode):",
                '    """A chunk of text with semantic embedding."""',
                '    __label__ = "Chunk"',
                "    ",
                "    chunk_id: str",
                "    text: str",
                '    embedding: Vector[4] = Field(metric="l2")  # [auth, routing, database, testing]',
                "    ",
                "    # Relationships",
                '    entities: list["Entity"] = Relationship("MENTIONS", direction="outgoing")',
                "",
                "",
                "class Entity(UniNode):",
                '    """A named entity extracted from text."""',
                '    __label__ = "Entity"',
                "    ",
                "    name: str",
                '    entity_type: str = Field(default="unknown")',
                "    ",
                "    # Relationships",
                '    mentioned_in: list[Chunk] = Relationship("MENTIONS", direction="incoming")',
                "",
                "",
                "class Mentions(UniEdge):",
                '    """Edge representing a chunk mentioning an entity."""',
                '    __edge_type__ = "MENTIONS"',
                "    __from__ = Chunk",
                "    __to__ = Entity",
            ]
        ),
        md_cell(["## 2. Setup Database and Session"]),
        code_cell(
            db_setup("rag")
            + [
                "session.register(Chunk, Entity, Mentions)",
                "session.sync_schema()",
                "",
                'print(f"Opened database at {db_path}")',
            ]
        ),
        md_cell(
            [
                "## 3. Create Data",
                "",
                "8 documentation chunks across 4 topics, with 6 named entities.",
            ]
        ),
        code_cell(
            [
                "# 4D embeddings: [auth, routing, database, testing]",
                'c1 = Chunk(chunk_id="c1", text="JWT tokens issued by /auth/login endpoint. Tokens expire after 1 hour.",',
                "           embedding=[1.0,  0.0,  0.0,  0.0 ])",
                'c2 = Chunk(chunk_id="c2", text="Token refresh via /auth/refresh. Send expired token, receive new one.",',
                "           embedding=[0.95, 0.05, 0.0,  0.0 ])",
                'c3 = Chunk(chunk_id="c3", text="Password hashing uses bcrypt with cost factor 12.",',
                "           embedding=[0.85, 0.0,  0.0,  0.15])",
                'c4 = Chunk(chunk_id="c4", text="Routes defined with @app.route decorator. Supports GET, POST, PUT, DELETE.",',
                "           embedding=[0.0,  1.0,  0.0,  0.0 ])",
                'c5 = Chunk(chunk_id="c5", text="Middleware intercepts requests before handlers. Register with app.use().",',
                "           embedding=[0.05, 0.9,  0.05, 0.0 ])",
                'c6 = Chunk(chunk_id="c6", text="ConnectionPool manages DB connections. Max pool size defaults to 10.",',
                "           embedding=[0.0,  0.0,  1.0,  0.0 ])",
                'c7 = Chunk(chunk_id="c7", text="ORM models inherit from BaseModel. Columns map to database fields.",',
                "           embedding=[0.0,  0.1,  0.9,  0.0 ])",
                'c8 = Chunk(chunk_id="c8", text="TestClient simulates HTTP requests without starting a server.",',
                "           embedding=[0.0,  0.2,  0.0,  0.8 ])",
                "",
                "# 6 entities",
                'jwt          = Entity(name="JWT",            entity_type="technology")',
                'auth_entity  = Entity(name="authentication", entity_type="concept")',
                'routing_ent  = Entity(name="routing",        entity_type="concept")',
                'db_entity    = Entity(name="database",       entity_type="concept")',
                'bcrypt_ent   = Entity(name="bcrypt",         entity_type="technology")',
                'pool_entity  = Entity(name="ConnectionPool", entity_type="class")',
                "",
                "session.add_all([c1, c2, c3, c4, c5, c6, c7, c8,",
                "                 jwt, auth_entity, routing_ent, db_entity, bcrypt_ent, pool_entity])",
                "session.commit()",
                "",
                "# Create vector index AFTER commit",
                'db.create_vector_index("Chunk", "embedding", "l2")',
                'print("Data ingested and vector index created")',
            ]
        ),
        code_cell(
            [
                "# MENTIONS edges",
                'session.create_edge(c1, "MENTIONS", jwt)',
                'session.create_edge(c1, "MENTIONS", auth_entity)',
                'session.create_edge(c2, "MENTIONS", jwt)',
                'session.create_edge(c2, "MENTIONS", auth_entity)',
                'session.create_edge(c3, "MENTIONS", bcrypt_ent)',
                'session.create_edge(c3, "MENTIONS", auth_entity)',
                'session.create_edge(c4, "MENTIONS", routing_ent)',
                'session.create_edge(c5, "MENTIONS", routing_ent)',
                'session.create_edge(c6, "MENTIONS", db_entity)',
                'session.create_edge(c6, "MENTIONS", pool_entity)',
                'session.create_edge(c7, "MENTIONS", db_entity)',
                "session.commit()",
                'print("Entity mention edges created")',
            ]
        ),
        md_cell(
            [
                "## 4. Pure Vector Search",
                "",
                "Find the 3 chunks most similar to an authentication query.",
            ]
        ),
        code_cell(
            [
                "auth_query = [1.0, 0.0, 0.0, 0.0]",
                "",
                'query_vec = """',
                "    CALL uni.vector.query('Chunk', 'embedding', $vec, 3)",
                "    YIELD node, distance",
                "    RETURN node.chunk_id AS chunk_id, node.text AS text, distance",
                "    ORDER BY distance",
                '"""',
                'results = session.cypher(query_vec, {"vec": auth_query})',
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
                "## 5. Graph Expansion",
                "",
                "Same vector seeds — now also show which entities each chunk mentions.",
            ]
        ),
        code_cell(
            [
                'query_expand = """',
                "    CALL uni.vector.query('Chunk', 'embedding', $vec, 3)",
                "    YIELD node, distance",
                "    MATCH (node)-[:MENTIONS]->(e:Entity)",
                "    RETURN node.chunk_id AS chunk_id, e.name AS entity, distance",
                "    ORDER BY distance, entity",
                '"""',
                'results = session.cypher(query_expand, {"vec": auth_query})',
                "print('Entities mentioned by top auth chunks:')",
                "for r in results:",
                "    print(f\"  {r['chunk_id']} -> {r['entity']}\")",
            ]
        ),
        md_cell(
            [
                "## 6. Entity Bridging",
                "",
                "Find all chunks related to the auth seeds via shared entity mentions — the core graph RAG technique.",
            ]
        ),
        code_cell(
            [
                'query_bridge = """',
                "    CALL uni.vector.query('Chunk', 'embedding', $vec, 3)",
                "    YIELD node AS anchor, distance",
                "    MATCH (anchor)-[:MENTIONS]->(e:Entity)<-[:MENTIONS]-(related:Chunk)",
                "    WHERE related._vid <> anchor._vid",
                "    RETURN anchor.chunk_id AS anchor_id, e.name AS bridge_entity,",
                "           related.chunk_id AS related_id",
                "    ORDER BY anchor_id, bridge_entity",
                '"""',
                'results = session.cypher(query_bridge, {"vec": auth_query})',
                "print('Entity bridges between auth chunks:')",
                "for r in results:",
                "    print(f\"  {r['anchor_id']} <-> {r['related_id']} (via {r['bridge_entity']})\")",
            ]
        ),
        md_cell(
            [
                "## 7. Context Assembly",
                "",
                "Full hybrid pipeline: vector seeds + graph bridging → collect unique chunks for the LLM context window.",
            ]
        ),
        code_cell(
            [
                'query_ctx = """',
                "    CALL uni.vector.query('Chunk', 'embedding', $vec, 3)",
                "    YIELD node AS seed, distance",
                "    MATCH (seed)-[:MENTIONS]->(e:Entity)<-[:MENTIONS]-(related:Chunk)",
                "    RETURN seed.chunk_id AS seed_id, seed.text AS seed_text,",
                "           related.chunk_id AS related_id, related.text AS related_text,",
                "           e.name AS shared_entity",
                "    ORDER BY seed_id, shared_entity",
                '"""',
                'results = session.cypher(query_ctx, {"vec": auth_query})',
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
        md_cell(
            [
                "## 8. Query Builder Demo",
                "",
                "Using the type-safe query builder to browse entities.",
            ]
        ),
        code_cell(
            [
                "# Find all technology entities using query builder",
                "tech_entities = (",
                "    session.query(Entity)",
                '    .filter(Entity.entity_type == "technology")',
                "    .all()",
                ")",
                "",
                'print("Technology entities:")',
                "for entity in tech_entities:",
                '    print(f"  - {entity.name}")',
                "",
                "total_chunks = session.query(Chunk).count()",
                'print(f"Total chunks in knowledge base: {total_chunks}")',
            ]
        ),
    ]
)


# =============================================================================
# 4. Fraud Detection Notebook
# =============================================================================
fraud_nb = create_notebook(
    [
        md_cell(
            [
                "# Fraud Detection with uni-pydantic",
                "",
                "Detecting money laundering rings (3-cycles) and shared device anomalies using Pydantic models.",
            ]
        ),
        code_cell(common_imports),
        md_cell(
            [
                "## 1. Define Models",
                "",
                "Named users with risk scores, devices, and financial transaction edges.",
            ]
        ),
        code_cell(
            [
                "class User(UniNode):",
                '    """A user in the fraud detection system."""',
                '    __label__ = "User"',
                "    ",
                "    name: str",
                "    email: str",
                "    risk_score: float | None = Field(default=None)",
                "    ",
                "    # Relationships",
                '    sent_to: list["User"] = Relationship("SENT_MONEY", direction="outgoing")',
                '    received_from: list["User"] = Relationship("SENT_MONEY", direction="incoming")',
                '    devices: list["Device"] = Relationship("USED_DEVICE", direction="outgoing")',
                "",
                "",
                "class Device(UniNode):",
                '    """A device used by users."""',
                '    __label__ = "Device"',
                "    ",
                "    device_id: str",
                "    ",
                "    # Relationships",
                '    users: list[User] = Relationship("USED_DEVICE", direction="incoming")',
                "",
                "",
                "class SentMoney(UniEdge):",
                '    """Edge representing money transfer between users."""',
                '    __edge_type__ = "SENT_MONEY"',
                "    __from__ = User",
                "    __to__ = User",
                "    ",
                "    amount: float",
                "",
                "",
                "class UsedDevice(UniEdge):",
                '    """Edge representing user-device association."""',
                '    __edge_type__ = "USED_DEVICE"',
                "    __from__ = User",
                "    __to__ = Device",
            ]
        ),
        md_cell(["## 2. Setup Database and Session"]),
        code_cell(
            db_setup("fraud")
            + [
                "session.register(User, Device, SentMoney, UsedDevice)",
                "session.sync_schema()",
                "",
                'print(f"Opened database at {db_path}")',
            ]
        ),
        md_cell(
            [
                "## 3. Create Data",
                "",
                "5 named users, 3 devices, a money ring, and suspicious cross-device links.",
            ]
        ),
        code_cell(
            [
                "# 5 users: 3 in a ring, 2 high-risk fraudsters",
                'alice  = User(name="Alice",  email="alice@example.com",  risk_score=0.10)',
                'bob    = User(name="Bob",    email="bob@example.com",    risk_score=0.15)',
                'carlos = User(name="Carlos", email="carlos@example.com", risk_score=0.20)',
                'dana   = User(name="Dana",   email="dana@example.com",   risk_score=0.92)',
                'eve    = User(name="Eve",    email="eve@example.com",    risk_score=0.88)',
                "",
                "# 3 devices",
                'device_a = Device(device_id="device_A")',
                'device_b = Device(device_id="device_B")',
                'device_c = Device(device_id="device_C")',
                "",
                "session.add_all([alice, bob, carlos, dana, eve, device_a, device_b, device_c])",
                "session.commit()",
                'print("Users and devices created")',
            ]
        ),
        code_cell(
            [
                "# Money ring: Alice -> Bob -> Carlos -> Alice",
                'session.create_edge(alice,  "SENT_MONEY", bob,    {"amount": 9500.0})',
                'session.create_edge(bob,    "SENT_MONEY", carlos, {"amount": 9000.0})',
                'session.create_edge(carlos, "SENT_MONEY", alice,  {"amount": 8750.0})',
                'session.create_edge(dana,   "SENT_MONEY", eve,    {"amount": 15000.0})  # Suspicious',
                "",
                "# Device sharing: Alice+Dana on device_A, Bob+Eve on device_B, Carlos alone on device_C",
                'session.create_edge(alice,  "USED_DEVICE", device_a)',
                'session.create_edge(dana,   "USED_DEVICE", device_a)',
                'session.create_edge(bob,    "USED_DEVICE", device_b)',
                'session.create_edge(eve,    "USED_DEVICE", device_b)',
                'session.create_edge(carlos, "USED_DEVICE", device_c)',
                "",
                "session.commit()",
                'print("Edges created")',
            ]
        ),
        md_cell(
            [
                "## 4. Ring Detection",
                "",
                "Find 3-cycles in the money transfer graph. Deduplication: `a._vid < b._vid AND a._vid < c._vid` prevents each ring appearing 3 times.",
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
                "results = session.cypher(query_ring)",
                "print('Money laundering rings detected:')",
                "for r in results:",
                "    print(f\"  Ring: {r['user_a']} | {r['user_b']} | {r['user_c']} ({r['rings']} ring(s))\")",
                "assert len(results) == 1, f'Expected 1 ring, got {len(results)}'",
            ]
        ),
        md_cell(
            [
                "## 5. Ring with Transfer Amounts",
                "",
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
                "results = session.cypher(query_amounts)",
                "for r in results:",
                "    print(f\"Ring: {r['user_a']} -> {r['user_b']} -> {r['user_c']} -> {r['user_a']}\")",
                "    print(f\"  Leg amounts: ${r['leg1']:.0f}, ${r['leg2']:.0f}, ${r['leg3']:.0f}\")",
                "    print(f\"  Total cycled: ${r['total_cycled']:,.0f}\")",
            ]
        ),
        md_cell(
            [
                "## 6. Shared Device Risk",
                "",
                "Find users who share a device with a high-risk user (risk > 0.8). Carlos should NOT appear — he only uses device_C alone.",
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
                "results = session.cypher(query_shared)",
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
                "## 7. Combined Alert: Ring + Device Sharing",
                "",
                "Users appearing in BOTH a money ring AND sharing a device with a fraudster are the highest-priority targets.",
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
                "ring_members = {r['n'] for r in session.cypher(ring_query)}",
                "",
                "# Device-sharing users",
                'device_query = """',
                "    MATCH (u:User)-[:USED_DEVICE]->(d:Device)<-[:USED_DEVICE]-(fraudster:User)",
                "    WHERE fraudster.risk_score > 0.8 AND u._vid <> fraudster._vid",
                "    RETURN u.name AS n",
                '"""',
                "device_risk = {r['n'] for r in session.cypher(device_query)}",
                "",
                "combined = ring_members & device_risk",
                "print(f'Ring members: {sorted(ring_members)}')",
                "print(f'Device-sharing users: {sorted(device_risk)}')",
                "print(f'HIGH PRIORITY (both signals): {sorted(combined)}')",
                "assert 'Alice' in combined, f'Alice should be in combined alert, got {combined}'",
            ]
        ),
        md_cell(
            [
                "## 8. Query Builder Demo",
                "",
                "Using the type-safe query builder to find high-risk users.",
            ]
        ),
        code_cell(
            [
                "# Find all high-risk users using the query builder",
                "high_risk_users = (",
                "    session.query(User)",
                "    .filter(User.risk_score >= 0.5)",
                "    .all()",
                ")",
                "",
                'print(f"High-risk users found: {len(high_risk_users)}")',
                "for user in high_risk_users:",
                '    print(f"  {user.name} (risk={user.risk_score})")',
            ]
        ),
    ]
)


# =============================================================================
# 5. Sales Analytics Notebook
# =============================================================================
sales_nb = create_notebook(
    [
        md_cell(
            [
                "# Regional Sales Analytics with uni-pydantic",
                "",
                "Combining graph traversal with columnar aggregation across multiple regions and product categories.",
            ]
        ),
        code_cell(common_imports),
        md_cell(
            [
                "## 1. Define Models",
                "",
                "Regions, Categories, and Orders with two edge types.",
            ]
        ),
        code_cell(
            [
                "class Region(UniNode):",
                '    """A geographic region for sales tracking."""',
                '    __label__ = "Region"',
                "    ",
                '    name: str = Field(index="btree")',
                "    ",
                "    # Relationships",
                '    orders: list["Order"] = Relationship("SHIPPED_TO", direction="incoming")',
                "",
                "",
                "class Category(UniNode):",
                '    """A product category."""',
                '    __label__ = "Category"',
                "    ",
                "    name: str",
                "    ",
                "    # Relationships",
                '    orders: list["Order"] = Relationship("IN_CATEGORY", direction="incoming")',
                "",
                "",
                "class Order(UniNode):",
                '    """A sales order."""',
                '    __label__ = "Order"',
                "    ",
                "    amount: float",
                "    ",
                "    # Relationships",
                '    region: "Region | None" = Relationship("SHIPPED_TO", direction="outgoing")',
                '    category: "Category | None" = Relationship("IN_CATEGORY", direction="outgoing")',
                "",
                "",
                "class ShippedTo(UniEdge):",
                '    """Edge representing order shipped to region."""',
                '    __edge_type__ = "SHIPPED_TO"',
                "    __from__ = Order",
                "    __to__ = Region",
                "",
                "",
                "class InCategory(UniEdge):",
                '    """Edge representing order in a product category."""',
                '    __edge_type__ = "IN_CATEGORY"',
                "    __from__ = Order",
                "    __to__ = Category",
            ]
        ),
        md_cell(["## 2. Setup Database and Session"]),
        code_cell(
            db_setup("sales")
            + [
                "session.register(Region, Category, Order, ShippedTo, InCategory)",
                "session.sync_schema()",
                "",
                'print(f"Opened database at {db_path}")',
            ]
        ),
        md_cell(
            [
                "## 3. Create Data",
                "",
                "4 regions, 3 categories, 27 orders distributed non-uniformly.",
            ]
        ),
        code_cell(
            [
                "# Regions",
                'north = Region(name="North")',
                'south = Region(name="South")',
                'east  = Region(name="East")',
                'west  = Region(name="West")',
                "",
                "# Categories",
                'electronics = Category(name="Electronics")',
                'apparel     = Category(name="Apparel")',
                'home_garden = Category(name="Home & Garden")',
                "",
                "session.add_all([north, south, east, west, electronics, apparel, home_garden])",
                "session.commit()",
                'print("Regions and categories created")',
            ]
        ),
        code_cell(
            [
                "# Orders: (amount, region_obj, category_obj)",
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
                "order_objects = [Order(amount=amt) for amt, _, _ in orders_data]",
                "session.add_all(order_objects)",
                "session.commit()",
                "",
                "for order_obj, (amt, region_obj, cat_obj) in zip(order_objects, orders_data):",
                '    session.create_edge(order_obj, "SHIPPED_TO",  region_obj)',
                '    session.create_edge(order_obj, "IN_CATEGORY", cat_obj)',
                "session.commit()",
                'print("Data ingested")',
            ]
        ),
        md_cell(
            [
                "## 4. Revenue by Region",
                "",
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
                "results = session.cypher(query_region)",
                "print('Revenue by region:')",
                "for r in results:",
                "    print(f\"  {r['region']:10s}: {r['order_count']:3d} orders, ${r['total_revenue']:8.2f}\")",
                "assert len(results) == 4, f'Expected 4 regions, got {len(results)}'",
            ]
        ),
        md_cell(
            [
                "## 5. Region \u00d7 Category Breakdown",
                "",
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
                "results = session.cypher(query_breakdown)",
                "print('Region x Category breakdown:')",
                "current_region = None",
                "for r in results:",
                "    if r['region'] != current_region:",
                "        current_region = r['region']",
                '        print(f"  {current_region}:")',
                "    print(f\"    {r['category']:15s}: {r['orders']} orders, ${r['revenue']:.2f}\")",
                "assert len(results) == 12, f'Expected 12 rows (4x3), got {len(results)}'",
            ]
        ),
        md_cell(
            [
                "## 6. Top Orders per Region",
                "",
                "Highest-value orders in each region.",
            ]
        ),
        code_cell(
            [
                "from collections import defaultdict",
                "",
                'query_top = """',
                "    MATCH (r:Region)<-[:SHIPPED_TO]-(o:Order)",
                "    RETURN r.name AS region, o.amount AS amount",
                "    ORDER BY region, amount DESC",
                '"""',
                "all_orders = session.cypher(query_top)",
                "",
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
                "## 7. Best Category per Region",
                "",
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
                "results = session.cypher(query_best_cat)",
                "",
                "best = {}",
                "for r in results:",
                "    if r['region'] not in best:",
                "        best[r['region']] = (r['category'], r['revenue'])",
                "",
                "print('Best category per region:')",
                "for region, (cat, rev) in sorted(best.items()):",
                "    print(f'  {region:10s}: {cat} (${rev:.2f})')",
                "",
                "best_cats = [cat for cat, _ in best.values()]",
                "assert len(set(best_cats)) > 1, f'Expected variance across regions, got {best_cats}'",
            ]
        ),
        md_cell(
            [
                "## 8. Query Builder Demo",
                "",
                "Using the type-safe query builder for high-value order analysis.",
            ]
        ),
        code_cell(
            [
                "# Find high-value orders using query builder",
                "high_value_orders = (",
                "    session.query(Order)",
                "    .filter(Order.amount >= 500.0)",
                "    .order_by(Order.amount, descending=True)",
                "    .limit(5)",
                "    .all()",
                ")",
                "",
                'print("Top 5 High-Value Orders (>=$500):")',
                "for i, order in enumerate(high_value_orders, 1):",
                '    print(f"  {i}. ${order.amount:,.2f}")',
                "",
                "# Count orders by value range",
                "small  = session.query(Order).filter(Order.amount < 100).count()",
                "medium = session.query(Order).filter(Order.amount >= 100).filter(Order.amount < 500).count()",
                "large  = session.query(Order).filter(Order.amount >= 500).count()",
                'print("Order Distribution:")',
                'print(f"  Small  (<$100):     {small}")',
                'print(f"  Medium ($100-499):  {medium}")',
                'print(f"  Large  (>=$500):    {large}")',
            ]
        ),
    ]
)


# =============================================================================
# Write all notebooks
# =============================================================================
if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))

    notebooks = {
        "supply_chain.ipynb": supply_chain_nb,
        "recommendation.ipynb": rec_nb,
        "rag.ipynb": rag_nb,
        "fraud_detection.ipynb": fraud_nb,
        "sales_analytics.ipynb": sales_nb,
    }

    for filename, notebook in notebooks.items():
        filepath = os.path.join(script_dir, filename)
        with open(filepath, "w") as f:
            json.dump(notebook, f, indent=2)
        print(f"Generated {filename}")

    print("\nAll notebooks generated successfully.")
