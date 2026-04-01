"""
Social Network modeled with uni-pydantic OGM.

Demonstrates:
  - UniNode / UniEdge model definitions with Vector fields
  - Schema registration and sync
  - Creating users and follow relationships
  - QueryBuilder filters, ordering, pagination
  - Vector similarity search on user bio embeddings
"""

from datetime import date, datetime

from uni_db import Uni
from uni_pydantic import (
    UniNode,
    UniEdge,
    UniSession,
    Field,
    Relationship,
    Vector,
    before_create,
)


# ---------------------------------------------------------------------------
# 1. Model Definitions
# ---------------------------------------------------------------------------

class User(UniNode):
    """A social-network user with a bio embedding for semantic search."""

    __label__ = "User"

    name: str = Field(index="btree")
    bio: str = Field(index="fulltext", tokenizer="standard")
    bio_embedding: Vector[384] = Field(metric="cosine")
    joined_at: datetime | None = None

    # Outgoing FOLLOWS edges
    following: list["User"] = Relationship(
        "FOLLOWS", direction="outgoing", edge_model="Follows",
    )
    # Incoming FOLLOWS edges
    followers: list["User"] = Relationship(
        "FOLLOWS", direction="incoming", edge_model="Follows",
    )

    @before_create
    def set_joined_at(self):
        if self.joined_at is None:
            self.joined_at = datetime.now()


class Follows(UniEdge):
    """Directed follow relationship with a date and a weight (0-1)."""

    __edge_type__ = "FOLLOWS"
    __from__ = User
    __to__ = User

    since: date
    weight: float = 1.0  # affinity / interaction weight


# ---------------------------------------------------------------------------
# 2. Fake embedding helper (replace with a real model in production)
# ---------------------------------------------------------------------------

def fake_embed(text: str, dim: int = 384) -> list[float]:
    """Deterministic pseudo-embedding derived from the text hash."""
    import hashlib

    h = hashlib.sha256(text.encode()).digest()
    # Expand hash bytes into `dim` floats in [-1, 1], then L2-normalise
    raw = [(b / 127.5 - 1.0) for b in (h * (dim // len(h) + 1))[:dim]]
    norm = max(sum(x * x for x in raw) ** 0.5, 1e-9)
    return [x / norm for x in raw]


# ---------------------------------------------------------------------------
# 3. Bootstrap database, register models, sync schema
# ---------------------------------------------------------------------------

db = Uni.temporary()
session = UniSession(db)
session.register(User, Follows)
session.sync_schema()

# ---------------------------------------------------------------------------
# 4. Create users
# ---------------------------------------------------------------------------

users_data = [
    ("Alice",   "Machine learning researcher focused on NLP and transformers"),
    ("Bob",     "Backend engineer building distributed systems in Rust"),
    ("Charlie", "Data scientist working on recommendation engines"),
    ("Diana",   "Frontend developer passionate about accessibility and UX"),
    ("Eve",     "Security engineer specialising in applied cryptography"),
    ("Frank",   "DevOps lead automating cloud infrastructure with Terraform"),
]

users: dict[str, User] = {}
for name, bio in users_data:
    user = User(
        name=name,
        bio=bio,
        bio_embedding=Vector(fake_embed(bio)),
    )
    session.add(user)
    users[name] = user

session.commit()

print("== Created users ==")
for u in users.values():
    print(f"  {u.name} (vid={u.vid})")

# ---------------------------------------------------------------------------
# 5. Add follow relationships
# ---------------------------------------------------------------------------

follow_specs = [
    # (follower, followee, since, weight)
    ("Alice",   "Bob",     date(2024, 1, 15),  0.9),
    ("Alice",   "Charlie", date(2024, 3, 10),  0.7),
    ("Bob",     "Alice",   date(2024, 2, 1),   0.8),
    ("Bob",     "Eve",     date(2024, 5, 20),  0.6),
    ("Charlie", "Alice",   date(2024, 1, 5),   0.95),
    ("Charlie", "Diana",   date(2024, 4, 12),  0.5),
    ("Diana",   "Alice",   date(2024, 6, 1),   0.85),
    ("Diana",   "Frank",   date(2024, 7, 8),   0.4),
    ("Eve",     "Bob",     date(2024, 3, 22),  0.7),
    ("Frank",   "Charlie", date(2024, 8, 15),  0.6),
]

for follower_name, followee_name, since, weight in follow_specs:
    session.create_edge(
        users[follower_name],
        "FOLLOWS",
        users[followee_name],
        Follows(since=since, weight=weight),
    )

session.commit()
print(f"\n== Created {len(follow_specs)} follow relationships ==")

# ---------------------------------------------------------------------------
# 6. Query followers with filters
# ---------------------------------------------------------------------------

# 6a. All users ordered alphabetically
print("\n== All users (alphabetical) ==")
all_users = session.query(User).order_by(User.name).all()
for u in all_users:
    print(f"  {u.name}: {u.bio[:50]}...")

# 6b. Users whose name starts with a specific prefix
print("\n== Users whose name starts with 'A' or 'B' ==")
ab_users = (
    session.query(User)
    .filter(User.name.in_(["Alice", "Bob"]))
    .order_by(User.name)
    .all()
)
for u in ab_users:
    print(f"  {u.name}")

# 6c. Who does Alice follow?
print("\n== Alice's following (via Cypher) ==")
alice_following = session.cypher(
    """
    MATCH (a:User {name: $name})-[f:FOLLOWS]->(b:User)
    RETURN b.name AS followee, f.since AS since, f.weight AS weight
    ORDER BY f.weight DESC
    """,
    params={"name": "Alice"},
)
for row in alice_following:
    print(f"  -> {row['followee']}  (since {row['since']}, weight {row['weight']})")

# 6d. Who follows Alice? (incoming)
print("\n== Alice's followers (via Cypher) ==")
alice_followers = session.cypher(
    """
    MATCH (a:User {name: $name})<-[f:FOLLOWS]-(b:User)
    RETURN b.name AS follower, f.since AS since, f.weight AS weight
    ORDER BY f.weight DESC
    """,
    params={"name": "Alice"},
)
for row in alice_followers:
    print(f"  <- {row['follower']}  (since {row['since']}, weight {row['weight']})")

# 6e. Mutual follows (Alice <-> X)
print("\n== Mutual follows with Alice ==")
mutual = session.cypher(
    """
    MATCH (a:User {name: 'Alice'})-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(a)
    RETURN b.name AS mutual_friend
    ORDER BY b.name
    """,
)
for row in mutual:
    print(f"  <-> {row['mutual_friend']}")

# 6f. Paginated user list (skip 2, limit 3)
print("\n== Paginated users (skip=2, limit=3) ==")
page = session.query(User).order_by(User.name).skip(2).limit(3).all()
for u in page:
    print(f"  {u.name}")

# 6g. Count users
total = session.query(User).count()
print(f"\n== Total user count: {total} ==")

# ---------------------------------------------------------------------------
# 7. Vector similarity search on user bios
# ---------------------------------------------------------------------------

# Find users whose bios are most similar to a query about "deep learning NLP"
query_text = "deep learning natural language processing research"
query_vec = fake_embed(query_text)

print(f"\n== Vector search: users similar to '{query_text}' ==")
similar_users = (
    session.query(User)
    .vector_search(User.bio_embedding, query_vec, k=5, threshold=0.0)
    .all()
)
for u in similar_users:
    print(f"  {u.name}: {u.bio}")

# Alternative: raw Cypher vector search for more control
print(f"\n== Raw Cypher vector search (top 3) ==")
vector_results = session.cypher(
    """
    CALL uni.vector.query('User', 'bio_embedding', $qvec, 3)
    YIELD node, score
    RETURN node.name AS name, node.bio AS bio, score
    ORDER BY score DESC
    """,
    params={"qvec": query_vec},
)
for row in vector_results:
    print(f"  {row['name']} (score={row['score']:.4f}): {row['bio']}")

# Inline similar_to scoring on already-matched nodes
print(f"\n== similar_to() scoring on matched users ==")
scored = session.cypher(
    """
    MATCH (u:User)
    RETURN u.name AS name,
           similar_to(u.bio_embedding, $qvec) AS sim_score
    ORDER BY sim_score DESC
    """,
    params={"qvec": query_vec},
)
for row in scored:
    print(f"  {row['name']}: similarity = {row['sim_score']:.4f}")

# ---------------------------------------------------------------------------
# 8. Cleanup
# ---------------------------------------------------------------------------

session.close()
db.shutdown()
print("\n== Done. Database shut down. ==")
