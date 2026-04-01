"""
Social Network modeled with uni-pydantic.

Demonstrates:
  - User nodes with name, bio, and embedding vector fields
  - FOLLOWS edges with since date and weight properties
  - Creating users and follow relationships
  - Querying followers with filters
  - Vector similarity search on user bio embeddings
"""

from __future__ import annotations

import random
import tempfile

import uni_db
from uni_pydantic import (
    Field,
    Relationship,
    UniEdge,
    UniNode,
    UniSession,
    Vector,
)


# ---------------------------------------------------------------------------
# 1. Model definitions
# ---------------------------------------------------------------------------

class User(UniNode):
    """A user in the social network."""

    __label__ = "User"

    name: str = Field(index="btree")
    bio: str | None = None
    embedding: Vector[8] = Field(metric="cosine")

    # Relationship helpers for lazy-loading
    following: list["User"] = Relationship("FOLLOWS", direction="outgoing")
    followers: list["User"] = Relationship("FOLLOWS", direction="incoming")


class FollowsEdge(UniEdge):
    """Edge representing a follow relationship between two users."""

    __edge_type__ = "FOLLOWS"
    __from__ = User
    __to__ = User

    since: str  # ISO date string
    weight: float = 1.0


# ---------------------------------------------------------------------------
# 2. Helper: generate a deterministic pseudo-embedding from text
# ---------------------------------------------------------------------------

def text_to_embedding(text: str, dims: int = 8) -> list[float]:
    """
    Convert a text string into a fixed-size pseudo-embedding.

    This is NOT a real embedding model -- it is a deterministic hash-based
    projection used so the demo runs without any ML dependencies.
    """
    rng = random.Random(text)
    vec = [rng.gauss(0, 1) for _ in range(dims)]
    # L2-normalize so cosine distance is meaningful
    norm = sum(v * v for v in vec) ** 0.5
    if norm > 0:
        vec = [v / norm for v in vec]
    return vec


# ---------------------------------------------------------------------------
# 3. Main demo
# ---------------------------------------------------------------------------

def main() -> None:
    # ---- database setup ----------------------------------------------------
    with tempfile.TemporaryDirectory() as tmp:
        db = uni_db.UniBuilder.open(tmp).build()

        with UniSession(db) as session:
            # Register models and sync schema (creates labels, edge types, indexes)
            session.register(User, FollowsEdge)
            session.sync_schema()

            # ---- create users ----------------------------------------------
            users_data = [
                ("Alice",   "Machine learning researcher interested in NLP and transformers"),
                ("Bob",     "Backend engineer who loves distributed systems and Rust"),
                ("Charlie", "Data scientist working on recommendation systems"),
                ("Diana",   "Frontend developer passionate about React and design systems"),
                ("Eve",     "Security researcher focused on cryptography and protocols"),
                ("Frank",   "DevOps engineer automating infrastructure with Kubernetes"),
                ("Grace",   "NLP engineer building large language model applications"),
                ("Hank",    "Full-stack developer with interest in graph databases"),
            ]

            users: dict[str, User] = {}
            for name, bio in users_data:
                emb = text_to_embedding(bio)
                user = User(name=name, bio=bio, embedding=emb)
                session.add(user)
                users[name] = user

            session.commit()

            print("=== Created Users ===")
            for name, user in users.items():
                print(f"  {name} (vid={user.vid})")

            # ---- create follow relationships -------------------------------
            follow_pairs: list[tuple[str, str, str, float]] = [
                ("Alice",   "Bob",     "2024-01-15", 0.9),
                ("Alice",   "Charlie", "2024-02-01", 0.8),
                ("Alice",   "Grace",   "2024-03-10", 0.95),
                ("Bob",     "Alice",   "2024-01-20", 0.85),
                ("Bob",     "Frank",   "2024-04-05", 0.7),
                ("Charlie", "Alice",   "2024-02-10", 0.9),
                ("Charlie", "Diana",   "2024-05-01", 0.6),
                ("Diana",   "Eve",     "2024-06-15", 0.75),
                ("Eve",     "Alice",   "2024-07-20", 0.8),
                ("Frank",   "Bob",     "2024-04-10", 0.65),
                ("Grace",   "Alice",   "2024-03-15", 0.9),
                ("Grace",   "Charlie", "2024-08-01", 0.7),
                ("Hank",    "Alice",   "2024-09-01", 0.5),
                ("Hank",    "Bob",     "2024-09-01", 0.6),
                ("Hank",    "Grace",   "2024-09-15", 0.8),
            ]

            for src_name, dst_name, since, weight in follow_pairs:
                edge = FollowsEdge(since=since, weight=weight)
                session.create_edge(users[src_name], "FOLLOWS", users[dst_name], edge)

            db.flush()

            print("\n=== Created Follow Relationships ===")
            for src, dst, since, weight in follow_pairs:
                print(f"  {src} --FOLLOWS(w={weight})--> {dst}")

            # ---- query: all users ------------------------------------------
            print("\n=== All Users (via query builder) ===")
            all_users = session.query(User).order_by("name").all()
            for u in all_users:
                print(f"  {u.name}: {u.bio}")

            # ---- query: filter by name prefix ------------------------------
            print("\n=== Users whose name starts with 'A' or 'B' ===")
            a_users = session.query(User).filter(User.name.starts_with("A")).all()
            b_users = session.query(User).filter(User.name.starts_with("B")).all()
            for u in a_users + b_users:
                print(f"  {u.name}")

            # ---- query: filter_by exact match ------------------------------
            print("\n=== Lookup user 'Eve' by name ===")
            eve = session.query(User).filter_by(name="Eve").first()
            if eve:
                print(f"  Found: {eve.name} (vid={eve.vid}), bio={eve.bio}")

            # ---- query: who does Alice follow? (Cypher) --------------------
            print("\n=== Alice's following list (raw Cypher) ===")
            alice = users["Alice"]
            results = session.cypher(
                "MATCH (a:User)-[r:FOLLOWS]->(b:User) "
                "WHERE a.name = $name "
                "RETURN b.name AS name, r.weight AS weight "
                "ORDER BY r.weight DESC",
                {"name": "Alice"},
            )
            for row in results:
                print(f"  -> {row['name']} (weight={row['weight']})")

            # ---- query: who follows Alice? (Cypher) ------------------------
            print("\n=== Alice's followers (raw Cypher) ===")
            results = session.cypher(
                "MATCH (a:User)-[r:FOLLOWS]->(b:User) "
                "WHERE b.name = $name "
                "RETURN a.name AS name, r.since AS since "
                "ORDER BY a.name",
                {"name": "Alice"},
            )
            for row in results:
                print(f"  <- {row['name']} (since {row['since']})")

            # ---- query: followers with weight filter (Cypher) --------------
            print("\n=== Strong followers of Alice (weight >= 0.85) ===")
            results = session.cypher(
                "MATCH (a:User)-[r:FOLLOWS]->(b:User) "
                "WHERE b.name = $name AND r.weight >= $min_weight "
                "RETURN a.name AS name, r.weight AS weight "
                "ORDER BY r.weight DESC",
                {"name": "Alice", "min_weight": 0.85},
            )
            for row in results:
                print(f"  <- {row['name']} (weight={row['weight']})")

            # ---- query: count followers ------------------------------------
            print("\n=== Follower counts ===")
            results = session.cypher(
                "MATCH (a:User)-[:FOLLOWS]->(b:User) "
                "RETURN b.name AS name, count(a) AS follower_count "
                "ORDER BY follower_count DESC",
            )
            for row in results:
                print(f"  {row['name']}: {row['follower_count']} followers")

            # ---- query: mutual follows -------------------------------------
            print("\n=== Mutual follows (A follows B AND B follows A) ===")
            results = session.cypher(
                "MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(a) "
                "WHERE a.name < b.name "
                "RETURN a.name AS user1, b.name AS user2 "
                "ORDER BY user1",
            )
            for row in results:
                print(f"  {row['user1']} <-> {row['user2']}")

            # ---- vector similarity search ----------------------------------
            print("\n=== Vector Similarity Search: users similar to 'NLP engineer' ===")
            query_text = "NLP engineer building language models"
            query_vec = text_to_embedding(query_text)

            similar_users = (
                session.query(User)
                .vector_search("embedding", query_vec, k=5)
                .all()
            )
            for u in similar_users:
                print(f"  {u.name}: {u.bio}")

            # ---- vector search with filter ---------------------------------
            print("\n=== Vector search excluding 'Alice' ===")
            similar_non_alice = (
                session.query(User)
                .vector_search("embedding", query_vec, k=5)
                .filter(User.name != "Alice")
                .all()
            )
            for u in similar_non_alice:
                print(f"  {u.name}: {u.bio}")

            # ---- traversal: 2-hop from Alice via FOLLOWS -------------------
            print("\n=== 2-hop traversal: friends-of-friends from Alice ===")
            results = session.cypher(
                "MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User) "
                "WHERE a.name = $name AND c.name <> a.name "
                "RETURN DISTINCT c.name AS name "
                "ORDER BY name",
                {"name": "Alice"},
            )
            for row in results:
                print(f"  {row['name']}")

            # ---- update and delete demo ------------------------------------
            print("\n=== Update Hank's bio ===")
            hank = session.get(User, name="Hank")
            if hank:
                print(f"  Before: {hank.bio}")
                hank.bio = "Graph database enthusiast and full-stack developer"
                hank.embedding = Vector[8](
                    text_to_embedding("Graph database enthusiast and full-stack developer")
                )
                session.commit()
                session.refresh(hank)
                print(f"  After:  {hank.bio}")

            print("\n=== Delete user 'Frank' ===")
            frank = session.get(User, name="Frank")
            if frank:
                session.delete(frank)
                session.commit()
                remaining = session.query(User).count()
                print(f"  Deleted Frank. Remaining users: {remaining}")

            print("\nDone!")


if __name__ == "__main__":
    main()
