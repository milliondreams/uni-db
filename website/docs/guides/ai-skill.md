# AI Agent Skill

Uni ships an agent skill (`uni-db-dev`) that gives AI coding assistants deep knowledge of the Uni API, Cypher dialect, Locy language, vector search, and schema design. With the skill installed, your AI assistant can build Uni-powered applications without you having to paste documentation into every conversation.

The skill works with **Claude Code, GitHub Copilot, Cursor, Cline**, and other agents that support the [skills.sh](https://skills.sh/) ecosystem.

## What the skill provides

The skill bundles reference material that the agent loads on demand:

| Reference | Covers |
|-----------|--------|
| Cypher reference | Full clause syntax, operators, functions, patterns |
| Vector search | KNN, FTS, hybrid search procedures and parameters |
| Locy reference | Rules, ALONG/FOLD/BEST BY, ASSUME/ABDUCE |
| Schema design | Modeling principles, index types, pushdown rules |
| Python API | Database, AsyncDatabase, builders, bulk ops, transactions |
| Rust API | Uni, query, schema, bulk, transactions, sessions |

## Installation

```bash
npx skills add https://github.com/rustic-ai/uni-db --skill uni-db-dev
```

This fetches the skill from the [Uni repository](https://github.com/rustic-ai/uni-db) and installs it into your agent's configuration. Works with any agent that supports the skills.sh directory.

## When the skill triggers

The skill activates automatically when the agent detects any of these signals:

- Code imports `uni_db` (Python) or depends on the `uni-db` crate (Rust)
- You mention "uni", "uni-db", or "graph database" in context of this project
- You write or ask about Cypher queries
- You work with Locy programs
- You need vector or hybrid search on a graph
- You ask about schema design, data ingestion, or query optimization for Uni

## Usage examples

Once installed, just ask your agent what you need. The skill provides context automatically.

**Build a schema:**
```
Design a schema for a social network with users, posts, and follows.
Include vector embeddings on posts for semantic search.
```

**Write queries:**
```
Write a Cypher query that finds friends-of-friends who liked
the same posts, ordered by overlap count.
```

**Set up vector search:**
```
Add hybrid search (vector + full-text) over a Document label
with a 768-dim embedding and an abstract field.
```

**Write Locy rules:**
```
Write Locy rules for transitive risk propagation through
a network of accounts connected by transfers.
```

## Updating the skill

To get the latest version after a Uni release, re-run the install command:

```bash
npx skills add https://github.com/rustic-ai/uni-db --skill uni-db-dev
```
