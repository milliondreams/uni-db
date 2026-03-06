# Locy

Locy is Uni's logic programming layer: **Logic + Cypher**. It extends OpenCypher with recursive rules, hypothetical reasoning, graph derivation, and abductive remediation workflows.

## Who This Is For

- Application engineers who need graph reasoning beyond one-shot queries.
- Data and policy teams modeling compliance, access control, and propagation rules.
- Advanced users who want compiler/runtime internals and TCK-level semantics.

## What You Get

- Declarative rules with recursion (`CREATE RULE`).
- Goal-directed evaluation (`QUERY`).
- Hypothetical analysis (`ASSUME ... THEN`).
- Remediation search (`ABDUCE`).
- Derivation/proof explainability (`EXPLAIN RULE`).
- Graph materialization (`DERIVE`).

## Start Here

1. Read [Foundations](foundations.md).
2. Run the [Quickstart](quickstart.md).
3. Learn syntax in [Language Guide](language-guide.md).
4. Move to [Advanced Features](advanced/along-fold-bestby.md).

## Locy vs Plain Cypher

Use plain Cypher when you need direct reads/writes on existing graph state. Use Locy when you need:

- Recursive closure and fixed-point reasoning.
- Explainable inferred facts.
- What-if analysis with rollback boundaries.
- Suggested minimal changes to satisfy or prevent outcomes.

## API Entry Points

- Rust: `db.locy().evaluate(...)` and `db.locy().evaluate_with_config(...)`.
- Python: `db.locy_evaluate(...)` and `await adb.locy_evaluate(...)`.

See [Rust API Integration](api/rust.md) and [Python API Integration](api/python.md).
