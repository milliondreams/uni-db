# Locy Architecture (Internals)

## Scope

This page describes how Locy is integrated into Uni from parser to runtime execution.

## End-to-End Flow

1. Parse Locy source into Locy AST (`parse_locy`).
2. Compile into stratified `CompiledProgram`.
3. Build logical program plan (`LocyPlanBuilder`).
4. Produce physical execution plan (`LocyProgramExec` and related operators).
5. Execute strata to fixpoint.
6. Dispatch command phase (`QUERY`, `EXPLAIN`, `ABDUCE`, `DERIVE`, `ASSUME`).
7. Map outputs into `LocyResult`.

## Key Components

- `uni-cypher` for Locy grammar and AST.
- `uni-locy` for compiler and command/result types.
- `uni` API integration via `LocyEngine`.
- `uni-query` DataFusion path for native execution and derived-store handling.

## Why This Shape

- Reuse Uni query engine and storage infrastructure.
- Keep semantics explicit in compiler phases.
- Preserve explainability and bounded execution controls.
