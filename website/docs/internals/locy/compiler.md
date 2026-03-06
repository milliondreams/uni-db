# Locy Compiler Internals

## Responsibilities

The compiler transforms Locy AST into an executable, validated, stratified program.

## Main Stages

1. Module context resolution.
2. Rule grouping and dependency graph construction.
3. Type and schema checks.
4. Wardedness and safety checks.
5. Stratification and topological ordering.
6. Command validation and binding checks.

## Key Guarantees

- No undefined rule references.
- No cyclic negation.
- Output schema consistency for overloaded rules.
- Guardrails for recursive aggregation semantics.

## Outputs

- `CompiledProgram`
- Ordered strata and command list
- Validation-ready metadata for execution
