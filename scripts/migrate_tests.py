#!/usr/bin/env python3
"""Classify and consolidate per-crate integration test files into a small
number of grouped binaries.

Phase 1 of the build-cost reduction plan. Each `tests/<group>.rs` becomes
a 3-line shim that pulls its members in via `#[path]` so each binary only
sees its own group's subtree.

Usage
-----
- `--crate <name> --dry-run` (default): print the classification table.
- `--crate <name> --execute --group <g>`: pilot move for one group.
- `--crate <name> --execute --all`: move every classifiable group.

Supported crates: `uni`, `uni-store`, `uni-query`.

Files in a crate's `keep_standalone` set stay as individual binaries
because CI references them by binary name (`cargo nextest run --test X`).
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


@dataclass
class CrateConfig:
    tests_dir: Path
    keep_standalone: set[str] = field(default_factory=set)
    # Ordered list of (group_name, filename predicate). First match wins, so
    # put narrow patterns before broad ones.
    groups: list[tuple[str, re.Pattern[str]]] = field(default_factory=list)


CRATE_CONFIGS: dict[str, CrateConfig] = {
    # --- crates/uni — the façade. 240 files → 15 group shims + 2 standalone.
    "uni": CrateConfig(
        tests_dir=REPO / "crates" / "uni" / "tests",
        keep_standalone={
            "reranker_integration.rs",   # ci.yml:104,122 — --features provider-onnx
            "hybrid_localstack_e2e.rs",  # ci.yml:191 — --run-ignored all
        },
        groups=[
            ("bugs", re.compile(r"^(bug_|issue_?\d+|issue4|issue5|repro_|test_issue_|test_overflow_fix|test_python_repro)")),
            ("cypher_path", re.compile(r"^(cypher_(qpp|shortest_path|var_length|match_vlp|match_edge_props)|vlp_|quantif|path_(functions|property|variable)_test|pattern_(comprehension|two_nodes))")),
            ("cypher_write", re.compile(r"^(cypher_(create|delete|merge|remove|set_advanced)|delete_vertex_test|df_mutation_test|bidirectional_create_test|bulk_loading_test|mutation_stress_test)")),
            ("cypher_read", re.compile(r"^(cypher_(aggregation|call|clauses_ext|copy|exists|filtering|gaps|limit_order|list_comprehension|optional|pushdown|reduce|subquery_parser|union|label_disjunction)|case_test|comparison_test|where_|with_clause|with_orderby|order_by_flow|return_expressions|map_(literal|projection)|null_handling|not_operator|dynamic_access|type_conversion|normalization_test|aggregate_window|window_execution|recursive_cte_execution|reduce_execution|explain_test|prepared_query|procedure_|pushdown_hydration|path_property_access)")),
            ("vector_search", re.compile(r"^(cypher_vector|vector_|hybrid_query|fts_integration|similar_to_integration|inverted_index_test)")),
            ("locy", re.compile(r"^(locy_|btic_cypher_test)")),
            ("fork", re.compile(r"^(fork_|session_clone_test)")),
            ("session_tx", re.compile(r"^(session_(read_only|template_test|test)|schema_(apply|dx|path_migration)|strict_schema_test|ddl_|constraint_enforcement_test|reserved_property_names_test)")),
            ("crdt", re.compile(r"^crdt_")),
            ("algo", re.compile(r"^algo_")),
            ("index", re.compile(r"^(adjacency_|composite_index|expression_index|indexing_integration|index_lifecycle|partial_index|uid_indexing|id_allocation|multi_label_|schemaless_|inline_properties|edge_type_properties)")),
            ("storage", re.compile(r"^(storage_|compaction_|async_flush|auto_flush|backup_test|delta_test|persistence_restart|wal_coordination|shutdown_test|writer_test|snapshot_|reader_isolation|parquet_export|overflow_json|embedding_stack_overflow)")),
            ("perf", re.compile(r"^(stress_test|test_100k_lookup|test_bulk_insert_performance|test_count_performance|query_(limits|streaming)_test)")),
            ("runtime", re.compile(r"^(hooks_test|notifications_test|metrics_test|admin_features_test|profile_test|error_handling_test|norn_gap_coverage)")),
            ("storage", re.compile(r"^(collection_types_test|created_updated_at_test|property_batch_test|property_manager|valid_at_test|vertex_edge_cases|test_list_labels)")),
            ("e2e", re.compile(r"^(e2e_comprehensive_test|notebook_examples|use_case_|multimodal_use_cases|api_|query_integration|subgraph_loading)")),
        ],
    ),
    # --- crates/uni-store — 39 files → 6 group shims + 1 standalone.
    "uni-store": CrateConfig(
        tests_dir=REPO / "crates" / "uni-store" / "tests",
        keep_standalone={
            "cloud_integration_test.rs",  # ci.yml:190 — workspace-level --test target
        },
        groups=[
            ("bugs", re.compile(r"^test_issue_")),
            ("crdt", re.compile(r"^crdt_")),
            ("fork_recovery", re.compile(r"^(fork_|recovery_|lance_branch_retention)")),
            ("cloud", re.compile(r"^(lancedb_integration_test|s3_compatibility_test)")),
            ("property", re.compile(r"^property_")),
            ("storage", re.compile(r"^(storage_|snapshot_manager_test|background_compaction_test|branched_backend_writes|wal_durability_test|overflow_json_tests|json_index_test)")),
        ],
    ),
    # --- crates/uni-query — 21 files → 5 group shims, no standalones.
    "uni-query": CrateConfig(
        tests_dir=REPO / "crates" / "uni-query" / "tests",
        keep_standalone=set(),
        groups=[
            ("parser", re.compile(r"^(parser_|.*_parser_test|ddl_parser_test)")),
            ("executor", re.compile(r"^executor_")),
            ("planner", re.compile(r"^(planner_edge_cases|pushdown_test|df_schemaless_integration)")),
            ("functions", re.compile(r"^(functions_test|bitwise_functions_test|btic_functions_test|datetime_functions_test|spatial_functions_test|list_comprehension_test|bitwise_parser_test|reduce_parser_test|recursive_cte_parser_test)")),
            ("integration", re.compile(r"^(json_fts_integration|quantified_path_test|property_tests)")),
        ],
    ),
}


@dataclass
class FileEntry:
    path: Path
    group: str  # one of the config's group names, "standalone", or "unclassified"


def classify(name: str, cfg: CrateConfig) -> str:
    if name in cfg.keep_standalone:
        return "standalone"
    for group, pat in cfg.groups:
        if pat.match(name):
            return group
    return "unclassified"


def gather(cfg: CrateConfig) -> list[FileEntry]:
    files = sorted(p for p in cfg.tests_dir.glob("*.rs") if p.is_file())
    return [FileEntry(path=p, group=classify(p.name, cfg)) for p in files]


def print_report(entries: list[FileEntry], cfg: CrateConfig) -> None:
    by_group: dict[str, list[FileEntry]] = defaultdict(list)
    for e in entries:
        by_group[e.group].append(e)

    print(f"\nTotal .rs files in {cfg.tests_dir.relative_to(REPO)}: {len(entries)}\n")
    print(f"{'group':<16} {'count':>5}")
    print("-" * 22)
    order = [g for g, _ in cfg.groups] + ["standalone", "unclassified"]
    for g in order:
        n = len(by_group.get(g, []))
        if n:
            print(f"{g:<16} {n:>5}")
    unclassified = by_group.get("unclassified", [])
    if unclassified:
        print("\nUNCLASSIFIED FILES (need rule additions):")
        for e in unclassified:
            print(f"  {e.path.name}")
    print("\nPer-group file listings:")
    for g in order:
        members = by_group.get(g, [])
        if not members or g == "unclassified":
            continue
        print(f"\n[{g}]  ({len(members)} files)")
        for e in members:
            print(f"  {e.path.name}")


INNER_ATTR_RE = re.compile(r"^#!\[.*\]\s*$", re.MULTILINE)


def extract_inner_attrs(path: Path) -> list[str]:
    """Return inner attrs (#![...]) from a file and strip them from disk."""
    text = path.read_text()
    attrs = INNER_ATTR_RE.findall(text)
    if attrs:
        path.write_text(INNER_ATTR_RE.sub("", text))
    return attrs


def execute_group(entries: list[FileEntry], group: str, cfg: CrateConfig) -> None:
    members = [e for e in entries if e.group == group]
    if not members:
        sys.exit(f"No files classified into group '{group}'.")
    common_dir = cfg.tests_dir / "common"
    target_dir = common_dir / group
    target_dir.mkdir(parents=True, exist_ok=True)
    submod_lines: list[str] = []
    hoisted_attrs: list[str] = []
    for e in members:
        new_name = e.path.stem
        dest = target_dir / f"{new_name}.rs"
        subprocess.run(
            ["git", "mv", str(e.path.relative_to(REPO)), str(dest.relative_to(REPO))],
            cwd=REPO,
            check=True,
        )
        hoisted_attrs.extend(extract_inner_attrs(dest))
        submod_lines.append(f"pub mod {new_name};")
    (target_dir / "mod.rs").write_text("\n".join(submod_lines) + "\n")
    shim = cfg.tests_dir / f"{group}.rs"
    seen: set[str] = set()
    unique_attrs = [a for a in hoisted_attrs if not (a in seen or seen.add(a))]
    attrs_block = ("\n".join(unique_attrs) + "\n") if unique_attrs else ""
    shim.write_text(
        attrs_block
        + f"// Auto-generated by scripts/migrate_tests.py — consolidates "
        + f"{len(members)} test files into one binary.\n"
        + f"#[path = \"common/{group}/mod.rs\"]\n"
        + f"mod {group};\n"
    )
    print(f"Moved {len(members)} files into {target_dir.relative_to(REPO)}/")
    print(f"Wrote shim {shim.relative_to(REPO)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--crate", default="uni", choices=sorted(CRATE_CONFIGS.keys()))
    ap.add_argument("--dry-run", action="store_true", default=True)
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--group", help="execute only this group (pilot mode)")
    ap.add_argument("--all", action="store_true", help="execute every classified group")
    args = ap.parse_args()

    cfg = CRATE_CONFIGS[args.crate]
    entries = gather(cfg)
    if not args.execute:
        print_report(entries, cfg)
        return
    if args.group:
        execute_group(entries, args.group, cfg)
    elif args.all:
        groups = sorted({e.group for e in entries if e.group not in {"standalone", "unclassified"}})
        for g in groups:
            execute_group(entries, g, cfg)
    else:
        sys.exit("--execute requires --group <name> or --all")


if __name__ == "__main__":
    main()
