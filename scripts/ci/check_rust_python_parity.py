#!/usr/bin/env python3
"""Assert the Rust surface and the Python bindings have not drifted apart.

Four guards already police the Python bindings, and every one of them compares
Python to Python: `test_sync_async_parity.py` (sync class vs async twin),
`test_stub_drift.py` (runtime vs `__init__.pyi`, both directions), `stubtest`,
and `check_doc_symbols.py`. Nothing compares Rust to Python, so a field or an
error variant added on the Rust side and never bound is not a drift failure --
it is an absence, and absence is exactly what none of those detect.

That is how two defects reached a release. `UniConfig` grew to 40 public fields
while `config({...})` understood 12, and the extractor accepted an unrecognised
key in silence, so `config({"ssi_enabled": False})` asked for a setting, raised
nothing, and did nothing.

This does not demand that every Rust item be bound -- most of the unbound ones
are deliberate. It demands the gap be *written down*. A new field or variant
fails this check until somebody either binds it or adds it to the allowlist
below with a reason, which turns an invisible omission into a reviewed decision.

Exit code 0 = no undeclared drift, 1 = an item is neither bound nor allowlisted.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONFIG_RS = ROOT / "crates/uni-common/src/config.rs"
CONVERT_RS = ROOT / "bindings/uni-db/src/convert.rs"
ERROR_RS = ROOT / "crates/uni-common/src/api/error.rs"
EXCEPTIONS_RS = ROOT / "bindings/uni-db/src/exceptions.rs"

# ---------------------------------------------------------------------------
# Allowlists: a Rust item deliberately not reachable from Python.
#
# Each entry needs a reason. "Not bound yet" is a reason; the point is that
# somebody looked. Deleting an entry whose item got bound is the other half --
# a stale allowlist entry is reported too, so this cannot rot into a rubber
# stamp.
# ---------------------------------------------------------------------------
CONFIG_NOT_BOUND = {
    "max_frontier_size": "traversal safety cap; no user has asked to tune it",
    "auto_flush_threshold": "flush tuning, Rust-embedder concern",
    "auto_flush_interval": "flush tuning, Rust-embedder concern",
    "auto_flush_min_mutations": "flush tuning, Rust-embedder concern",
    "commit_channel_capacity": "internal channel sizing",
    "commit_timeout": "not bound yet; reachable need, no binding written",
    "compaction": "nested struct; needs a Python dataclass to bind well",
    "throttle": "nested struct; needs a Python dataclass to bind well",
    "file_sandbox": "nested struct; security policy set by the embedder",
    "object_store": "nested struct; Python uses cloud_config() instead",
    "index_rebuild": "nested struct; needs a Python dataclass to bind well",
    "max_compaction_rows": "compaction OOM guard, Rust-embedder concern",
    "max_recursive_cte_iterations": "not bound yet",
    "partial_lance_writes": "storage-internal toggle",
    "defer_embeddings": "not bound yet",
    "fork_fragment_warn_threshold": "operational warning threshold",
    "tx_id_reservoir_batch": "internal id allocation",
    "async_flush_enabled": "flush tuning, Rust-embedder concern",
    "max_pending_flushes": "flush tuning, Rust-embedder concern",
    "flush_stream_timeout": "flush tuning, Rust-embedder concern",
    "drop_fork_drain_timeout": "fork shutdown tuning",
    "flush_drain_timeout": "flush tuning, Rust-embedder concern",
    "fork_index_build_threshold": "fork index auto-builder tuning",
    "fork_index_builder_interval": "fork index auto-builder tuning",
    "disable_fork_index_builder": "test-only knob",
    "ssi_enabled": "not bound yet; defaults on, and turning it off is unsafe",
    "ec_flush_interval": "eventually-consistent flush tuning",
    "ec_flush_threshold": "eventually-consistent flush tuning",
}

ERROR_NO_DEDICATED_EXCEPTION = {
    "TransactionRollbackOnly": "collapses to UniError; no payload to lose",
    "GraphComputeIncomplete": "not bound yet; LocyIncomplete has one, this does not",
    "TriggerRejected": "triggers are a Rust-side surface",
    "AuthenticationFailed": "auth is not reachable through the Python entry points",
    "AuthorizationDenied": "auth is not reachable through the Python entry points",
    "EphemeralWriteAttempt": "scratch sessions are not bound to Python",
    "ForkNameInvalid": "collapses to UniError; message carries the reason",
    "PendingFlushTimeout": "not bound yet; the only fork-lifecycle variant without one",
    "ForkWritesNotYetSupported": "explicitly routed to base UniError",
}


def fail(lines: list[str]) -> None:
    print("\n".join(lines), file=sys.stderr)
    sys.exit(1)


def uni_config_fields() -> list[str]:
    """Public field names of `struct UniConfig`, in declaration order."""
    src = CONFIG_RS.read_text()
    m = re.search(r"pub struct UniConfig\s*\{(.*?)\n\}", src, re.S)
    if not m:
        fail(["could not locate `pub struct UniConfig` in %s" % CONFIG_RS])
    return re.findall(r"^\s+pub ([a-z_0-9]+):", m.group(1), re.M)


def python_config_keys() -> list[str]:
    """Keys listed in `UNI_CONFIG_KEYS`, the extractor's source of truth."""
    src = CONVERT_RS.read_text()
    m = re.search(r"pub const UNI_CONFIG_KEYS: &\[&str\] = &\[(.*?)\];", src, re.S)
    if not m:
        fail(["could not locate `UNI_CONFIG_KEYS` in %s" % CONVERT_RS])
    return re.findall(r'"([a-z_0-9]+)"', m.group(1))


def error_variants() -> list[str]:
    """Variant names of the public `UniError` enum.

    Non-greedy to the first closing brace at column zero: `error.rs` declares
    two more enums after this one (`LocyIncompleteReason`,
    `GraphComputeIncompleteReason`), and a greedy match swallows their variants
    too -- which showed up as `Timeout` and `IterationLimit` appearing twice.
    """
    src = ERROR_RS.read_text()
    m = re.search(r"pub enum UniError\s*\{(.*?)\n\}", src, re.S)
    if not m:
        fail(["could not locate `pub enum UniError` in %s" % ERROR_RS])
    # A variant is a CamelCase identifier at one indent level, not inside a
    # `#[...]` attribute or a doc comment.
    return re.findall(r"^    ([A-Z][A-Za-z0-9]*)\s*[({,]", m.group(1), re.M)


def mapped_error_variants() -> set[str]:
    """Variants mapped to a *dedicated* Python exception in `uni_error_to_pyerr`.

    The arms are bare identifiers, not `UniError::X` -- the function opens with
    `use uni_common::UniError::*`. Matching the qualified form finds nothing and
    reports every variant as unmapped, which is how this was first written.
    """
    src = EXCEPTIONS_RS.read_text()
    m = re.search(r"fn uni_error_to_pyerr\b.*?\n    match e \{(.*?)\n    \}", src, re.S)
    if not m:
        fail(["could not locate the `match e` block of `uni_error_to_pyerr` in %s"
              % EXCEPTIONS_RS])
    arms = re.findall(
        # `=>` is followed by a block for every payload-carrying arm, so the
        # optional `{` is load-bearing: without it those arms match nothing and
        # are silently dropped, which reported the nine richest exceptions
        # (`ForkInUse`, `LocyIncomplete`, ...) as unbound.
        r"^        ([A-Z][A-Za-z0-9]*)[^=]*=>\s*\{?\s*([A-Za-z_][A-Za-z0-9_]*)",
        m.group(1),
        re.M,
    )
    # An arm that routes to the base `UniError` is not a dedicated exception --
    # it lands the caller in exactly the same place as the `_ =>` wildcard, so
    # counting it as bound would let a variant look handled while its payload
    # and its type are both gone. `ForkWritesNotYetSupported` is written as an
    # explicit arm and is one of these.
    return {variant for variant, target in arms if target != "UniError"}


def check(kind: str, rust_items: list[str], bound: set[str],
          allowlist: dict[str, str]) -> list[str]:
    problems: list[str] = []

    undeclared = [i for i in rust_items if i not in bound and i not in allowlist]
    if undeclared:
        problems.append(
            f"{kind}: {len(undeclared)} item(s) exist in Rust, are not reachable from\n"
            f"  Python, and are not allowlisted. Bind them, or add them to\n"
            f"  {Path(__file__).name} with a reason:\n"
            + "".join(f"    - {i}\n" for i in undeclared)
        )

    # A stale entry means the item got bound and nobody cleaned up; left alone,
    # the allowlist slowly becomes a place where anything can hide.
    stale = [i for i in allowlist if i in bound]
    if stale:
        problems.append(
            f"{kind}: {len(stale)} allowlist entr(ies) are now reachable from Python.\n"
            f"  Remove them from {Path(__file__).name}:\n"
            + "".join(f"    - {i}\n" for i in stale)
        )

    phantom = [i for i in allowlist if i not in rust_items]
    if phantom:
        problems.append(
            f"{kind}: {len(phantom)} allowlist entr(ies) name something that no longer\n"
            f"  exists in Rust. Remove them from {Path(__file__).name}:\n"
            + "".join(f"    - {i}\n" for i in phantom)
        )
    return problems


def main() -> int:
    fields = uni_config_fields()
    keys = python_config_keys()
    variants = error_variants()
    mapped = mapped_error_variants()

    # Guard against the checker itself going quiet: a regex that stops matching
    # would otherwise report a clean run over an empty set.
    if len(fields) < 20 or len(variants) < 20 or len(mapped) < 20:
        fail([
            "parity check parsed implausibly little and would pass vacuously:",
            f"  UniConfig fields parsed: {len(fields)}",
            f"  UniError variants parsed: {len(variants)}",
            f"  mapped exception arms parsed: {len(mapped)}",
            "Fix the parser rather than the threshold.",
        ])

    # Over-capture is the other parser failure, and it does not look like one:
    # a greedy enum match pulled in two neighbouring enums and duplicated
    # `Timeout` and `IterationLimit`. A Rust enum cannot repeat a variant name,
    # so a duplicate means the regex left its enum.
    dupes = sorted({v for v in variants if variants.count(v) > 1})
    if dupes:
        fail([
            "parity check over-captured: repeated UniError variant(s) "
            + ", ".join(dupes),
            "A Rust enum cannot repeat a variant, so the body regex ran past "
            "the enum. Fix the parser.",
        ])

    # A key accepted by Python that is not a real field is the mirror failure.
    unknown_keys = [k for k in keys if k not in fields]
    problems: list[str] = []
    if unknown_keys:
        problems.append(
            "config: UNI_CONFIG_KEYS names key(s) that are not UniConfig fields:\n"
            + "".join(f"    - {k}\n" for k in unknown_keys)
        )

    problems += check("config", fields, set(keys), CONFIG_NOT_BOUND)
    problems += check("error", variants, mapped, ERROR_NO_DEDICATED_EXCEPTION)

    if problems:
        fail(["Rust <-> Python parity drift:", ""] + problems)

    print(
        f"OK: {len(keys)}/{len(fields)} UniConfig fields reachable from Python "
        f"({len(CONFIG_NOT_BOUND)} allowlisted); "
        f"{len(mapped)}/{len(variants)} UniError variants mapped to a dedicated "
        f"exception ({len(ERROR_NO_DEDICATED_EXCEPTION)} allowlisted)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
