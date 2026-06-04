#!/usr/bin/env python3
"""Guard: every Python wheel-variant crate must enable the plugin loader
features its shared binding source calls unconditionally.

All `bindings/uni-db*` variants reuse `bindings/uni-db/src/*.rs`, which calls
the Rhai and PyO3 plugin loaders (`load_rhai_plugin`, `add_python_plugin`,
`finalize_python_plugin`, `load_python_plugin`) WITHOUT `#[cfg]` guards. Those
methods are gated on the core `uni-db` crate behind `rhai-plugins` (in its
default feature set) and `pyo3-plugins` (always opt-in). A variant that uses
`default-features = false` (the slim `*-onnx*` wheels) drops `rhai-plugins`,
and any variant that forgets `pyo3-plugins` fails to compile — but only in the
wheel build, which CI's `nextest` run never exercises. This check encodes the
invariant so the drift is caught on every push instead of at release time.

Exits non-zero and prints the offending variants if the invariant is violated.
"""
from __future__ import annotations

import sys
import tomllib
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

# Features the shared binding source requires on the `uni-db` dependency.
REQUIRED = {"rhai-plugins", "pyo3-plugins"}

# Wheel-variant binding crates (each has its own Cargo.toml feature set).
VARIANT_DIRS = [
    "bindings/uni-db",
    "bindings/uni-db-onnx",
    "bindings/uni-db-onnx-cuda",
    "bindings/uni-db-onnx-metal",
    "bindings/uni-db-cuda",
    "bindings/uni-db-metal",
]


def core_default_features() -> set[str]:
    """Features directly listed in core `uni-db`'s `default` set."""
    core = tomllib.loads((REPO / "crates/uni/Cargo.toml").read_text())
    return set(core.get("features", {}).get("default", []))


def effective_uni_db_features(variant_dir: str, defaults: set[str]) -> set[str]:
    manifest = tomllib.loads((REPO / variant_dir / "Cargo.toml").read_text())
    dep = manifest["dependencies"]["uni-db"]
    if not isinstance(dep, dict):
        # bare `uni-db = "x"` form — gets defaults, no explicit features.
        return set(defaults)
    explicit = set(dep.get("features", []))
    uses_default = dep.get("default-features", True)
    return explicit | (defaults if uses_default else set())


def main() -> int:
    defaults = core_default_features()
    failures: list[str] = []
    for variant in VARIANT_DIRS:
        eff = effective_uni_db_features(variant, defaults)
        missing = REQUIRED - eff
        status = "OK" if not missing else f"MISSING {sorted(missing)}"
        print(f"{variant:32} {status}")
        if missing:
            failures.append(f"{variant}: uni-db dependency is missing {sorted(missing)}")

    if failures:
        print("\nERROR: wheel-variant plugin-feature invariant violated:", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        print(
            "\nThe shared binding source (bindings/uni-db/src) calls the Rhai/PyO3 "
            "plugin loaders unconditionally, so every variant's `uni-db` dependency "
            "must enable both `rhai-plugins` and `pyo3-plugins` (explicitly when it "
            "uses `default-features = false`).",
            file=sys.stderr,
        )
        return 1

    print(f"\nAll {len(VARIANT_DIRS)} wheel variants enable {sorted(REQUIRED)}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
