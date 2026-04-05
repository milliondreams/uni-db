#!/bin/bash
# Verify flagship Locy notebooks locally.
#
# Builds an editable uni-db wheel into an isolated venv, then runs each
# flagship notebook verification script against it.
#
# Usage:
#   scripts/verify_notebooks.sh            # verify all 3 notebooks
#   scripts/verify_notebooks.sh --check    # only check generators are up to date (no execution)
#
# Requirements: uv, maturin (installed via uv), Rust toolchain

set -e
cd "$(dirname "$0")/.."

# ── Check-only mode: just verify generators ──────────────────────────
if [ "${1:-}" = "--check" ]; then
    echo "Checking notebook generators are up to date..."
    python3 website/scripts/generate_locy_notebooks.py --check
    python3 website/scripts/generate_semiconductor_flagship_notebook.py --check
    python3 website/scripts/generate_pharma_flagship_notebook.py --check
    python3 website/scripts/generate_cyber_flagship_notebook.py --check
    echo "All notebook generators up to date."
    exit 0
fi

# ── Full verification: build wheel + execute notebooks ───────────────

VENV_DIR=".venv-notebooks"

cleanup() {
    rm -rf "$VENV_DIR"
}
trap cleanup EXIT

echo "Creating isolated venv..."
uv venv "$VENV_DIR" --quiet

echo "Installing uni-db (maturin develop)..."
cd bindings/uni-db
VIRTUAL_ENV="../../$VENV_DIR" maturin develop --uv --quiet 2>&1 | tail -1
cd ../..

echo ""
echo "Running flagship notebook verifications..."
echo ""

PASS=0
FAIL=0

run_notebook() {
    local name="$1"
    local script="$2"
    echo -n "  $name ... "
    if "$VENV_DIR/bin/python" "$script" > /dev/null 2>&1; then
        echo "PASS"
        PASS=$((PASS + 1))
    else
        echo "FAIL"
        "$VENV_DIR/bin/python" "$script" 2>&1 | tail -5
        FAIL=$((FAIL + 1))
    fi
}

run_notebook "Semiconductor yield" "website/scripts/verify_semiconductor_flagship_notebook.py"
run_notebook "Pharma batch genealogy" "website/scripts/verify_pharma_flagship_notebook.py"
run_notebook "Cyber exposure twin" "website/scripts/verify_cyber_flagship_notebook.py"

echo ""
echo "Results: $PASS passed, $FAIL failed"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
