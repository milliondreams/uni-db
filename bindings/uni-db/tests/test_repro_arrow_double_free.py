# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team
#
# Regression test for a FIXED Critical memory-safety defect
# bindings/uni-db/src/builders.rs:2202 — record_batch_from_pyarrow used
# std::ptr::read on the Arrow C Data Interface structs held by the pyarrow
# capsules WITHOUT nulling the source `release` callback. Rust took ownership
# of the read-out FFI_ArrowArray (and later released it), while the pyarrow
# capsule destructor still saw release != NULL and released the SAME buffers
# again => double free / use-after-free.
#
# The fix imports the structs via FFI_Arrow{Schema,Array}::from_raw, which
# moves the value out AND nulls the capsule-owned `release`, so the producer's
# destructor becomes a no-op. This test drives a real write_batch ingest in an
# isolated subprocess and asserts it completes cleanly (exit 0, "NO_CRASH"),
# keeping the parent test process (and CI) alive if a regression re-aborts it.

import subprocess
import sys
import textwrap

_CHILD = textwrap.dedent(
    """
    import pyarrow as pa
    import uni_db

    db = uni_db.UniBuilder.temporary().build()
    (db.schema().label("Person")
        .property("name", "string").property("age", "int").done().apply())
    s = db.session()
    tx = s.tx()
    app = tx.appender("Person")
    batch = pa.record_batch({
        "name": pa.array(["Alice", "Bob", "Carol"]),
        "age": pa.array([30, 25, 40], type=pa.int64()),
    })
    app.write_batch(batch)   # <-- record_batch_from_pyarrow double-frees here
    app.finish()
    tx.commit()
    print("NO_CRASH")
    """
)


def test_write_batch_does_not_double_free():
    proc = subprocess.run(
        [sys.executable, "-c", _CHILD],
        capture_output=True,
        text=True,
        timeout=120,
    )
    combined = proc.stdout + proc.stderr
    # A correct ingest exits 0 and prints "NO_CRASH". A regression re-introducing
    # the double-free would abort with SIGABRT (returncode -6 / 134) after
    # "double free detected" / "corruption". (builders.rs:2202)
    assert proc.returncode == 0, f"rc={proc.returncode}\n{combined}"
    assert "NO_CRASH" in proc.stdout, combined
    assert "double free" not in combined.lower(), combined
    assert "corruption" not in combined.lower(), combined
