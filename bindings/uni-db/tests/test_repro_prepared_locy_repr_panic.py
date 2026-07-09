# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team
#
# Regression guard for correctness-scan finding R16 / uni-db-bindings[3]
# (bindings/uni-db/src/types.rs) — PyPreparedLocy.__repr__ used to truncate the
# program text with a BYTE-index slice `&t[..60]`, which panics when byte 60
# falls inside a multibyte UTF-8 codepoint. Fixed (commit 1ea719890) to truncate
# by char boundary via `char_indices().nth(60)`, matching the sibling reprs.
#
# This test constructs the exact input that tripped the old byte-slice and
# asserts the repr now returns a clean truncated string instead of panicking.

import pytest

import uni_db


def test_prepared_locy_repr_truncates_on_char_boundary():
    db = uni_db.UniBuilder.temporary().build()
    s = db.session()

    # Build a VALID Locy program whose program_text() has a 2-byte 'é'
    # starting at byte 59, so byte 60 (its continuation byte) is not a char
    # boundary. pad=9 places 'é' exactly at byte 59 for this template — the old
    # `&t[..60]` slice landed mid-codepoint here and panicked.
    name = "x" * 9 + "é" + "y"
    prog = "CREATE RULE r AS MATCH (a:Person) WHERE a.name = '%s' YIELD KEY a" % name
    encoded = prog.encode()
    assert encoded.find(b"\xc3\xa9") == 59  # é starts at 59 => byte 60 mid-char

    pl = s.prepare_locy(prog)

    # Must NOT panic. The program text is > 60 chars, so the repr is truncated
    # with an ellipsis, and the multibyte 'é' straddling the old byte-60 cut
    # point is preserved intact rather than triggering a char-boundary panic.
    rendered = repr(pl)
    assert isinstance(rendered, str)
    assert rendered.startswith("PreparedLocy(")
    assert "..." in rendered  # long program was truncated
    assert "é" in rendered  # multibyte char survived the truncation boundary


def test_prepared_locy_repr_no_truncation_for_short_program():
    # A short program (< 60 chars) renders in full, no ellipsis.
    db = uni_db.UniBuilder.temporary().build()
    s = db.session()

    prog = "CREATE RULE r AS MATCH (a:A) YIELD KEY a"
    assert len(prog) < 60
    pl = s.prepare_locy(prog)

    rendered = repr(pl)
    assert rendered.startswith("PreparedLocy(")
    assert "..." not in rendered
