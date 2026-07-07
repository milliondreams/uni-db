# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team
#
# Regression test for FIXED defect bindings/uni-db/src/core.rs:317 —
# xervo_generate_core previously mapped ANY unrecognized message role to
# Message::user via a `_ => Message::user(content)` wildcard, so "System",
# "tool", " user", etc. were silently reinterpreted as user turns. It now
# rejects unknown roles with UniInvalidArgumentError before any model call.
#
# A valid role still fails later at the "runtime not configured" step (no
# offline text-generation provider exists in this environment); an unknown role
# is now rejected earlier, at the role mapping, regardless of backend config.

import pytest

import uni_db


def test_valid_role_reaches_runtime_and_fails_on_no_model():
    db = uni_db.UniBuilder.temporary().build()
    x = db.xervo()

    # A known role passes role validation and fails only at runtime config.
    with pytest.raises(uni_db.UniInternalError) as valid:
        x.generate("no_model", [{"role": "user", "content": "hi"}])
    assert "not configured" in str(valid.value)


@pytest.mark.parametrize("role", ["System", "tool", " user", "USER", "human"])
def test_unknown_role_is_rejected(role):
    db = uni_db.UniBuilder.temporary().build()
    x = db.xervo()

    # An unknown role is rejected with UniInvalidArgumentError, NOT silently
    # coerced to a user turn (core.rs:313-319).
    with pytest.raises(uni_db.UniInvalidArgumentError) as unknown:
        x.generate("no_model", [{"role": role, "content": "hi"}])
    assert "role" in str(unknown.value)
    # It fails at role validation, before the "runtime not configured" step.
    assert "not configured" not in str(unknown.value)
