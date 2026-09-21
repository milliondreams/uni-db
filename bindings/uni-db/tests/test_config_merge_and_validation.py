"""`Uni.builder().config({...})` must merge, and must refuse a key it cannot honour.

Both behaviours replace a silent failure.

`config()` used to assign a value built from `UniConfig::default()`, so
`.batch_size(4096).config({"query_timeout": 11})` put `batch_size` back to its
default without saying anything. And the extractor read the keys it knew and
ignored the rest, so a misspelled `quer_timeout` -- or a real `UniConfig` field
with no Python binding, such as `ssi_enabled` -- was accepted and dropped. In
both cases the caller asked for a setting, got no error, and got the default.
"""

from __future__ import annotations

import pytest

import uni_db


def _builder(tmp_path, name="db"):
    return uni_db.UniBuilder.open(str(tmp_path / name))


# --- rejecting keys we cannot honour ---------------------------------------


@pytest.mark.parametrize(
    "key, value",
    [
        ("quer_timeout", 5.0),  # misspelling of a real key
        ("totally_not_a_real_key", 123),  # nothing like a real key
        ("ssi_enabled", False),  # a real UniConfig field with no binding
        ("commit_timeout", 5.0),  # ditto
    ],
)
def test_unknown_config_key_is_refused(tmp_path, key, value):
    with pytest.raises(ValueError) as excinfo:
        _builder(tmp_path).config({key: value})
    message = str(excinfo.value)
    assert key in message, f"the error must name the offending key: {message}"
    # Naming the accepted set is what turns a refusal into a fix.
    assert "query_timeout" in message, (
        f"the error must list the accepted keys: {message}"
    )


def test_a_valid_key_alongside_an_invalid_one_still_refuses(tmp_path):
    """Partial application would be the worst outcome: some settings taken,
    others dropped, and no error."""
    with pytest.raises(ValueError):
        _builder(tmp_path).config({"query_timeout": 5.0, "nonsense": 1})


def test_every_accepted_key_is_accepted(tmp_path):
    """The complement of the test above, and the reason it is not vacuous:
    a validator that refused everything would satisfy the refusal tests."""
    from datetime import timedelta

    accepted = {
        "query_timeout": 5.0,
        "max_query_memory": 256 * 1024 * 1024,
        "parallelism": 2,
        "cache_size": 64 * 1024 * 1024,
        "max_transaction_memory": 64 * 1024 * 1024,
        "batch_size": 2048,
        "wal_enabled": True,
        "strict_schema": False,
        "max_forks": 4,
        "fork_default_ttl": timedelta(hours=1),
        "fork_sweeper_interval": timedelta(seconds=30),
        "disable_fork_sweeper": True,
    }
    # One at a time, so a failure names the key rather than the whole dict.
    for key, value in accepted.items():
        _builder(tmp_path, f"db_{key}").config({key: value})
    # And all together.
    _builder(tmp_path, "db_all").config(accepted).build()


# --- merging ----------------------------------------------------------------


def _rejects_undeclared_label(db) -> bool:
    """Whether `strict_schema` is in force on a built database.

    `strict_schema` is the one setting reachable both as a builder method and as
    a config key whose effect is *observable* from Python, which is what makes
    the merge tests below discriminating: it defaults to `false`, so if a
    `config()` call discarded it the write below succeeds.
    """
    session = db.session()
    try:
        with session.tx() as tx:
            tx.execute("CREATE (:NeverDeclared {x: 1})")
    except Exception:
        return True
    return False


def test_config_does_not_discard_an_earlier_setter(tmp_path):
    """`.strict_schema(True)` before `.config({...})` must survive.

    This is the regression itself: `config()` assigned a freshly-defaulted
    config, so the earlier setter was silently reverted and the write below
    succeeded.
    """
    db = _builder(tmp_path).strict_schema(True).config({"query_timeout": 11.0}).build()
    assert _rejects_undeclared_label(db), (
        "strict_schema(True) was discarded by the later config() call"
    )


def test_a_setter_after_config_also_survives(tmp_path):
    """The mirror order, which the old code happened to get right."""
    db = _builder(tmp_path).config({"query_timeout": 11.0}).strict_schema(True).build()
    assert _rejects_undeclared_label(db)


def test_repeated_config_calls_accumulate(tmp_path):
    """A second `config()` must not erase the first."""
    db = (
        _builder(tmp_path)
        .config({"strict_schema": True})
        .config({"batch_size": 2048})
        .build()
    )
    assert _rejects_undeclared_label(db), "the second config() call discarded the first"


def test_the_last_write_to_one_setting_wins(tmp_path):
    """Merging must not mean the first write to a key sticks."""
    db = (
        _builder(tmp_path)
        .config({"strict_schema": True})
        .config({"strict_schema": False})
        .build()
    )
    assert not _rejects_undeclared_label(db), (
        "the later write to strict_schema did not win"
    )
