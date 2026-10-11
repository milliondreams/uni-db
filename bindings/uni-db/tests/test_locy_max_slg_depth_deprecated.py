"""`LocyConfig.max_slg_depth` is deprecated: it has no effect.

It bounded Locy's in-memory resolver, which no longer exists (every command
reads the facts the fixpoint published). Setting or reading it still works so
existing configurations run, and warns so callers can drop it.
"""

import warnings

import pytest

import uni_db


def test_setting_max_slg_depth_in_the_constructor_warns():
    with pytest.warns(DeprecationWarning, match="max_slg_depth"):
        config = uni_db.LocyConfig(max_slg_depth=5)
    with pytest.warns(DeprecationWarning, match="max_slg_depth"):
        assert config.max_slg_depth == 5


def test_setting_max_slg_depth_in_a_config_dict_warns():
    db = uni_db.Uni.in_memory()
    with pytest.warns(DeprecationWarning, match="max_slg_depth"):
        db.session().locy_with("CREATE RULE r AS MATCH (n) YIELD KEY n").with_config(
            {"max_slg_depth": 5}
        ).run()


def test_a_config_without_it_does_not_warn():
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        uni_db.LocyConfig(max_iterations=10)
        db = uni_db.Uni.in_memory()
        db.session().locy_with("CREATE RULE r AS MATCH (n) YIELD KEY n").with_config(
            {"max_iterations": 10}
        ).run()
