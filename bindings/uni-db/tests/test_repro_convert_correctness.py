# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team
#
# Regression tests for four FIXED correctness defects in the PyO3 value-
# conversion layer (bindings/uni-db/src/convert.rs). Each test drives the REAL
# public API (Uni / Session / tx.execute with params) and asserts the CORRECT
# post-fix behavior.

import datetime
import os
import time

import pytest

import uni_db


def _db_with_prop():
    return uni_db.UniBuilder.temporary().build()


# ---------------------------------------------------------------------------
# convert.rs — naive datetime is stored wall-clock-as-UTC (matching the core's
# LocalDateTime semantics), NOT shifted by the host's local UTC offset.
# ---------------------------------------------------------------------------
def test_naive_datetime_preserves_wall_clock():
    prev_tz = os.environ.get("TZ")
    os.environ["TZ"] = "America/New_York"  # UTC-4 in July
    time.tzset()
    try:
        offset = datetime.datetime(2026, 7, 4, 12, 0, 0).astimezone().utcoffset()
        assert offset != datetime.timedelta(0), "test host TZ did not apply"

        db = _db_with_prop()
        s = db.session()
        tx = s.tx()
        naive = datetime.datetime(2026, 7, 4, 12, 0, 0)
        tx.execute("CREATE (n:Ev {t: $t})", {"t": naive})
        tx.commit()

        rendered = s.query("MATCH (n:Ev) RETURN toString(n.t) AS s")[0]["s"]
        # Wall-clock preserved regardless of host timezone.
        assert rendered == "2026-07-04T12:00", rendered

        # An equality query against the same wall-clock literal matches.
        match = s.query(
            'MATCH (n:Ev) WHERE n.t = localdatetime("2026-07-04T12:00:00") RETURN n'
        )
        assert len(match) == 1, len(match)
    finally:
        if prev_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = prev_tz
        time.tzset()


# ---------------------------------------------------------------------------
# convert.rs — unrecognized Python types raise TypeError instead of silently
# becoming Value::Null (which would drop the caller's data without warning).
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("value", [(1, 2, 3), {1, 2, 3}, frozenset({1, 2})])
def test_unsupported_param_type_raises(value):
    db = _db_with_prop()
    s = db.session()
    tx = s.tx()
    with pytest.raises(TypeError):
        tx.execute("CREATE (n:X {p: $v})", {"v": value})


# ---------------------------------------------------------------------------
# convert.rs — tz-aware datetime.time converts successfully. Previously it
# called t.utcoffset(None), but time.utcoffset() takes NO args, raising
# TypeError for every aware time.
# ---------------------------------------------------------------------------
def test_tz_aware_time_converts():
    db = _db_with_prop()
    s = db.session()
    tx = s.tx()
    aware_time = datetime.time(12, 30, 0, tzinfo=datetime.timezone.utc)
    tx.execute("CREATE (n:T {v: $v})", {"v": aware_time})
    tx.commit()
    back = s.query("MATCH (n:T) RETURN n.v AS v")[0]["v"]
    assert isinstance(back, datetime.time), repr(back)
    assert back.hour == 12 and back.minute == 30, repr(back)
    assert back.utcoffset() == datetime.timedelta(0), repr(back.utcoffset())


# ---------------------------------------------------------------------------
# convert.rs — datetime nanos computed with exact integer arithmetic, so
# microsecond precision round-trips exactly at modern epochs (previously the
# f64 ((ts*1e9) as i64) scaling corrupted ~half of all microsecond values).
# ---------------------------------------------------------------------------
def test_datetime_microsecond_precision_exact():
    db = _db_with_prop()
    s = db.session()
    tx = s.tx()
    val = datetime.datetime(2026, 7, 5, 12, 34, 56, 2)  # microsecond == 2
    tx.execute("CREATE (n:P {t: $t})", {"t": val})
    tx.commit()
    back = s.query("MATCH (n:P) RETURN n.t AS t")[0]["t"]
    assert back.microsecond == 2, back.microsecond
