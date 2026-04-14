# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for BTIC temporal interval Python bindings."""

import pytest

import uni_db
from uni_db import Btic, DataType, Value

# =============================================================================
# PyBtic Construction & Properties
# =============================================================================


class TestBticConstruction:
    """Test Btic construction from string literals."""

    def test_year(self):
        b = Btic("1985")
        assert b.lo_granularity == "year"
        assert b.is_finite

    def test_month(self):
        b = Btic("1985-03")
        assert b.lo_granularity == "month"

    def test_day(self):
        b = Btic("1985-03-15")
        assert b.lo_granularity == "day"

    def test_range(self):
        b = Btic("1985-03/2024-06")
        assert b.is_finite
        assert b.lo < b.hi

    def test_approximate(self):
        b = Btic("~1985")
        assert b.lo_certainty == "approximate"

    def test_uncertain(self):
        b = Btic("?1985")
        assert b.lo_certainty == "uncertain"

    def test_bce(self):
        b = Btic("500 BCE")
        assert b.lo < 0  # Before epoch
        assert b.is_finite

    def test_unbounded_right(self):
        b = Btic("2020-03/")
        assert b.is_unbounded
        assert not b.is_finite

    def test_fully_unbounded(self):
        b = Btic("/")
        assert b.is_unbounded

    def test_invalid_literal(self):
        with pytest.raises(ValueError):
            Btic("not-a-date")

    def test_from_raw_roundtrip(self):
        b1 = Btic("1985")
        b2 = Btic.from_raw(b1.lo, b1.hi, b1.meta)
        assert b1 == b2

    def test_from_raw_invalid(self):
        with pytest.raises(ValueError):
            # lo > hi is invalid
            Btic.from_raw(100, 50, 0)


class TestBticProperties:
    """Test Btic property accessors."""

    def test_lo_hi_meta(self):
        b = Btic("1985")
        assert isinstance(b.lo, int)
        assert isinstance(b.hi, int)
        assert isinstance(b.meta, int)
        assert b.lo < b.hi

    def test_duration_ms(self):
        b = Btic("1985")
        dur = b.duration_ms
        assert dur is not None
        assert dur > 0

    def test_duration_ms_unbounded(self):
        b = Btic("/")
        assert b.duration_ms is None

    def test_is_instant(self):
        b = Btic("1985")
        assert not b.is_instant  # A year is not an instant

    def test_granularity(self):
        b = Btic("1985-03-15")
        assert b.lo_granularity == "day"
        assert b.hi_granularity == "day"

    def test_certainty_definite(self):
        b = Btic("1985")
        assert b.lo_certainty == "definite"
        assert b.hi_certainty == "definite"


# =============================================================================
# Allen Predicates & Set Operations
# =============================================================================


class TestBticPredicates:
    """Test Allen interval algebra predicates."""

    def test_overlaps(self):
        a = Btic("1980/1990")
        b = Btic("1985/1995")
        assert a.overlaps(b)
        assert b.overlaps(a)

    def test_disjoint(self):
        a = Btic("1980/1985")
        b = Btic("1990/1995")
        assert a.disjoint(b)
        assert not a.overlaps(b)

    def test_contains(self):
        outer = Btic("1980/2000")
        inner = Btic("1985/1990")
        assert outer.contains(inner)
        assert not inner.contains(outer)

    def test_contains_point(self):
        b = Btic("1985")
        # A point inside the year 1985
        mid = b.lo + (b.hi - b.lo) // 2
        assert b.contains_point(mid)
        # A point far outside
        assert not b.contains_point(0)

    def test_before_after(self):
        a = Btic("1980/1985")
        b = Btic("1990/1995")
        assert a.before(b)
        assert b.after(a)

    def test_meets(self):
        # Use from_raw for exact boundary control — solidus parsing expands granular bounds
        a = Btic.from_raw(0, 100, 0)
        b = Btic.from_raw(100, 200, 0)
        assert a.meets(b)

    def test_adjacent(self):
        a = Btic.from_raw(0, 100, 0)
        b = Btic.from_raw(100, 200, 0)
        assert a.adjacent(b)
        assert b.adjacent(a)


class TestBticSetOps:
    """Test set operations on intervals."""

    def test_intersection(self):
        a = Btic("1980/1990")
        b = Btic("1985/1995")
        result = a.intersection(b)
        assert result is not None
        assert result.lo == b.lo
        assert result.hi == a.hi

    def test_intersection_disjoint(self):
        a = Btic("1980/1985")
        b = Btic("1990/1995")
        assert a.intersection(b) is None

    def test_span(self):
        a = Btic("1980/1985")
        b = Btic("1990/1995")
        result = a.span(b)
        assert result.lo == a.lo
        assert result.hi == b.hi

    def test_gap(self):
        a = Btic("1980/1985")
        b = Btic("1990/1995")
        result = a.gap(b)
        assert result is not None
        assert result.lo == a.hi
        assert result.hi == b.lo

    def test_gap_overlapping(self):
        a = Btic("1980/1990")
        b = Btic("1985/1995")
        assert a.gap(b) is None


# =============================================================================
# Dunder Methods
# =============================================================================


class TestBticDunders:
    """Test special methods."""

    def test_repr(self):
        b = Btic("1985")
        r = repr(b)
        assert r.startswith("Btic(")
        assert "1985" in r

    def test_str(self):
        b = Btic("1985")
        s = str(b)
        assert "1985" in s

    def test_equality(self):
        a = Btic("1985")
        b = Btic("1985")
        assert a == b

    def test_inequality(self):
        a = Btic("1985")
        b = Btic("1986")
        assert a != b

    def test_hash(self):
        a = Btic("1985")
        b = Btic("1985")
        assert hash(a) == hash(b)
        # Can be used in sets/dicts
        s = {a, b}
        assert len(s) == 1

    def test_ordering(self):
        a = Btic("1985")
        b = Btic("1990")
        assert a < b
        assert b > a
        assert a <= a
        assert a >= a


# =============================================================================
# PyValue Integration
# =============================================================================


class TestPyValueBtic:
    """Test PyValue BTIC support."""

    def test_value_btic_constructor(self):
        v = Value.btic("1985")
        assert v.type_name == "btic"
        assert v.is_btic()
        assert not v.is_null()

    def test_value_btic_invalid(self):
        with pytest.raises(ValueError):
            Value.btic("not-a-date")

    def test_value_btic_to_python(self):
        v = Value.btic("1985")
        py_val = v.to_python()
        assert isinstance(py_val, Btic)

    def test_value_btic_repr(self):
        v = Value.btic("1985")
        assert "btic" in repr(v)


# =============================================================================
# PyDataType Integration
# =============================================================================


class TestDataTypeBtic:
    """Test DataType.BTIC support."""

    def test_datatype_btic(self):
        dt = DataType.BTIC()
        assert repr(dt) == "DataType.BTIC"


# =============================================================================
# Database E2E Round-trip
# =============================================================================


class TestBticE2E:
    """End-to-end tests with actual database."""

    @pytest.fixture
    def btic_db(self):
        db = uni_db.UniBuilder.temporary().build()
        (
            db.schema()
            .label("Event")
            .property("name", "string")
            .property_nullable("when", "btic")
            .done()
            .apply()
        )
        return db

    def test_create_and_return_btic(self, btic_db):
        """CREATE with btic() function and verify return type."""
        session = btic_db.session()
        tx = session.tx()
        result = tx.query(
            "CREATE (e:Event {name: 'WW2', when: btic('1939/1945')}) RETURN e.when AS w"
        )
        tx.commit()
        row = result.rows[0]
        w = row["w"]
        assert isinstance(w, Btic)

    def test_match_return_btic(self, btic_db):
        """Write and read back BTIC value."""
        session = btic_db.session()
        tx = session.tx()
        tx.execute("CREATE (e:Event {name: 'Moon', when: btic('1969-07-20')})")
        tx.commit()
        btic_db.flush()

        session2 = btic_db.session()
        result = session2.query("MATCH (e:Event {name: 'Moon'}) RETURN e.when AS w")
        w = result.rows[0]["w"]
        assert isinstance(w, Btic)
        assert "1969" in str(w)

    def test_btic_parameter(self, btic_db):
        """Pass BTIC as a query parameter."""
        session = btic_db.session()
        tx = session.tx()
        btic_val = Btic("1985")
        tx.execute(
            "CREATE (e:Event {name: 'Birth', when: $when})",
            params={"when": btic_val},
        )
        tx.commit()
        btic_db.flush()

        session2 = btic_db.session()
        result = session2.query("MATCH (e:Event {name: 'Birth'}) RETURN e.when AS w")
        w = result.rows[0]["w"]
        assert isinstance(w, Btic)
        assert w.lo_granularity == "year"

    def test_btic_set_parameter(self, btic_db):
        """SET a BTIC property via parameter."""
        session = btic_db.session()
        tx = session.tx()
        tx.execute("CREATE (e:Event {name: 'Update'})")
        tx.commit()

        tx2 = session.tx()
        tx2.execute(
            "MATCH (e:Event {name: 'Update'}) SET e.when = $w",
            params={"w": Btic("2024-06")},
        )
        tx2.commit()
        btic_db.flush()

        session2 = btic_db.session()
        result = session2.query("MATCH (e:Event {name: 'Update'}) RETURN e.when AS w")
        w = result.rows[0]["w"]
        assert isinstance(w, Btic)
        assert w.lo_granularity == "month"

    def test_schema_with_btic_datatype(self, btic_db):
        """Verify schema accepts 'btic' data type string."""
        session = btic_db.session()
        tx = session.tx()
        result = tx.query(
            "CREATE (e:Event {name: 'Test', when: btic('2024')}) RETURN e.when AS w"
        )
        tx.commit()
        assert isinstance(result.rows[0]["w"], Btic)

    def test_delete_vertex_with_btic(self, btic_db):
        """DELETE removes vertex with BTIC property."""
        session = btic_db.session()
        tx = session.tx()
        tx.execute("CREATE (e:Event {name: 'ToDelete', when: btic('1985')})")
        tx.commit()
        btic_db.flush()

        tx2 = btic_db.session().tx()
        tx2.execute("MATCH (e:Event {name: 'ToDelete'}) DELETE e")
        tx2.commit()
        btic_db.flush()

        result = btic_db.session().query(
            "MATCH (e:Event {name: 'ToDelete'}) RETURN e"
        )
        assert len(result) == 0

    def test_remove_btic_property(self, btic_db):
        """REMOVE sets BTIC property to None."""
        session = btic_db.session()
        tx = session.tx()
        tx.execute("CREATE (e:Event {name: 'RemTest', when: btic('1985')})")
        tx.commit()
        btic_db.flush()

        tx2 = btic_db.session().tx()
        tx2.execute("MATCH (e:Event {name: 'RemTest'}) REMOVE e.when")
        tx2.commit()
        btic_db.flush()

        result = btic_db.session().query(
            "MATCH (e:Event {name: 'RemTest'}) RETURN e.when AS w"
        )
        assert len(result) == 1
        assert result.rows[0]["w"] is None

    def test_transaction_rollback_with_btic(self, btic_db):
        """Rolled-back BTIC mutations are invisible."""
        session = btic_db.session()
        tx = session.tx()
        tx.execute("CREATE (e:Event {name: 'Kept', when: btic('1985')})")
        tx.commit()
        btic_db.flush()

        tx2 = btic_db.session().tx()
        tx2.execute("CREATE (e:Event {name: 'Lost', when: btic('2024')})")
        tx2.rollback()

        result = btic_db.session().query(
            "MATCH (e:Event) RETURN e.name AS name"
        )
        names = [row["name"] for row in result.rows]
        assert "Kept" in names
        assert "Lost" not in names

    def test_btic_in_where_filter(self, btic_db):
        """btic_overlaps() works in WHERE clause."""
        session = btic_db.session()
        tx = session.tx()
        tx.execute("CREATE (e:Event {name: 'WW2', when: btic('1939/1945')})")
        tx.execute("CREATE (e:Event {name: 'Moon', when: btic('1969-07-20')})")
        tx.commit()
        btic_db.flush()

        result = btic_db.session().query(
            "MATCH (e:Event) WHERE btic_overlaps(e.when, btic('1940')) "
            "RETURN e.name AS name"
        )
        assert len(result) == 1
        assert result.rows[0]["name"] == "WW2"
