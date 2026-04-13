# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Integration tests for BTIC temporal interval with Pydantic OGM."""

import pytest

from uni_pydantic import Btic, Field, UniNode, UniSession


class Event(UniNode):
    """Test model with a BTIC temporal field."""

    __label__ = "Event"
    name: str = Field(index="btree")
    when: Btic | None = None


@pytest.fixture
def btic_session(db):
    """Create a session with the Event model registered."""
    with UniSession(db) as session:
        session.register(Event)
        session.sync_schema()
        yield session


class TestBticPydanticIntegration:
    """End-to-end tests for BTIC with Pydantic models."""

    def test_create_event_with_btic(self, btic_session):
        """Create an Event with a BTIC value."""
        event = Event(name="WW2", when=Btic("1939/1945"))
        btic_session.add(event)
        btic_session.commit()

        # Query back
        results = btic_session.query(Event).filter(Event.name == "WW2").all()
        assert len(results) == 1
        assert isinstance(results[0].when, Btic)

    def test_create_event_with_string_coercion(self, btic_session):
        """Pydantic auto-coerces string to Btic."""
        event = Event(name="Moon", when="1969-07-20")
        assert isinstance(event.when, Btic)
        assert event.when.lo_granularity == "day"

    def test_create_event_none_btic(self, btic_session):
        """Create an Event with no BTIC value (None)."""
        event = Event(name="NoDate")
        assert event.when is None
        btic_session.add(event)
        btic_session.commit()

        results = btic_session.query(Event).filter(Event.name == "NoDate").all()
        assert len(results) == 1
        assert results[0].when is None

    def test_btic_roundtrip_properties(self, btic_session):
        """Verify BTIC properties survive a database round-trip."""
        original = Btic("~1985-03")
        event = Event(name="Approx", when=original)
        btic_session.add(event)
        btic_session.commit()

        results = btic_session.query(Event).filter(Event.name == "Approx").all()
        restored = results[0].when
        assert isinstance(restored, Btic)
        assert restored.lo_granularity == "month"
        assert restored.lo_certainty == "approximate"

    def test_model_validation_invalid_btic(self):
        """Invalid BTIC literal should raise validation error."""
        with pytest.raises(Exception):
            Event(name="Bad", when="not-a-date")
