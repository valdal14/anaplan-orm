from datetime import date

import pytest
from pydantic import BaseModel, ValidationError

from anaplan_orm.types import AnaplanBoolean, AnaplanDate


# NOTE: Test Model #######################################################################
class DummyRow(BaseModel):
    is_active: AnaplanBoolean
    start_date: AnaplanDate


# NOTE: Boolean Tests ####################################################################
def test_anaplan_boolean_parsing():
    # It should accept native Python booleans
    assert DummyRow(is_active=True, start_date="2026-01-01").is_active is True
    assert DummyRow(is_active=False, start_date="2026-01-01").is_active is False

    # It should cleanly parse Anaplan-style truthy strings
    assert DummyRow(is_active="Yes", start_date="2026-01-01").is_active is True
    assert DummyRow(is_active="true", start_date="2026-01-01").is_active is True
    assert DummyRow(is_active="1", start_date="2026-01-01").is_active is True

    # It should cleanly parse Anaplan-style falsy strings
    assert DummyRow(is_active="No", start_date="2026-01-01").is_active is False
    assert DummyRow(is_active="false", start_date="2026-01-01").is_active is False
    assert DummyRow(is_active="0", start_date="2026-01-01").is_active is False


def test_anaplan_boolean_serialization():
    row_true = DummyRow(is_active=True, start_date="2026-01-01")
    row_false = DummyRow(is_active=False, start_date="2026-01-01")

    # The payload MUST be strictly "true" or "false" for the Anaplan API
    payload_true = row_true.model_dump()
    payload_false = row_false.model_dump()

    assert payload_true["is_active"] == "true"
    assert payload_false["is_active"] == "false"


# NOTE: Date Tests #######################################################################
def test_anaplan_date_parsing():
    # It should accept native Python datetime.date objects
    row_native = DummyRow(is_active=True, start_date=date(2026, 3, 19))
    assert row_native.start_date == date(2026, 3, 19)

    # It should accept standard ISO string dates
    row_str = DummyRow(is_active=True, start_date="2026-03-19")
    assert row_str.start_date == date(2026, 3, 19)


def test_anaplan_date_serialization():
    # Even if initialized with a native date object...
    row = DummyRow(is_active=True, start_date=date(2026, 3, 19))

    # ...the payload MUST serialize to a strict YYYY-MM-DD string
    payload = row.model_dump()
    assert payload["start_date"] == "2026-03-19"


def test_invalid_date_format_rejected():
    # Pydantic should block garbage data before it ever reaches Anaplan
    with pytest.raises(ValidationError) as exc_info:
        DummyRow(is_active=True, start_date="Not-A-Valid-Date")

    assert "start_date" in str(exc_info.value)
