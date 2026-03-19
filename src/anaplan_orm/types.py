from datetime import date
from typing import Annotated, Any

from pydantic import BeforeValidator, PlainSerializer

# ---------------------------------------------------------
# AnaplanDate
# ---------------------------------------------------------
# Pydantic inherently accepts strings ("2026-03-19"), datetime.date,
# and datetime.datetime objects and parses them into `date`.
# The PlainSerializer guarantees that when we call `.model_dump()`,
# it outputs the strict YYYY-MM-DD string Anaplan demands.
AnaplanDate = Annotated[
    date, PlainSerializer(lambda d: d.strftime("%Y-%m-%d"), return_type=str, when_used="always")
]


# ---------------------------------------------------------
# AnaplanBoolean
# ---------------------------------------------------------
# Anaplan can be notoriously picky about booleans depending on the module setup.
# This accepts standard Python True/False, 1/0, or "true"/"false" strings,
# but strictly serializes it to a lowercase string for the Anaplan API payload.
def parse_anaplan_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes", "y", "t")
    return bool(value)


AnaplanBoolean = Annotated[
    bool,
    BeforeValidator(parse_anaplan_bool),
    PlainSerializer(lambda b: "true" if b else "false", return_type=str, when_used="always"),
]
