"""Small helpers for building typed test DataFrames from the DML schemas."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

D = Decimal


def date(s: str) -> dt.date:
    """Parse a ``yyyy-MM-dd`` string to ``datetime.date``."""
    return dt.datetime.strptime(s, "%Y-%m-%d").date()
