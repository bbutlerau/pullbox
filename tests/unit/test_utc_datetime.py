"""UTC timestamps retain their instant at each database driver's boundary."""

from datetime import UTC, datetime, timedelta, timezone

import pytest
from sqlalchemy.dialects import postgresql, sqlite

from pullbox.models.base import UTCDateTime


@pytest.mark.parametrize(
    "dialect", [sqlite.dialect(), postgresql.dialect()], ids=["sqlite", "postgresql"]
)
@pytest.mark.parametrize(
    "value",
    [
        None,
        datetime(2026, 9, 28, 7, 30),
        datetime(2026, 9, 28, 7, 30, tzinfo=UTC),
        datetime(2026, 9, 28, 0, 30, tzinfo=timezone(timedelta(hours=-7))),
    ],
)
def test_bind_normalizes_to_utc_without_losing_postgresql_timezone(dialect, value):
    expected = None if value is None else datetime(2026, 9, 28, 7, 30, tzinfo=UTC)
    if expected is not None and dialect.name == "sqlite":
        expected = expected.replace(tzinfo=None)
    assert UTCDateTime().process_bind_param(value, dialect) == expected


def test_results_normalize_aware_non_utc_values():
    value = datetime(2026, 9, 28, 0, 30, tzinfo=timezone(timedelta(hours=-7)))
    result = UTCDateTime().process_result_value(value, postgresql.dialect())
    assert result == datetime(2026, 9, 28, 7, 30, tzinfo=UTC)
    assert result.tzinfo is UTC
