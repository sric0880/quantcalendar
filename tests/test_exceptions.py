import pytest

from quantcalendar import CalendarAstock, bar_unit, to_seconds, to_timepoint


def test_exceptions():
    cal = CalendarAstock()
    with pytest.raises(ValueError):
        cal.get_bartimes_gte(5 * bar_unit.hour, to_timepoint(2024, 9, 13), 2)
    with pytest.raises(ValueError):
        cal.get_bartimes_between(
            20 * bar_unit.min, to_timepoint(2024, 9, 13), to_timepoint(2024, 9, 14)
        )
    with pytest.raises(ValueError):
        cal.get_bartime_next(2 * bar_unit.day, to_timepoint(2024, 9, 13))

    assert (
        cal.get_bartimes_between(
            bar_unit.hour,
            to_timepoint(2024, 9, 13, 10, 31),
            to_timepoint(2024, 9, 13, 11, 30),
        )
        == []
    )

    with pytest.raises(IndexError):
        # start of session is not found
        cal.get_next_open_close(to_timepoint(2024, 12, 31, 12))
    with pytest.raises(IndexError):
        # start of session is not found
        cal.get_next_session(to_timepoint(2024, 12, 31, 14))
    with pytest.raises(IndexError):
        # both start and end of session are not found
        cal.get_next_session(to_timepoint(2025, 1, 1))
    with pytest.raises(IndexError):
        # actually it's trading, but next start of session if not found
        cal.is_trading(to_timepoint(2024, 12, 31, 15))
    with pytest.raises(IndexError):
        cal.is_trading_day(to_timepoint(2025, 1, 1))
