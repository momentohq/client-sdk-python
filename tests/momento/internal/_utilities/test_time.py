from datetime import timedelta

import pytest
from momento.internal._utilities import _timedelta_to_ms


@pytest.mark.parametrize(
    ["delta", "actual_ms"],
    [
        [timedelta(days=1), 24 * 60 * 60 * 1000],
        [timedelta(hours=1), 60 * 60 * 1000],
        [timedelta(minutes=1), 60 * 1000],
        [timedelta(seconds=1), 1000],
        [timedelta(milliseconds=1), 1],
        [timedelta(microseconds=1), 0],
        [timedelta(days=1, seconds=1), 24 * 60 * 60 * 1000 + 1000],
        [timedelta(seconds=1, milliseconds=100), 1100],
        [timedelta(milliseconds=1100), 1100],
    ],
)
def test_timedelta_to_ms(delta: timedelta, actual_ms: int) -> None:
    assert _timedelta_to_ms(delta) == actual_ms
