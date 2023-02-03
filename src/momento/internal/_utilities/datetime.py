from datetime import timedelta


def total_milliseconds(td: timedelta) -> int:
    """Calculate total non fractional milliseconds.

    Args:
        td (timedelta): time duration

    Returns:
        int: total whole milliseconds
    """
    return int(td.total_seconds() * 1000)
