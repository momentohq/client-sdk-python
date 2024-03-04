from datetime import timedelta


def _timedelta_to_ms(delta: timedelta) -> int:
    """Expresses a timedelta as milliseconds.

    Note: truncates the microseconds.

    Args:
        delta (timedelta): The timedelta to convert.

    Returns:
        int: The total duration of the timedelta in milliseconds.
    """
    return int(delta.total_seconds() * 1000)
