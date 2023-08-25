from datetime import timedelta

DEFAULT_EAGER_CONNECTION_TIMEOUT_SECONDS = 30


def validate_eager_connection_timeout(timeout: timedelta) -> None:
    if timeout.total_seconds() < 0:
        raise ValueError("The eager connection timeout must be greater than or equal to 0 seconds.")
