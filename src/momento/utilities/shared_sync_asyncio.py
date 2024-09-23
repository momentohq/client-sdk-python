from __future__ import annotations

from datetime import timedelta

from momento.utilities.expiration import ExpiresIn

DEFAULT_EAGER_CONNECTION_TIMEOUT_SECONDS = 30


def validate_eager_connection_timeout(timeout: timedelta) -> None:
    if timeout.total_seconds() < 0:
        raise ValueError("The eager connection timeout must be greater than or equal to 0 seconds.")


def validate_disposable_token_expiry(expires_in: ExpiresIn) -> None:
    if not expires_in.does_expire():
        raise ValueError("Disposable tokens must have an expiry")
    if expires_in.valid_for_seconds() < 0:
        raise ValueError("Disposable token expiry must be positive")
    if expires_in.valid_for_seconds() > 60 * 60:
        raise ValueError("Disposable tokens must expire within 1 hour")
