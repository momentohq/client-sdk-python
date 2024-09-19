from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Expiration:
    """Represents an expiration time for a token."""

    def __init__(self, does_expire: bool) -> None:
        self._does_expire = does_expire

    def does_expire(self) -> bool:
        return self._does_expire


@dataclass
class ExpiresIn(Expiration):
    """Represents an expiration time for a token that expires in a certain amount of time."""

    # Must be a float in order to use math.inf to indicate non-expiration
    def __init__(self, valid_for: Optional[float] = math.inf) -> None:
        super().__init__(valid_for != math.inf)
        if valid_for is None:
            self._valid_for = math.inf
        else:
            self._valid_for = valid_for

    def valid_for_seconds(self) -> int:
        return int(self._valid_for)

    @staticmethod
    def never() -> ExpiresIn:
        return ExpiresIn(math.inf)

    @staticmethod
    def seconds(valid_for_seconds: int) -> ExpiresIn:
        """Constructs a ExpiresIn with a specified valid_for period in seconds. If seconds are undefined, or null, then token never expires."""
        return ExpiresIn(valid_for_seconds)

    @staticmethod
    def minutes(valid_for_minutes: int) -> ExpiresIn:
        """Constructs a ExpiresIn with a specified valid_for period in minutes."""
        return ExpiresIn(valid_for_minutes * 60)

    @staticmethod
    def hours(valid_for_hours: int) -> ExpiresIn:
        """Constructs a ExpiresIn with a specified valid_for period in hours."""
        return ExpiresIn(valid_for_hours * 3600)

    @staticmethod
    def days(valid_for_days: int) -> ExpiresIn:
        """Constructs an ExpiresIn with a specified valid_for period in days."""
        return ExpiresIn(valid_for_days * 86400)

    @staticmethod
    def epoch(expires_by: int) -> ExpiresIn:
        """Constructs an ExpiresIn with a specified expires_by period in epoch format."""
        current_epoch = int(datetime.now().timestamp())
        seconds_until_epoch = expires_by - current_epoch
        return ExpiresIn(seconds_until_epoch)


@dataclass
class ExpiresAt(Expiration):
    """Represents an expiration time for a token that expires at a certain UNIX epoch timestamp."""

    # Must be a float in order to use math.inf to indicate non-expiration
    def __init__(self, epoch_timestamp: Optional[float] = None) -> None:
        super().__init__(epoch_timestamp is not None and epoch_timestamp != 0)
        if self._does_expire and epoch_timestamp is not None:
            self._expires_at = epoch_timestamp
        else:
            self._expires_at = math.inf

    def epoch(self) -> int:
        """Returns epoch timestamp of when api token expires."""
        return int(self._expires_at)

    @staticmethod
    def from_epoch(epoch: Optional[int] = None) -> ExpiresAt:
        """Constructs an ExpiresAt with the specified epoch timestamp. If timestamp is undefined, then epoch timestamp will instead be Infinity."""
        return ExpiresAt(epoch)
