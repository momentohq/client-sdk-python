from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Expiration():
    """Represents an expiration time for a token."""

    def __init__(self, does_expire: bool) -> None:
        self._does_expire = does_expire

    def does_expire(self) -> bool:
        return self._does_expire


@dataclass
class ExpiresIn(Expiration):
    """Represents an expiration time for a token that expires in a certain amount of time."""

    # Must be a float in order to use math.inf to indicate non-expiration
    validFor: float

    def __init__(self, validFor: Optional[float] = math.inf) -> None:
        super().__init__(validFor != math.inf)
        if validFor is None:
            self.validFor = math.inf
        else:
            self.validFor = validFor

    def valid_for_seconds(self) -> int:
        return int(self.validFor)

    @staticmethod
    def never() -> ExpiresIn:
        return ExpiresIn(math.inf)

    @staticmethod
    def seconds(validForSeconds: int) -> ExpiresIn:
        """Constructs a ExpiresIn with a specified validFor period in seconds. If seconds are undefined, or null, then token never expires."""
        return ExpiresIn(validForSeconds)

    @staticmethod
    def minutes(validForMinutes: int) -> ExpiresIn:
        """Constructs a ExpiresIn with a specified validFor period in minutes."""
        return ExpiresIn(validForMinutes * 60)

    @staticmethod
    def hours(validForHours: int) -> ExpiresIn:
        """Constructs a ExpiresIn with a specified validFor period in hours."""
        return ExpiresIn(validForHours * 3600)

    @staticmethod
    def days(validForDays: int) -> ExpiresIn:
        """Constructs an ExpiresIn with a specified validFor period in days."""
        return ExpiresIn(validForDays * 86400)

    @staticmethod
    def epoch(expiresBy: int) -> ExpiresIn:
        """Constructs an ExpiresIn with a specified expiresBy period in epoch format."""
        current_epoch = int(datetime.now().timestamp())
        seconds_until_epoch = expiresBy - current_epoch
        return ExpiresIn(seconds_until_epoch)


@dataclass
class ExpiresAt(Expiration):
    """Represents an expiration time for a token that expires at a certain UNIX epoch timestamp."""

    # Must be a float in order to use math.inf to indicate non-expiration
    expires_at: float

    def __init__(self, epochTimestamp: Optional[float] = None) -> None:
        super().__init__(epochTimestamp is not None and epochTimestamp != 0)
        if self._does_expire and epochTimestamp is not None:
            self.expires_at = epochTimestamp
        else:
            self.expires_at = math.inf

    def epoch(self) -> int:
        """Returns epoch timestamp of when api token expires."""
        return int(self.expires_at)

    @staticmethod
    def from_epoch(epoch: Optional[int] = None) -> ExpiresAt:
        """Constructs an ExpiresAt with the specified epoch timestamp. If timestamp is undefined, then epoch timestamp will instead be Infinity."""
        return ExpiresAt(epoch)
