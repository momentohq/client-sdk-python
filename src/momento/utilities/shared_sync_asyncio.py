from __future__ import annotations

import math
from abc import ABC, abstractmethod
from datetime import time, timedelta
from typing import Optional

DEFAULT_EAGER_CONNECTION_TIMEOUT_SECONDS = 30


def validate_eager_connection_timeout(timeout: timedelta) -> None:
    if timeout.total_seconds() < 0:
        raise ValueError("The eager connection timeout must be greater than or equal to 0 seconds.")


class Expiration(ABC):
    """Represents an expiration time for a token."""

    _does_expire: bool

    def __init__(self, does_expire: bool) -> None:
        self._does_expire = does_expire

    @abstractmethod
    def does_expire(self) -> bool:
        pass


class ExpiresIn(Expiration):
    """Represents an expiration time for a token that expires in a certain amount of time."""

    validFor: int

    def __init__(self, validFor: Optional[int] = math.inf) -> None:
        super().__init__(validFor != math.inf)
        self.validFor = validFor

    def does_expire(self) -> bool:
        return self._does_expire

    def valid_for_seconds(self) -> int:
        return self.validFor

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
        current_epoch = int(time.time())
        seconds_until_epoch = expiresBy - current_epoch
        return ExpiresIn(seconds_until_epoch)


class ExpiresAt(Expiration):
    """Represents an expiration time for a token that expires at a certain UNIX epoch timestamp."""

    expiresAt: int

    def does_expire(self) -> bool:
        return self._does_expire

    def __init__(self, epochTimestamp: Optional[int] = None) -> None:
        super().__init__(epochTimestamp is not None and epochTimestamp != 0)
        if self._does_expire:
            self.expiresAt = epochTimestamp
        else:
            self.expiresAt = math.inf

    def epoch(self) -> int:
        """Returns epoch timestamp of when api token expires."""
        return self.expiresAt

    @staticmethod
    def from_epoch(epoch: Optional[int] = None) -> ExpiresAt:
        """Constructs an ExpiresAt with the specified epoch timestamp. If timestamp is undefined, then epoch timestamp will instead be Infinity."""
        return ExpiresAt(epoch)
