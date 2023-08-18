from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta


class GrpcConfiguration(ABC):
    @abstractmethod
    def get_deadline(self) -> timedelta:
        pass

    @abstractmethod
    def with_deadline(self, deadline: timedelta) -> GrpcConfiguration:
        pass

    @abstractmethod
    def get_eager_connection_timeout(self) -> timedelta:
        pass

    @abstractmethod
    def with_eager_connection_timeout(self, timeout: timedelta) -> GrpcConfiguration:
        pass
