from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Optional


class GrpcConfiguration(ABC):
    @abstractmethod
    def get_deadline(self) -> timedelta:
        pass

    @abstractmethod
    def with_deadline(self, deadline: timedelta) -> GrpcConfiguration:
        pass
