from abc import ABC, abstractmethod
from typing import Optional

from .retryable_props import RetryableProps


class RetryStrategy(ABC):
    @abstractmethod
    def determine_when_to_retry(self, props: RetryableProps) -> Optional[float]:
        pass
