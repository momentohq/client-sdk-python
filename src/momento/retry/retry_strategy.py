from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from .retryable_props import RetryableProps


class RetryStrategy(ABC):
    @abstractmethod
    def determine_when_to_retry(self, props: RetryableProps) -> Optional[float]:
        pass

    # Currently used only by the FixedTimeoutRetryStrategy
    def calculate_retry_deadline(self, overall_deadline: datetime) -> Optional[float]:
        return None
