from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from .retryable_props import RetryableProps


class RetryStrategy(ABC):
    @abstractmethod
    def determine_when_to_retry(self, props: RetryableProps) -> Optional[float]:
        pass

    def calculate_retry_deadline(overall_deadline: datetime) -> Optional[float]:
        """Calculates the deadline for a retry attempt using the retry timeout, but clips it to the overall 
        deadline if the overall deadline is sooner.

        Args:
            overall_deadline (float): The overall deadline for the operation.

        Returns:
            float: The calculated retry deadline.
        """
        return None
