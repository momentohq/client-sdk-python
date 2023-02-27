from abc import ABC, abstractmethod

from .retryable_props import RetryableProps


class EligibilityStrategy(ABC):
    @abstractmethod
    def is_eligible_for_retry(self, props: RetryableProps) -> bool:
        pass
