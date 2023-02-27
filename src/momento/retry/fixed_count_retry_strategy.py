import logging
from typing import Optional

from .default_eligibility_strategy import DefaultEligibilityStrategy
from .eligibility_strategy import EligibilityStrategy
from .retry_strategy import RetryStrategy
from .retryable_props import RetryableProps

logger = logging.getLogger("fixed-count-retry-strategy")


class FixedCountRetryStrategy(RetryStrategy):
    def __init__(
        self, *, max_attempts: int = 3, eligibility_strategy: DefaultEligibilityStrategy = DefaultEligibilityStrategy()
    ):
        self._eligibility_strategy: EligibilityStrategy = eligibility_strategy
        self._max_attempts: int = max_attempts

    def determine_when_to_retry(self, props: RetryableProps) -> Optional[float]:
        """Determines whether a grpc call can be retried and how long to wait before that retry.

        Args:
            props (RetryableProps): Information about the grpc call, its last invocation, and how many times the call
            has been made.

        :Returns
            The time in seconds before the next retry should occur or None if no retry should be attempted.
        """
        if self._eligibility_strategy.is_eligible_for_retry(props) is False:
            logger.debug(
                "Request path: %s; retryable status code: %s. Request is not retryable.",
                props.grpc_method,
                props.grpc_status,  # type: ignore[misc]
            )
            return None

        if props.attempt_number > self._max_attempts:
            logger.debug(
                "Request path: %s; retryable status code: %s; number of attempts (%i) "
                "has exceeded max (%i); not retrying.",
                props.grpc_method,
                props.grpc_status,  # type: ignore[misc]
                props.attempt_number,
                self._max_attempts,
            )
            return None

        logger.debug(
            "Request path: %s; retryable status code: %s; number of attempts (%i) " "is not above max (%i); retrying.",
            props.grpc_method,
            props.grpc_status,  # type: ignore[misc]
            props.attempt_number,
            self._max_attempts,
        )
        return 0.0
