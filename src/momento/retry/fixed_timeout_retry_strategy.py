import logging
import random
from datetime import datetime, timedelta
from typing import Optional

import grpc

from .default_eligibility_strategy import DefaultEligibilityStrategy
from .eligibility_strategy import EligibilityStrategy
from .retry_strategy import RetryStrategy
from .retryable_props import RetryableProps

logger = logging.getLogger("fixed-timeout-retry-strategy")


class FixedTimeoutRetryStrategy(RetryStrategy):
    def __init__(
        self,
        *,
        retry_timeout_millis: int,
        retry_delay_interval_millis: int,
        eligibility_strategy: DefaultEligibilityStrategy = DefaultEligibilityStrategy(),
    ):
        self._eligibility_strategy: EligibilityStrategy = eligibility_strategy
        self._retry_timeout_millis: int = retry_timeout_millis
        self._retry_delay_interval_millis: int = retry_delay_interval_millis

    def determine_when_to_retry(self, props: RetryableProps) -> Optional[float]:
        """Determines whether a grpc call can be retried and how long to wait before that retry.

        Args:
            props (RetryableProps): Information about the grpc call, its last invocation, and how many times the call
            has been made.

        :Returns
            The time in seconds before the next retry should occur or None if no retry should be attempted.
        """
        logger.debug(
            "Determining whether request is eligible for retry; status code: %s, request type: %s, attemptNumber: %d",
            props.grpc_status,  # type: ignore[misc]
            props.grpc_method,
            props.attempt_number,
        )

        if props.overall_deadline is None:
            logger.debug("Overall deadline is None; not retrying.")
            return None

        # If a retry attempt's timeout has passed but the client's overall timeout has not yet passed,
        # we should reset the deadline and retry.
        if (
            props.attempt_number > 0  # type: ignore[misc]
            and props.grpc_status == grpc.StatusCode.DEADLINE_EXCEEDED  # type: ignore[misc]
            and props.overall_deadline > datetime.now()
        ):
            return self.get_jitter_in_millis(props)

        if self._eligibility_strategy.is_eligible_for_retry(props) is False:
            logger.debug(
                "Request path: %s; retryable status code: %s. Request is not retryable.",
                props.grpc_method,
                props.grpc_status,  # type: ignore[misc]
            )
            return None

        return self.get_jitter_in_millis(props)

    def get_jitter_in_millis(self, props: RetryableProps) -> float:
        timeout_with_jitter = self.add_jitter(self._retry_delay_interval_millis)
        logger.debug(
            "Determined request is retryable; retrying after %d ms: [method: %s, status: %s, attempt: %d]",
            timeout_with_jitter,
            props.grpc_method,
            props.grpc_status,  # type: ignore[misc]
            props.attempt_number,
        )
        return timeout_with_jitter / 1000.0

    def add_jitter(self, base_delay: int) -> int:
        return int((0.2 * random.random() + 0.9) * float(base_delay))

    def calculate_retry_deadline(self, overall_deadline: datetime) -> Optional[float]:
        """Calculates the deadline for a retry attempt using the retry timeout, but clips it to the overall deadline if the overall deadline is sooner.

        Args:
            overall_deadline (datetime): The overall deadline for the operation.

        Returns:
            float: The calculated retry deadline.
        """
        logger.debug(
            f"Calculating retry deadline:\nnow: {datetime.now()}\noverall deadline: {overall_deadline}\n"
            + f"retry timeout millis: {self._retry_timeout_millis}"
        )
        if datetime.now() + timedelta(milliseconds=self._retry_timeout_millis) > overall_deadline:
            return (overall_deadline - datetime.now()).total_seconds() * 1000
        return self._retry_timeout_millis
