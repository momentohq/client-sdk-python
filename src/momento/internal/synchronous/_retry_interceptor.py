from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Callable, TypeVar

import grpc

from momento.retry import RetryableProps, RetryStrategy

RequestType = TypeVar("RequestType")
InterceptorCall = TypeVar("InterceptorCall")
ResponseType = TypeVar("ResponseType")

# TODO: This is very duplicative of the asyncio retry interceptor; we need to
# DRY these up, but for now I am prioritizing getting a fix out for a customer.

logger = logging.getLogger("retry-interceptor")


# TODO: We need to send retry count information to the server so that we
# will have some visibility into how often this is happening to customers:
# https://github.com/momentohq/client-sdk-javascript/issues/80
# TODO: we need to add backoff/jitter for the retries:
# https://github.com/momentohq/client-sdk-javascript/issues/81


class RetryInterceptor(grpc.UnaryUnaryClientInterceptor):
    def __init__(self, retry_strategy: RetryStrategy):
        self._retry_strategy = retry_strategy

    def intercept_unary_unary(
        self,
        continuation: Callable[[grpc.ClientCallDetails, RequestType], InterceptorCall],
        client_call_details: grpc.ClientCallDetails,
        request: RequestType,
    ) -> InterceptorCall | ResponseType:
        call = None
        attempt_number = 1
        # The overall deadline is calculated from the timeout set on the client call details.
        # That value is set in our gRPC configurations and, while typed as optional, will never be None here.
        overall_deadline = datetime.now() + timedelta(seconds=client_call_details.timeout or 0.0)
        # variable to capture the penultimate call to a deadline-aware retry strategy, which
        # will hold the call object before a terminal DEADLINE_EXCEEDED response is returned
        last_call = None

        while True:
            if attempt_number > 1:
                retry_deadline = self._retry_strategy.calculate_retry_deadline(overall_deadline)
                if retry_deadline is not None:
                    client_call_details = grpc.aio._interceptor.ClientCallDetails(
                        client_call_details.method,
                        retry_deadline,
                        client_call_details.metadata,
                        client_call_details.credentials,
                        client_call_details.wait_for_ready,
                    )
                    last_call = call

            call = continuation(client_call_details, request)
            response_code = call.code()  # type: ignore[attr-defined]  # noqa: F401

            if response_code == grpc.StatusCode.OK:
                return call

            retryTime = self._retry_strategy.determine_when_to_retry(
                # Note: the async interceptor gets `client_call_details.method` as a binary string that needs to be decoded
                # but the sync interceptor gets it as a string.
                RetryableProps(response_code, client_call_details.method, attempt_number, overall_deadline)
            )

            if retryTime is None:
                return last_call or call

            attempt_number += 1
            time.sleep(retryTime)
