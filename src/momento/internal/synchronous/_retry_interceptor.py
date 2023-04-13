from __future__ import annotations

import logging
import time
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
        attempt_number = 1
        while True:
            call = continuation(client_call_details, request)
            response_code = call.code()  # type: ignore[attr-defined]  # noqa: F401

            if response_code == grpc.StatusCode.OK:
                return call

            retryTime = self._retry_strategy.determine_when_to_retry(
                RetryableProps(response_code, client_call_details.method, attempt_number)
            )

            if retryTime is None:
                return call

            attempt_number += 1
            time.sleep(retryTime)
