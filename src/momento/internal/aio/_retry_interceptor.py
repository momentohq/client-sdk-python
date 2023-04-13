from __future__ import annotations

import asyncio
import logging
from typing import Callable

import grpc

from momento.retry import RetryableProps, RetryStrategy

# TODO: This is very duplicative of the synchronous retry interceptor; we need to
# DRY these up, but for now I am prioritizing getting a fix out for a customer.

logger = logging.getLogger("retry-interceptor")


# TODO: We need to send retry count information to the server so that we
# will have some visibility into how often this is happening to customers:
# https://github.com/momentohq/client-sdk-javascript/issues/80
# TODO: we need to add backoff/jitter for the retries:
# https://github.com/momentohq/client-sdk-javascript/issues/81


class RetryInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    def __init__(self, retry_strategy: RetryStrategy):
        self._retry_strategy = retry_strategy

    async def intercept_unary_unary(
        self,
        continuation: Callable[
            [grpc.aio._interceptor.ClientCallDetails, grpc.aio._typing.RequestType],
            grpc.aio._call.UnaryUnaryCall,
        ],
        client_call_details: grpc.aio._interceptor.ClientCallDetails,
        request: grpc.aio._typing.RequestType,
    ) -> grpc.aio._call.UnaryUnaryCall | grpc.aio._typing.ResponseType:
        attempt_number = 1
        while True:
            call = await continuation(client_call_details, request)
            response_code = await call.code()

            if response_code == grpc.StatusCode.OK:
                return call

            retryTime = self._retry_strategy.determine_when_to_retry(
                RetryableProps(response_code, client_call_details.method.decode("utf-8"), attempt_number)
            )

            if retryTime is None:
                return call

            attempt_number += 1
            await asyncio.sleep(retryTime)
