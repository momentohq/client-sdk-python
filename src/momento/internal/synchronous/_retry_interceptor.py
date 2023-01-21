import logging
from typing import Callable, List, TypeVar, Union

import grpc

import momento.errors

RequestType = TypeVar("RequestType")
InterceptorCall = TypeVar("InterceptorCall")
ResponseType = TypeVar("ResponseType")

# TODO: This is very duplicative of the asyncio retry interceptor; we need to
# DRY these up, but for now I am prioritizing getting a fix out for a customer.

# TODO: Retry interceptor behavior should be configurable, but we need to
# add the configuration API first.
# For now, for convenience during development, you can toggle this hard-coded
# variable to enable/disable it.

RETRIES_ENABLED = True
MAX_ATTEMPTS = 3

RETRYABLE_STATUS_CODES: List[grpc.StatusCode] = [
    # # including all the status codes for reference, but
    # # commenting out the ones we don't want to retry on for now.
    # grpc.StatusCode.OK,
    # grpc.StatusCode.CANCELLED,
    # grpc.StatusCode.UNKNOWN,
    # grpc.StatusCode.INVALID_ARGUMENT,
    # grpc.StatusCode.DEADLINE_EXCEEDED,
    # grpc.StatusCode.NOT_FOUND,
    # grpc.StatusCode.ALREADY_EXISTS,
    # grpc.StatusCode.PERMISSION_DENIED,
    # grpc.StatusCode.RESOURCE_EXHAUSTED,
    # grpc.StatusCode.FAILED_PRECONDITION,
    # grpc.StatusCode.ABORTED,
    # grpc.StatusCode.OUT_OF_RANGE,
    # grpc.StatusCode.UNIMPLEMENTED,
    grpc.StatusCode.INTERNAL,
    grpc.StatusCode.UNAVAILABLE,
    # grpc.StatusCode.DATA_LOSS,
    # grpc.StatusCode.UNAUTHENTICATED,
]

LOGGER = logging.getLogger("retry-interceptor")


# TODO: We need to send retry count information to the server so that we
# will have some visibility into how often this is happening to customers:
# https://github.com/momentohq/client-sdk-javascript/issues/80
# TODO: we need to add backoff/jitter for the retries:
# https://github.com/momentohq/client-sdk-javascript/issues/81
def get_retry_interceptor_if_enabled() -> List[grpc.UnaryUnaryClientInterceptor]:
    if not RETRIES_ENABLED:
        return []

    return [RetryInterceptor()]


class RetryInterceptor(grpc.UnaryUnaryClientInterceptor):
    def intercept_unary_unary(
        self,
        continuation: Callable[[grpc.ClientCallDetails, RequestType], InterceptorCall],
        client_call_details: grpc.ClientCallDetails,
        request: RequestType,
    ) -> Union[InterceptorCall, ResponseType]:
        for try_i in range(MAX_ATTEMPTS):
            call = continuation(client_call_details, request)
            response_code = call.code()  # type: ignore[attr-defined]  # noqa: F401

            if response_code == grpc.StatusCode.OK:
                return call

            # Return if it was last attempt
            if try_i == (MAX_ATTEMPTS - 1):
                LOGGER.debug(
                    "Request path: %s; retryable status code: %s; number of retries (%i) "
                    "has exceeded max (%i), not retrying.",
                    client_call_details.method,
                    response_code,
                    try_i,
                    MAX_ATTEMPTS,
                )
                return call

            # If status code is not in retryable status codes
            if response_code not in RETRYABLE_STATUS_CODES:
                return call

            LOGGER.debug(
                "Request path: %s; retryable status code: %s; number of retries (%i) "
                "is less than max (%i), retrying.",
                client_call_details.method,
                response_code,
                try_i,
                MAX_ATTEMPTS,
            )

        raise momento.errors.UnknownException("Failed to return from RetryInterceptor!  This is a bug.")
