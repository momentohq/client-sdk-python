import logging
from typing import List, Callable, Union

import grpc


# TODO: Retry interceptor behavior should be configurable, but we need to
# align on basic API design first:
# https://github.com/momentohq/client-sdk-javascript/issues/79
# https://github.com/momentohq/dev-eco-issue-tracker/issues/85
# For now, for convenience during development, you can toggle this hard-coded
# variable to enable/disable it.
import momento.errors

RETRIES_ENABLED = True
MAX_ATTEMPTS = 3


RETRYABLE_STATUS_CODES: List[grpc.StatusCode] = [
    ## including all the status codes for reference, but
    ## commenting out the ones we don't want to retry on for now.
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
def get_retry_interceptor_if_enabled() -> List[grpc.aio.UnaryUnaryClientInterceptor]:
    if not RETRIES_ENABLED:
        return []

    return [RetryInterceptor()]


class RetryInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    async def intercept_unary_unary(
        self,
        continuation: Callable[
            [grpc.aio._interceptor.ClientCallDetails, grpc.aio._typing.RequestType],
            grpc.aio._call.UnaryUnaryCall,
        ],
        client_call_details: grpc.aio._interceptor.ClientCallDetails,
        request: grpc.aio._typing.RequestType,
    ) -> Union[grpc.aio._call.UnaryUnaryCall, grpc.aio._typing.ResponseType]:
        for try_i in range(MAX_ATTEMPTS):
            call = await continuation(client_call_details, request)
            response_code = await call.code()

            if response_code == grpc.StatusCode.OK:
                return call

            # Return if it was last attempt
            if try_i == (MAX_ATTEMPTS - 1):
                LOGGER.debug(
                    "Request path: %s; retryable status code: %s; number of retries (%i) has exceeded max (%i), not retrying.",
                    client_call_details.method.decode("utf-8"),
                    response_code,
                    try_i,
                    MAX_ATTEMPTS,
                )
                return call

            # If status code is not in retryable status codes
            if response_code not in RETRYABLE_STATUS_CODES:
                return call

            LOGGER.debug(
                f"Request path: %s; retryable status code: %s; number of retries (%i) is less than max (%i), retrying.",
                client_call_details.method.decode("utf-8"),
                response_code,
                try_i,
                MAX_ATTEMPTS,
            )

        raise momento.errors.ClientSdkError(
            "Failed to return from RetryInterceptor!  This is a bug."
        )
