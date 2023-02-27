from __future__ import annotations

import grpc

from .eligibility_strategy import EligibilityStrategy
from .retryable_props import RetryableProps

RETRYABLE_STATUS_CODES: list[grpc.StatusCode] = [
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
    grpc.StatusCode.INTERNAL,  # type: ignore[misc]
    grpc.StatusCode.UNAVAILABLE,  # type: ignore[misc]
    # grpc.StatusCode.DATA_LOSS,
    # grpc.StatusCode.UNAUTHENTICATED,
]

RETRYABLE_REQUEST_TYPES: list[str] = [
    "/cache_client.Scs/Set",
    "/cache_client.Scs/Get",
    "/cache_client.Scs/Delete",
    # not idempotent: "/cache_client.Scs/Increment"
    "/cache_client.Scs/DictionarySet",
    # not idempotent: "/cache_client.Scs/DictionaryIncrement",
    "/cache_client.Scs/DictionaryGet",
    "/cache_client.Scs/DictionaryFetch",
    "/cache_client.Scs/DictionaryDelete",
    "/cache_client.Scs/SetUnion",
    "/cache_client.Scs/SetDifference",
    "/cache_client.Scs/SetFetch",
    # not idempotent: "/cache_client.Scs/SetIfNotExists"
    # not idempotent: "/cache_client.Scs/ListPushFront",
    # not idempotent: "/cache_client.Scs/ListPushBack",
    # not idempotent: "/cache_client.Scs/ListPopFront",
    # not idempotent: "/cache_client.Scs/ListPopBack",
    "/cache_client.Scs/ListFetch",
    # Warning: in the future, this may not be idempotent
    # Currently it supports removing all occurrences of a value.
    # In the future, we may also add "the first/last N occurrences of a value".
    # In the latter case it is not idempotent.
    "/cache_client.Scs/ListRemove",
    "/cache_client.Scs/ListLength",
    # not idempotent: "/cache_client.Scs/ListConcatenateFront",
    # not idempotent: "/cache_client.Scs/ListConcatenateBack"
]


class DefaultEligibilityStrategy(EligibilityStrategy):
    def is_eligible_for_retry(self, props: RetryableProps) -> bool:
        """Determines whether a grpc call is able to be retried.

        The determination is based on the result of the last invocation of the call and whether the call is idempotent.

        Args:
            props (RetryableProps): Information about the grpc call and its last invocation.
        """
        if props.grpc_status not in RETRYABLE_STATUS_CODES:  # type: ignore[misc]
            return False

        if props.grpc_method not in RETRYABLE_REQUEST_TYPES:
            return False

        return True
