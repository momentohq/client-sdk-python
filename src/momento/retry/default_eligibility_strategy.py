from __future__ import annotations

import grpc

from .eligibility_strategy import EligibilityStrategy
from .retryable_props import RetryableProps

RETRYABLE_STATUS_CODES: set[grpc.StatusCode] = {
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
}

RETRYABLE_REQUEST_TYPES: set[str] = {
    "/cache_client.Scs/Get",
    "/cache_client.Scs/GetBatch",
    "/cache_client.Scs/Set",
    "/cache_client.Scs/SetBatch",
    # Not retryable: "/cache_client.Scs/SetIf",
    # SetIfNotExists is deprecated
    # Not retryable: "/cache_client.Scs/SetIfNotExists",
    "/cache_client.Scs/Delete",
    "/cache_client.Scs/KeysExist",
    # Not retryable: "/cache_client.Scs/Increment",
    # Not retryable: "/cache_client.Scs/UpdateTtl",
    "/cache_client.Scs/ItemGetTtl",
    "/cache_client.Scs/ItemGetType",
    "/cache_client.Scs/DictionaryGet",
    "/cache_client.Scs/DictionaryFetch",
    "/cache_client.Scs/DictionarySet",
    # Not retryable: "/cache_client.Scs/DictionaryIncrement",
    "/cache_client.Scs/DictionaryDelete",
    "/cache_client.Scs/DictionaryLength",
    "/cache_client.Scs/SetFetch",
    "/cache_client.Scs/SetSample",
    "/cache_client.Scs/SetUnion",
    "/cache_client.Scs/SetDifference",
    "/cache_client.Scs/SetContains",
    "/cache_client.Scs/SetLength",
    # Not retryable: "/cache_client.Scs/SetPop",
    # Not retryable: "/cache_client.Scs/ListPushFront",
    # Not retryable: "/cache_client.Scs/ListPushBack",
    # Not retryable: "/cache_client.Scs/ListPopFront",
    # Not retryable: "/cache_client.Scs/ListPopBack",
    # Not used: "/cache_client.Scs/ListErase",
    "/cache_client.Scs/ListRemove",
    "/cache_client.Scs/ListFetch",
    "/cache_client.Scs/ListLength",
    # Not retryable: "/cache_client.Scs/ListConcatenateFront",
    # Not retryable: "/cache_client.Scs/ListConcatenateBack",
    # Not retryable: "/cache_client.Scs/ListRetain",
    "/cache_client.Scs/SortedSetPut",
    "/cache_client.Scs/SortedSetFetch",
    "/cache_client.Scs/SortedSetGetScore",
    "/cache_client.Scs/SortedSetRemove",
    # Not retryable: "/cache_client.Scs/SortedSetIncrement",
    "/cache_client.Scs/SortedSetGetRank",
    "/cache_client.Scs/SortedSetLength",
    "/cache_client.Scs/SortedSetLengthByScore",
    "/cache_client.pubsub.Pubsub/Subscribe",
}


class DefaultEligibilityStrategy(EligibilityStrategy):
    def is_eligible_for_retry(self, props: RetryableProps) -> bool:
        """Determines whether a grpc call can be retried.

        The determination is based on the result of the last invocation of the call and whether the call is idempotent.

        Args:
            props (RetryableProps): Information about the grpc call and its last invocation.
        """
        if props.grpc_status not in RETRYABLE_STATUS_CODES:  # type: ignore[misc]
            return False

        if props.grpc_method not in RETRYABLE_REQUEST_TYPES:
            return False

        return True
