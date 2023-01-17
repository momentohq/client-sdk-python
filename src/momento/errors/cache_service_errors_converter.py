from typing import Any, Dict, List, Optional, Tuple, Type, Union

import grpc
from grpc.aio import Metadata

from momento import logs
from momento.errors import (
    AlreadyExistsError,
    AlreadyExistsException,
    AuthenticationError,
    AuthenticationException,
    BadRequestError,
    BadRequestException,
    CancelledError,
    CancelledException,
    ClientSdkError,
    FailedPreconditionException,
    InternalServerError,
    InternalServerException,
    InvalidArgumentException,
    LimitExceededError,
    LimitExceededException,
    MomentoErrorTransportDetails,
    MomentoGrpcErrorDetails,
    NotFoundError,
    NotFoundException,
    PermissionDeniedException,
    PermissionError,
    SdkError,
    SdkException,
    ServerUnavailableException,
    TimeoutError,
    TimeoutException,
    UnknownException,
    UnknownServiceException,
)

new_mapping: Dict[grpc.StatusCode, Type[SdkException]] = {
    grpc.StatusCode.INVALID_ARGUMENT: InvalidArgumentException,
    grpc.StatusCode.OUT_OF_RANGE: BadRequestException,
    grpc.StatusCode.UNIMPLEMENTED: BadRequestException,
    grpc.StatusCode.FAILED_PRECONDITION: FailedPreconditionException,
    grpc.StatusCode.PERMISSION_DENIED: PermissionDeniedException,
    grpc.StatusCode.UNAUTHENTICATED: AuthenticationException,
    grpc.StatusCode.RESOURCE_EXHAUSTED: LimitExceededException,
    grpc.StatusCode.NOT_FOUND: NotFoundException,
    grpc.StatusCode.ALREADY_EXISTS: AlreadyExistsException,
    grpc.StatusCode.DEADLINE_EXCEEDED: TimeoutException,
    grpc.StatusCode.CANCELLED: CancelledException,
    grpc.StatusCode.UNAVAILABLE: ServerUnavailableException,
    grpc.StatusCode.UNKNOWN: UnknownServiceException,
    grpc.StatusCode.ABORTED: InternalServerException,
    grpc.StatusCode.DATA_LOSS: InternalServerException,
}

__rpc_to_error = {
    grpc.StatusCode.INVALID_ARGUMENT: BadRequestError,
    grpc.StatusCode.OUT_OF_RANGE: BadRequestError,
    grpc.StatusCode.UNIMPLEMENTED: BadRequestError,
    grpc.StatusCode.FAILED_PRECONDITION: BadRequestError,
    grpc.StatusCode.CANCELLED: CancelledError,
    grpc.StatusCode.DEADLINE_EXCEEDED: TimeoutError,
    grpc.StatusCode.PERMISSION_DENIED: PermissionError,
    grpc.StatusCode.UNAUTHENTICATED: AuthenticationError,
    grpc.StatusCode.RESOURCE_EXHAUSTED: LimitExceededError,
    grpc.StatusCode.ALREADY_EXISTS: AlreadyExistsError,
    grpc.StatusCode.NOT_FOUND: NotFoundError,
    grpc.StatusCode.UNKNOWN: InternalServerError,
    grpc.StatusCode.ABORTED: InternalServerError,
    grpc.StatusCode.INTERNAL: InternalServerError,
    grpc.StatusCode.UNAVAILABLE: InternalServerError,
    grpc.StatusCode.DATA_LOSS: InternalServerError,
}

INTERNAL_SERVER_ERROR_MESSAGE = "Unexpected exception occurred while trying to fulfill the request."
SDK_ERROR_MESSAGE = "SDK Failed to process the request."


TMetadata = Union[Metadata, List[Tuple[str, str]]]
"""Metadata in the asynchronous gRPC client is of type Metadata, while
metadata in the synchronous client is a list of tuples. The types are isomorphic."""


def new_convert(exception: Exception, transport_metadata: Optional[TMetadata] = None) -> SdkException:
    if isinstance(exception, SdkException):
        return exception

    # Normalize synchronous client metadata to `Metadata`
    if isinstance(transport_metadata, list):
        transport_metadata = Metadata.from_tuple(transport_metadata)

    if isinstance(exception, grpc.RpcError):
        status_code: grpc.StatusCode = exception.code()
        details = exception.details()

        if status_code in new_mapping:
            concrete_exception_type = new_mapping[status_code]
            transport_details = MomentoErrorTransportDetails(
                MomentoGrpcErrorDetails(status_code, details, transport_metadata)
            )
            return concrete_exception_type(details, transport_details)  # type: ignore
        else:
            # TODO exception chaining from .NET redundant here?
            return InternalServerException(INTERNAL_SERVER_ERROR_MESSAGE, transport_details)

    # TODO exception chaining from .NET redundant here?
    return UnknownException(SDK_ERROR_MESSAGE, None)


def convert(exception: Exception, transport_metadata: Optional[List[Tuple[str, str]]] = None) -> Exception:
    if isinstance(exception, SdkError):
        return exception

    if isinstance(exception, SdkException):
        return exception

    if isinstance(exception, grpc.RpcError):
        if exception.code() in __rpc_to_error:
            return __rpc_to_error[exception.code()](exception.details())
        else:
            return InternalServerError("CacheService failed with an internal error")

    return ClientSdkError("Operation failed with error: " + str(exception))


def convert_ecache_result(  # type: ignore[misc]
    ecache_result: Any, message: str, operation_name: str
) -> InternalServerError:
    logs.debug("Converting ECacheResult: %s to error.", ecache_result)
    return InternalServerError(
        f"CacheService returned an unexpected result: {ecache_result}"
        + f" for operation: {operation_name} with message: {message}"
    )
