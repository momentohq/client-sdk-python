from typing import Any, Dict, List, Optional, Tuple, Type

import grpc

from momento import logs
from momento.errors import (
    MomentoErrorTransportDetails,
    MomentoGrpcErrorDetails,
    SdkException,
    AlreadyExistsException,
    ServerUnavailableException,
    InvalidArgumentException,
    SdkError,
    BadRequestError,
    CancelledError,
    ClientSdkError,
    TimeoutError,
    PermissionError,
    AuthenticationError,
    LimitExceededError,
    AlreadyExistsError,
    NotFoundError,
    InternalServerError,
)

new_mapping: Dict[grpc.StatusCode, Type[SdkException]] = {
    grpc.StatusCode.ALREADY_EXISTS: AlreadyExistsException,
    grpc.StatusCode.UNAVAILABLE: ServerUnavailableException,
    grpc.StatusCode.INVALID_ARGUMENT: InvalidArgumentException,
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


def new_convert(exception: Exception, transport_metadata: Optional[List[Tuple[str, str]]] = None) -> SdkException:
    if isinstance(exception, SdkException):
        return exception

    if isinstance(exception, grpc.RpcError):
        status_code: grpc.StatusCode = exception.code()
        # TODO: is this correct vs the exception args?
        details = exception.details()

        if status_code in new_mapping:
            concrete_exception_type = new_mapping[status_code]
            transport_details = MomentoErrorTransportDetails(
                MomentoGrpcErrorDetails(status_code, details, transport_metadata)
            )
            return concrete_exception_type(details, transport_details)  # type: ignore

    return InvalidArgumentException("Operation failed with error: " + str(exception))


def convert(exception: Exception, transport_metadata: Optional[List[Tuple[str, str]]] = None) -> Exception:
    if isinstance(exception, SdkError):
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
