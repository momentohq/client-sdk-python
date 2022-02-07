from typing import Any

import grpc
from . import errors
from . import _momento_logger

__rpc_to_error = {
    grpc.StatusCode.INVALID_ARGUMENT: errors.BadRequestError,
    grpc.StatusCode.OUT_OF_RANGE: errors.BadRequestError,
    grpc.StatusCode.UNIMPLEMENTED: errors.BadRequestError,
    grpc.StatusCode.FAILED_PRECONDITION: errors.BadRequestError,
    grpc.StatusCode.CANCELLED: errors.CancelledError,
    grpc.StatusCode.DEADLINE_EXCEEDED: errors.TimeoutError,
    grpc.StatusCode.PERMISSION_DENIED: errors.PermissionError,
    grpc.StatusCode.UNAUTHENTICATED: errors.AuthenticationError,
    grpc.StatusCode.RESOURCE_EXHAUSTED: errors.LimitExceededError,
    grpc.StatusCode.ALREADY_EXISTS: errors.AlreadyExistsError,
    grpc.StatusCode.NOT_FOUND: errors.NotFoundError,
    grpc.StatusCode.UNKNOWN: errors.InternalServerError,
    grpc.StatusCode.ABORTED: errors.InternalServerError,
    grpc.StatusCode.INTERNAL: errors.InternalServerError,
    grpc.StatusCode.UNAVAILABLE: errors.InternalServerError,
    grpc.StatusCode.DATA_LOSS: errors.InternalServerError,
}


def convert(exception: Exception) -> Exception:
    if isinstance(exception, errors.SdkError):
        return exception

    if isinstance(exception, grpc.RpcError):
        if exception.code() in __rpc_to_error:
            return __rpc_to_error[exception.code()](exception.details())
        else:
            return errors.InternalServerError(
                "CacheService failed with an internal error"
            )

    return errors.ClientSdkError("Operation failed with error: " + str(exception))


def convert_ecache_result(ecache_result: Any, message: str, operation_name: str) -> errors.InternalServerError:  # type: ignore[misc]
    _momento_logger.debug(f"Converting ECacheResult: {ecache_result} to error.")
    return errors.InternalServerError(
        f"CacheService returned an unexpected result: {ecache_result}"
        + f" for operation: {operation_name} with message: {message}"
    )
