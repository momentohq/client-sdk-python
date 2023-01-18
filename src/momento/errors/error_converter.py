from typing import Dict, List, Optional, Tuple, Type, Union

import grpc
from grpc.aio import Metadata

from momento.errors import (
    AlreadyExistsException,
    AuthenticationException,
    BadRequestException,
    CancelledException,
    FailedPreconditionException,
    InternalServerException,
    InvalidArgumentException,
    LimitExceededException,
    MomentoErrorTransportDetails,
    MomentoGrpcErrorDetails,
    NotFoundException,
    PermissionDeniedException,
    SdkException,
    ServerUnavailableException,
    TimeoutException,
    UnknownException,
    UnknownServiceException,
)

grpc_status_to_exception: Dict[grpc.StatusCode, Type[SdkException]] = {
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


INTERNAL_SERVER_ERROR_MESSAGE = "Unexpected exception occurred while trying to fulfill the request."
SDK_ERROR_MESSAGE = "SDK Failed to process the request."


TMetadata = Union[Metadata, List[Tuple[str, str]]]
"""Metadata in the asynchronous gRPC client is of type Metadata, while
metadata in the synchronous client is a list of tuples. The types are isomorphic."""


def convert_error(exception: Exception, transport_metadata: Optional[TMetadata] = None) -> SdkException:
    """Convert a low-level exception raised by gRPC to a Momento `SdkException`

    Args:
        exception (Exception): Low-level (transport, server-side, validation) exception
        transport_metadata (Optional[TMetadata], optional): Transport metadata to enrich the new exception.
        Defaults to None.

    Returns:
        SdkException: High-level Momento exception object
    """
    if isinstance(exception, SdkException):
        return exception

    # Normalize synchronous client metadata to `Metadata`
    if isinstance(transport_metadata, list):
        transport_metadata = Metadata.from_tuple(transport_metadata)

    if isinstance(exception, grpc.RpcError):
        status_code: grpc.StatusCode = exception.code()
        details = exception.details()

        if status_code in grpc_status_to_exception:
            concrete_exception_type = grpc_status_to_exception[status_code]
            transport_details = MomentoErrorTransportDetails(
                MomentoGrpcErrorDetails(status_code, details, transport_metadata)
            )
            return concrete_exception_type(details, transport_details)  # type: ignore
        else:
            # TODO exception chaining from .NET redundant here?
            return InternalServerException(INTERNAL_SERVER_ERROR_MESSAGE, transport_details)

    # TODO exception chaining from .NET redundant here?
    return UnknownException(SDK_ERROR_MESSAGE, None)
