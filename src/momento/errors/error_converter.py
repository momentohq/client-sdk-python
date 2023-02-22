from __future__ import annotations

from typing import Optional, Tuple, Type

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

grpc_status_to_exception: dict[grpc.StatusCode, Type[SdkException]] = {
    grpc.StatusCode.INVALID_ARGUMENT: InvalidArgumentException,
    grpc.StatusCode.INTERNAL: InternalServerException,
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


def convert_error(
    exception: Exception, transport_metadata: Optional[Metadata | list[Tuple[str, str]]] = None
) -> SdkException:
    """Convert a low-level exception raised by gRPC to a Momento `SdkException`.

    Note about the metadata type: Metadata in the asynchronous gRPC client is of
    type Metadata, while metadata in the synchronous client is a list of tuples.
    The types are isomorphic.

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
        transport_metadata = _synchronous_metadata_to_metadata(transport_metadata)

    if isinstance(exception, grpc.RpcError):
        status_code: grpc.StatusCode = exception.code()
        details = exception.details()
        transport_details = MomentoErrorTransportDetails(
            MomentoGrpcErrorDetails(status_code, details, transport_metadata)
        )

        if status_code in grpc_status_to_exception:
            concrete_exception_type = grpc_status_to_exception[status_code]
            return concrete_exception_type(details, transport_details)  # type: ignore
        else:
            # TODO exception chaining from .NET redundant here?
            return InternalServerException(INTERNAL_SERVER_ERROR_MESSAGE, transport_details)

    # TODO exception chaining from .NET redundant here?
    return UnknownException(SDK_ERROR_MESSAGE, None)


def _synchronous_metadata_to_metadata(metadata: list[Tuple[str, str]]) -> Metadata:
    """Represent synchronous client metadata as a `Metadata` object.

    Args:
        metadata (List[Tuple[str, str]]): The synchronous client metadata.

    Returns:
        Metadata: async client metadata representation
    """
    new_metadata = Metadata()
    for key, value in metadata:
        new_metadata.add(key, value)
    return new_metadata
