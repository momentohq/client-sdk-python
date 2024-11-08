from enum import Enum
from typing import Optional

from momento.errors import MomentoErrorCode, MomentoErrorTransportDetails
from momento.internal.services import Service


class SdkException(Exception):
    """Base class for all Momento client exceptions."""

    message: str
    """Exception message"""
    error_code: MomentoErrorCode
    """Enumeration of all possible Momento error types.  Should be used in
    code to distinguish between different types of errors.
    """
    service: Service
    """The service which generated the error, e.g. cache or topics"""
    transport_details: Optional[MomentoErrorTransportDetails] = None
    """Low-level error details, from the transport layer.  Hopefully only needed
    in rare cases, by Momento engineers, for debugging.
    """
    message_wrapper: str
    """Prefix with basic information about the error class; this will be appended
    with specific information about the individual error instance at runtime."""

    def __init__(
        self,
        message: str,
        error_code: MomentoErrorCode,
        service: Service,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
        message_wrapper: str = "",
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.service = service
        self.transport_details = transport_details
        self.message_wrapper = message_wrapper

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"message={self.message!r}, "
            f"error_code={self.error_code!r}, "
            f"transport_details={self.transport_details!r}, "
            f"message_wrapper={self.message_wrapper!r})"
        )

    def __str__(self) -> str:
        return repr(self)


class AlreadyExistsException(SdkException):
    """Resource already exists."""

    def __init__(
        self,
        message: str,
        service: Service,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.ALREADY_EXISTS_ERROR,
            service,
            transport_details,
            message_wrapper=f"A {service.value} with the specified name already exists. To resolve this error, either delete the existing {service.value} and make a new one, or use a different name",  # noqa: E501
        )


class AuthenticationException(SdkException):
    """Authentication token is not provided or is invalid."""

    def __init__(
        self,
        message: str,
        service: Service,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.AUTHENTICATION_ERROR,
            service,
            transport_details,
            message_wrapper=f"Invalid authentication credentials to connect to {service.value} service",
        )


class BadRequestException(SdkException):
    """Invalid parameters sent to Momento Services."""

    def __init__(
        self,
        message: str,
        service: Service,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.BAD_REQUEST_ERROR,
            service,
            transport_details,
            message_wrapper="The request was invalid; please contact us at support@momentohq.com",
        )


class CancelledException(SdkException):
    """Operation was cancelled."""

    def __init__(
        self,
        message: str,
        service: Service,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.CANCELLED_ERROR,
            service,
            transport_details,
            message_wrapper="The request was cancelled by the server; please contact us at support@momentohq.com",
        )


class ConnectionException(SdkException):
    """Connection to the Momento server failed."""

    def __init__(
        self,
        message: str,
        service: Service,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.CONNECTION_ERROR,
            service,
            transport_details,
            message_wrapper="Connection to the Momento server failed; please contact us at support@momentohq.com",
        )


class FailedPreconditionException(SdkException):
    """The server did not meet the precondition to run a command.

    For example, calling `Increment` on a key that doesn't store
    a number.
    """

    def __init__(
        self,
        message: str,
        service: Service,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.FAILED_PRECONDITION_ERROR,
            service,
            transport_details,
            message_wrapper="System is not in a state required for the operation's execution",
        )


class InternalServerException(SdkException):
    """Momento Service encountered an unexpected exception while trying to fulfill the request."""

    def __init__(
        self,
        message: str,
        service: Service,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.INTERNAL_SERVER_ERROR,
            service,
            transport_details,
            message_wrapper="An unexpected error occurred while trying to fulfill the request; please contact us at support@momentohq.com",  # noqa: E501
        )


class InvalidArgumentException(SdkException):
    """SDK client-side validation failed."""

    def __init__(
        self,
        message: str,
        service: Service,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.INVALID_ARGUMENT_ERROR,
            service,
            transport_details,
            message_wrapper="Invalid argument passed to Momento client",
        )


class LimitExceededException(SdkException):
    """Requested operation couldn't be completed because system limits were hit."""

    def __init__(
        self,
        message: str,
        service: Service,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.LIMIT_EXCEEDED_ERROR,
            service,
            transport_details,
            message_wrapper=determineLimitExceededMessageWrapper(transport_details),  # noqa: E501
        )


class LimitExceededMessageWrapper(Enum):
    TOPIC_SUBSCRIPTIONS_LIMIT_EXCEEDED = "Topic subscriptions limit exceeded for this account"
    OPERATIONS_RATE_LIMIT_EXCEEDED = "Request rate limit exceeded for this account"
    THROUGHPUT_LIMIT_EXCEEDED = "Bandwidth limit exceeded for this account"
    REQUEST_SIZE_LIMIT_EXCEEDED = "Request size limit exceeded for this account"
    ITEM_SIZE_LIMIT_EXCEEDED = "Item size limit exceeded for this account"
    ELEMENTS_SIZE_LIMIT_EXCEEDED = "Element size limit exceeded for this account"
    UNKNOWN_LIMIT_EXCEEDED = "Limit exceeded for this account"


LIMIT_EXCEEDED_ERROR_TO_MESSAGE_WRAPPER = {
    "topic_subscriptions_limit_exceeded": LimitExceededMessageWrapper.TOPIC_SUBSCRIPTIONS_LIMIT_EXCEEDED.value,
    "operations_rate_limit_exceeded": LimitExceededMessageWrapper.OPERATIONS_RATE_LIMIT_EXCEEDED.value,
    "throughput_rate_limit_exceeded": LimitExceededMessageWrapper.THROUGHPUT_LIMIT_EXCEEDED.value,
    "request_size_limit_exceeded": LimitExceededMessageWrapper.REQUEST_SIZE_LIMIT_EXCEEDED.value,
    "item_size_limit_exceeded": LimitExceededMessageWrapper.ITEM_SIZE_LIMIT_EXCEEDED.value,
    "element_size_limit_exceeded": LimitExceededMessageWrapper.ELEMENTS_SIZE_LIMIT_EXCEEDED.value,
}


def determineLimitExceededMessageWrapper(transport_details: Optional[MomentoErrorTransportDetails] = None) -> str:
    # If provided, use the `err` metadata to determine the specific message wrapper to return.
    if transport_details is not None and transport_details.grpc.metadata is not None:  # type: ignore[misc]
        err_cause: Optional[str] = transport_details.grpc.metadata.get("err")  # type: ignore[misc]
        if err_cause is not None and err_cause in LIMIT_EXCEEDED_ERROR_TO_MESSAGE_WRAPPER:
            return LIMIT_EXCEEDED_ERROR_TO_MESSAGE_WRAPPER[err_cause]

    # If `err` metadata is unavailable, try to use the error details field to return
    # an appropriate message wrapper.
    if transport_details is not None and transport_details.grpc.details is not None:
        lower_cased_message = transport_details.grpc.details.lower()
        if "subscribe" in lower_cased_message:
            return LimitExceededMessageWrapper.TOPIC_SUBSCRIPTIONS_LIMIT_EXCEEDED.value
        elif "operations" in lower_cased_message:
            return LimitExceededMessageWrapper.OPERATIONS_RATE_LIMIT_EXCEEDED.value
        elif "throughput" in lower_cased_message:
            return LimitExceededMessageWrapper.THROUGHPUT_LIMIT_EXCEEDED.value
        elif "request limit" in lower_cased_message:
            return LimitExceededMessageWrapper.REQUEST_SIZE_LIMIT_EXCEEDED.value
        elif "item size" in lower_cased_message:
            return LimitExceededMessageWrapper.ITEM_SIZE_LIMIT_EXCEEDED.value
        elif "element size" in lower_cased_message:
            return LimitExceededMessageWrapper.ELEMENTS_SIZE_LIMIT_EXCEEDED.value

    return LimitExceededMessageWrapper.UNKNOWN_LIMIT_EXCEEDED.value


class NotFoundException(SdkException):
    """Requested resource or the resource on which an operation was requested doesn't exist."""

    def __init__(
        self,
        message: str,
        service: Service,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.NOT_FOUND_ERROR,
            service,
            transport_details,
            message_wrapper=f"A {service.value} with the specified name does not exist. To resolve this error, make sure you have created the {service.value} before attempting to use it",  # noqa: E501
        )


class PermissionDeniedException(SdkException):
    """Insufficient permissions to execute an operation."""

    def __init__(
        self,
        message: str,
        service: Service,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.PERMISSION_ERROR,
            service,
            transport_details,
            message_wrapper=f"Insufficient permissions to perform an operation on the {service.value}",
        )


class ServerUnavailableException(SdkException):
    """Server was unable to handle the request."""

    def __init__(
        self,
        message: str,
        service: Service,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.SERVER_UNAVAILABLE,
            service,
            transport_details,
            message_wrapper="The server was unable to handle the request; consider retrying. If the error persists, please contact us at support@momentohq.com",  # noqa: E501
        )


class TimeoutException(SdkException):
    """Requested operation did not complete in allotted time."""

    def __init__(
        self,
        message: str,
        service: Service,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.TIMEOUT_ERROR,
            service,
            transport_details,
            message_wrapper="The client's configured timeout was exceeded; you may need to use a Configuration with more lenient timeouts",  # noqa: E501
        )


class UnknownException(SdkException):
    """Unhandled exception from CacheExceptionMapper."""

    def __init__(
        self,
        message: str,
        service: Service = None,  # type: ignore
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.UNKNOWN_ERROR,
            service,
            transport_details,
            message_wrapper="Unknown error has occurred",
        )


class UnknownServiceException(SdkException):
    """Service returned an unknown response."""

    def __init__(
        self,
        message: str,
        service: Service,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.BAD_REQUEST_ERROR,
            service,
            transport_details,
            message_wrapper="Service returned an unknown response; please contact us at support@momentohq.com",
        )
