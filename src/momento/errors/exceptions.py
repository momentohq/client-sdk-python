from typing import Optional

from momento.errors import MomentoErrorCode, MomentoErrorTransportDetails


class SdkException(Exception):
    """Base class for all Momento client exceptions."""

    message: str
    """Exception message"""
    error_code: MomentoErrorCode
    """Enumeration of all possible Momento error types.  Should be used in
    code to distinguish between different types of errors.
    """
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
        transport_details: Optional[MomentoErrorTransportDetails] = None,
        message_wrapper: str = "",
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
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
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.ALREADY_EXISTS_ERROR,
            transport_details,
            message_wrapper="A cache with the specified name already exists. To resolve this error, either delete the existing cache and make a new one, or use a different name",  # noqa: E501
        )


class AuthenticationException(SdkException):
    """Authentication token is not provided or is invalid."""

    def __init__(
        self,
        message: str,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.AUTHENTICATION_ERROR,
            transport_details,
            message_wrapper="Invalid authentication credentials to connect to cache service",
        )


class BadRequestException(SdkException):
    """Invalid parameters sent to Momento Services."""

    def __init__(
        self,
        message: str,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.BAD_REQUEST_ERROR,
            transport_details,
            message_wrapper="The request was invalid; please contact us at support@momentohq.com",
        )


class CancelledException(SdkException):
    """Operation was cancelled."""

    def __init__(
        self,
        message: str,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.CANCELLED_ERROR,
            transport_details,
            message_wrapper="The request was cancelled by the server; please contact us at support@momentohq.com",
        )


class FailedPreconditionException(SdkException):
    """The server did not meet the precondition to run a command.

    For example, calling `Increment` on a key that doesn't store
    a number.
    """

    def __init__(
        self,
        message: str,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.FAILED_PRECONDITION_ERROR,
            transport_details,
            message_wrapper="System is not in a state required for the operation's execution",
        )


class InternalServerException(SdkException):
    """Momento Service encountered an unexpected exception while trying to fulfill the request."""

    def __init__(
        self,
        message: str,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.INTERNAL_SERVER_ERROR,
            transport_details,
            message_wrapper="An unexpected error occurred while trying to fulfill the request; please contact us at support@momentohq.com",  # noqa: E501
        )


class InvalidArgumentException(SdkException):
    """SDK client-side validation failed."""

    def __init__(
        self,
        message: str,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.INVALID_ARGUMENT_ERROR,
            transport_details,
            message_wrapper="Invalid argument passed to Momento client",
        )


class LimitExceededException(SdkException):
    """Requested operation couldn't be completed because system limits were hit."""

    def __init__(
        self,
        message: str,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.LIMIT_EXCEEDED_ERROR,
            transport_details,
            message_wrapper="Request rate, bandwidth, or object size exceeded the limits for this account. To resolve this error, reduce your usage as appropriate or contact us at support@momentohq.com to request a limit increase",  # noqa: E501
        )


class NotFoundException(SdkException):
    """Requested resource or the resource on which an operation was requested doesn't exist."""

    def __init__(
        self,
        message: str,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.NOT_FOUND_ERROR,
            transport_details,
            message_wrapper="A cache with the specified name does not exist. To resolve this error, make sure you have created the cache before attempting to use it",  # noqa: E501
        )


class PermissionDeniedException(SdkException):
    """Insufficient permissions to execute an operation."""

    def __init__(
        self,
        message: str,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.PERMISSION_ERROR,
            transport_details,
            message_wrapper="Insufficient permissions to perform an operation on a cache",
        )


class ServerUnavailableException(SdkException):
    """Server was unable to handle the request."""

    def __init__(
        self,
        message: str,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.SERVER_UNAVAILABLE,
            transport_details,
            message_wrapper="The server was unable to handle the request; consider retrying. If the error persists, please contact us at support@momentohq.com",  # noqa: E501
        )


class TimeoutException(SdkException):
    """Requested operation did not complete in allotted time."""

    def __init__(
        self,
        message: str,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.TIMEOUT_ERROR,
            transport_details,
            message_wrapper="The client's configured timeout was exceeded; you may need to use a Configuration with more lenient timeouts",  # noqa: E501
        )


class UnknownException(SdkException):
    """Unhandled exception from CacheExceptionMapper."""

    def __init__(
        self,
        message: str,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message, MomentoErrorCode.UNKNOWN_ERROR, transport_details, message_wrapper="Unknown error has occurred"
        )


class UnknownServiceException(SdkException):
    """Service returned an unknown response."""

    def __init__(
        self,
        message: str,
        transport_details: Optional[MomentoErrorTransportDetails] = None,
    ):
        super().__init__(
            message,
            MomentoErrorCode.BAD_REQUEST_ERROR,
            transport_details,
            message_wrapper="Service returned an unknown response; please contact us at support@momentohq.com",
        )
