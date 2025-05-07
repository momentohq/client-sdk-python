from momento.errors import MomentoErrorCode

MOMENTO_ERROR_CODE_TO_METADATA = {
    MomentoErrorCode.INVALID_ARGUMENT_ERROR: "invalid-argument",
    MomentoErrorCode.UNKNOWN_SERVICE_ERROR: "unknown",
    MomentoErrorCode.ALREADY_EXISTS_ERROR: "already-exists",
    MomentoErrorCode.NOT_FOUND_ERROR: "not-found",
    MomentoErrorCode.INTERNAL_SERVER_ERROR: "internal",
    MomentoErrorCode.PERMISSION_ERROR: "permission-denied",
    MomentoErrorCode.AUTHENTICATION_ERROR: "unauthenticated",
    MomentoErrorCode.CANCELLED_ERROR: "cancelled",
    MomentoErrorCode.LIMIT_EXCEEDED_ERROR: "resource-exhausted",
    MomentoErrorCode.BAD_REQUEST_ERROR: "invalid-argument",
    MomentoErrorCode.TIMEOUT_ERROR: "deadline-exceeded",
    MomentoErrorCode.SERVER_UNAVAILABLE: "unavailable",
    MomentoErrorCode.CLIENT_RESOURCE_EXHAUSTED: "resource-exhausted",
    MomentoErrorCode.FAILED_PRECONDITION_ERROR: "failed-precondition",
    MomentoErrorCode.UNKNOWN_ERROR: "unknown",
    MomentoErrorCode.CONNECTION_ERROR: "unavailable",
}
