from . import _generic_client_interceptor
from ._generic_client_interceptor import _ClientCallDetails


def _timeout_adder_interceptor(request_timeout_seconds):
    def intercept_call(client_call_details, request_iterator,
                       request_streaming, response_streaming):
        metadata = []
        if client_call_details.metadata is not None:
            metadata = list(client_call_details.metadata)

        client_call_details = _ClientCallDetails(
            client_call_details.method, request_timeout_seconds, metadata,
            client_call_details.credentials)
        return client_call_details, request_iterator, None

    return _generic_client_interceptor.create(intercept_call)


def get(request_timeout_seconds: float):
    return _timeout_adder_interceptor(request_timeout_seconds)
