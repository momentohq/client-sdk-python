import collections

from momento_sdk import generic_client_interceptor
import grpc


class _ClientCallDetails(
        collections.namedtuple(
            '_ClientCallDetails',
            ('method', 'timeout', 'metadata', 'credentials')),
        grpc.ClientCallDetails):
    pass


def header_adder_interceptor(header, value):

    def intercept_call(client_call_details, request_iterator, request_streaming,
                       response_streaming):
        metadata = []
        if client_call_details.metadata is not None:
            metadata = list(client_call_details.metadata)
        metadata.append((
            header,
            value,
        ))
        client_call_details = _ClientCallDetails(
            client_call_details.method, client_call_details.timeout, metadata,
            client_call_details.credentials)
        return client_call_details, request_iterator, None

    return generic_client_interceptor.create(intercept_call)
