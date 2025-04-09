from typing import Optional

import grpc
from grpc.aio import ClientCallDetails, Metadata

from momento.errors import InvalidArgumentException
from momento.internal.services import Service


def make_metadata(cache_name: str) -> Metadata:
    return Metadata(("cache", cache_name))


def sanitize_client_call_details(client_call_details: grpc.aio.ClientCallDetails) -> grpc.aio.ClientCallDetails:
    """Defensive function meant to handle inbound gRPC client request objects.

    Args:
        client_call_details: the original inbound client grpc request we are intercepting

    Returns: a new client_call_details object with metadata properly initialized to a `grpc.aio.Metadata` object
    """
    # Makes sure we can handle properly when we inject our own metadata onto request object.
    # This was mainly done as temporary fix after we observed ddtrace grpc client interceptor passing
    # client_call_details.metadata as a list instead of a grpc.aio.Metadata object.
    # See this ticket for follow-up actions to come back in and address this longer term:
    # https://github.com/momentohq/client-sdk-python/issues/149
    new_client_call_details = None
    # If no metadata set on passed in client call details then we are first to set, so we should just initialize
    if client_call_details.metadata is None:
        new_client_call_details = create_client_call_details(
            method=client_call_details.method,
            timeout=client_call_details.timeout,
            metadata=Metadata(),
            credentials=client_call_details.credentials,
            wait_for_ready=client_call_details.wait_for_ready,
        )

    # This is block hit when ddtrace interceptor runs first and sets metadata as a list
    elif isinstance(client_call_details.metadata, list):
        existing_headers = client_call_details.metadata
        metadata = Metadata()
        # re-add all existing values to new metadata
        for md_key, md_value in existing_headers:
            metadata.add(md_key, md_value)
        new_client_call_details = create_client_call_details(
            method=client_call_details.method,
            timeout=client_call_details.timeout,
            metadata=metadata,
            credentials=client_call_details.credentials,
            wait_for_ready=client_call_details.wait_for_ready,
        )
    elif isinstance(client_call_details.metadata, grpc.aio.Metadata):
        # If  proper grpc `grpc.aio.Metadata()` object is passed just use original object passed and pass back
        new_client_call_details = client_call_details
    else:
        # Else we raise exception for now since we don't know how to handle an unknown type
        raise InvalidArgumentException(
            "unexpected grpc client request metadata property passed to interceptor "
            "type=" + str(type(client_call_details.metadata)),
            Service.AUTH,
        )

    return new_client_call_details


# noinspection PyArgumentList
def create_client_call_details(
    method: str,
    timeout: Optional[float],
    metadata: Optional[Metadata],
    credentials: Optional[grpc.CallCredentials],
    wait_for_ready: Optional[bool],
) -> ClientCallDetails:
    return ClientCallDetails(
        method=method,
        timeout=timeout,
        metadata=metadata,
        credentials=credentials,
        wait_for_ready=wait_for_ready,
    )
