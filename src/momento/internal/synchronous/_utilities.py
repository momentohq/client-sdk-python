from __future__ import annotations

import collections
from typing import Optional, Tuple

import grpc
from grpc import CallCredentials
from grpc._typing import MetadataType

from momento.errors import InvalidArgumentException
from momento.internal.services import Service


def make_metadata(cache_name: str) -> list[Tuple[str, str]]:
    return [("cache", cache_name)]


class _ClientCallDetails(
    collections.namedtuple("_ClientCallDetails", ("method", "timeout", "metadata", "credentials")),
    grpc.ClientCallDetails,
):
    def __new__(
        cls, method: str, timeout: Optional[float], metadata: MetadataType, credentials: Optional[CallCredentials]
    ) -> _ClientCallDetails:
        return super().__new__(cls, method, timeout, metadata, credentials)


def sanitize_client_call_details(client_call_details: grpc.ClientCallDetails) -> grpc.ClientCallDetails:
    """Defensive function meant to handle inbound gRPC client request objects.

    Args:
        client_call_details: the original inbound client grpc request we are intercepting

    Returns: a new client_call_details object with metadata properly initialized to a `grpc.aio.Metadata` object
    """
    # Makes sure we can handle properly when we inject our own metadata onto request object.
    # This was mainly done as temporary fix after we observed ddtrace grpc client interceptor passing
    # client_call_details.metadata as a list instead of a grpc.aio.Metadata object.
    # See this ticket for follow-up actions to come back in and address this longer term:
    #    https://github.com/momentohq/client-sdk-python/issues/149
    # If no metadata set on passed in client call details then we are first to set, so we should just initialize
    if client_call_details.metadata is None:
        return _ClientCallDetails(
            method=client_call_details.method,
            timeout=client_call_details.timeout,
            metadata=[],
            credentials=client_call_details.credentials,
        )

    # This is block hit when ddtrace interceptor runs first and sets metadata as a list
    elif isinstance(client_call_details.metadata, list):
        return client_call_details
    else:
        # Else we raise exception for now since we don't know how to handle an unknown type
        raise InvalidArgumentException(
            "unexpected grpc client request metadata property passed to interceptor "
            "type=" + str(type(client_call_details.metadata)),
            Service.AUTH,
        )
