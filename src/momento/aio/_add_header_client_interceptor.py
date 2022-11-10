from typing import Callable, List, Union

import grpc
from grpc.aio import Metadata, ClientCallDetails


class Header:
    once_only_headers = ["agent"]

    def __init__(self, name: str, value: str):
        self.name = name
        self.value = value


class AddHeaderClientInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    are_only_once_headers_sent = False

    def __init__(self, headers: List[Header]):
        self._headers_to_add_once: List[Header] = list(
            filter(lambda header: header.name in header.once_only_headers, headers)
        )
        self.headers_to_add_every_time = list(
            filter(lambda header: header.name not in header.once_only_headers, headers)
        )

    async def intercept_unary_unary(
            self,
            continuation: Callable[
                [grpc.aio._interceptor.ClientCallDetails, grpc.aio._typing.RequestType],
                grpc.aio._call.UnaryUnaryCall,
            ],
            client_call_details: grpc.aio._interceptor.ClientCallDetails,
            request: grpc.aio._typing.RequestType,
    ) -> Union[grpc.aio._call.UnaryUnaryCall, grpc.aio._typing.ResponseType]:

        # FIXME Hack to account for ddtrace lib injecting grpc metadata as list instead of proper grpc.aio.Metadata()
        # Will open issue on data dog repo to discuss this more.
        # Since once client_call_details.metadata is set as list we cant overwrite with `Metadata()` class we build
        # up a new ClientCallDetails() in each request now in this interceptor and make sure Metadata() is set properly
        new_client_call_details = None

        # If no metadata set on passed in client call details then we are first to set, so we should just initialize
        if client_call_details.metadata is None:
            new_client_call_details = ClientCallDetails(
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
            new_client_call_details = ClientCallDetails(
                method=client_call_details.method,
                timeout=client_call_details.timeout,
                metadata=metadata,
                credentials=client_call_details.credentials,
                wait_for_ready=client_call_details.wait_for_ready,
            )
        else:
            # Default assuming previous interceptors have passed proper `Metadata()` object for now and just copy over
            new_client_call_details = ClientCallDetails(
                method=client_call_details.method,
                timeout=client_call_details.timeout,
                metadata=client_call_details.metadata,
                credentials=client_call_details.credentials,
                wait_for_ready=client_call_details.wait_for_ready,
            )
        for header in self.headers_to_add_every_time:
            new_client_call_details.metadata.add(header.name, header.value)
        if not AddHeaderClientInterceptor.are_only_once_headers_sent:
            for header in self._headers_to_add_once:
                new_client_call_details.metadata.add(header.name, header.value)
            AddHeaderClientInterceptor.are_only_once_headers_sent = True

        return await continuation(new_client_call_details, request)
