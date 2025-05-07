import asyncio
from typing import List

from grpc.aio import Metadata
from momento import logs
from momento.config.middleware import MiddlewareMessage, MiddlewareRequestHandlerContext, MiddlewareStatus
from momento.config.middleware.aio import Middleware, MiddlewareMetadata, MiddlewareRequestHandler

from tests.momento.local.momento_error_code_metadata import MOMENTO_ERROR_CODE_TO_METADATA
from tests.momento.local.momento_local_middleware_args import MomentoLocalMiddlewareArgs
from tests.momento.local.momento_rpc_method import MomentoRpcMethod


class MomentoLocalAsyncMiddlewareRequestHandler(MiddlewareRequestHandler):
    def __init__(self, args: MomentoLocalMiddlewareArgs):
        self._args = args
        self._cache_name = None
        self._logger = logs.logger

    async def on_request_metadata(self, metadata: MiddlewareMetadata) -> MiddlewareMetadata:
        grpc_metadata = metadata.grpc_metadata

        if grpc_metadata is not None:
            self._set_grpc_metadata(grpc_metadata, "request-id", self._args.request_id)

            if self._args.return_error is not None:
                error = MOMENTO_ERROR_CODE_TO_METADATA[self._args.return_error]
                if error is not None:
                    self._set_grpc_metadata(grpc_metadata, "return-error", error)

            if self._args.error_rpc_list is not None:
                rpcs = self._concatenate_rpcs(self._args.error_rpc_list)
                self._set_grpc_metadata(grpc_metadata, "error-rpcs", rpcs)

            if self._args.delay_rpc_list is not None:
                rpcs = self._concatenate_rpcs(self._args.delay_rpc_list)
                self._set_grpc_metadata(grpc_metadata, "delay-rpcs", rpcs)

            if self._args.error_count is not None:
                self._set_grpc_metadata(grpc_metadata, "error-count", str(self._args.error_count))

            if self._args.delay_millis is not None:
                self._set_grpc_metadata(grpc_metadata, "delay-ms", str(self._args.delay_millis))

            if self._args.delay_count is not None:
                self._set_grpc_metadata(grpc_metadata, "delay-count", str(self._args.delay_count))

            if self._args.stream_error_rpc_list is not None:
                rpcs = self._concatenate_rpcs(self._args.stream_error_rpc_list)
                self._set_grpc_metadata(grpc_metadata, "stream-error-rpcs", rpcs)

            if self._args.stream_error is not None:
                error = MOMENTO_ERROR_CODE_TO_METADATA[self._args.stream_error]
                if error is not None:
                    self._set_grpc_metadata(grpc_metadata, "stream-error", error)

            if self._args.stream_error_message_limit is not None:
                limit_str = str(self._args.stream_error_message_limit)
                self._set_grpc_metadata(grpc_metadata, "stream-error-message-limit", limit_str)

            cache_name = grpc_metadata.get("cache")
            if cache_name is not None:
                self._cache_name = cache_name
            else:
                self._logger.debug("No cache name found in metadata.")

        return metadata

    async def on_request_body(self, request: MiddlewareMessage) -> MiddlewareMessage:
        request_type = request.constructor_name

        if self._cache_name is not None:
            if self._args.test_metrics_collector is not None:  # type: ignore[unreachable]
                rpc_method = MomentoRpcMethod.from_request_name(request_type)
                if rpc_method:
                    self._args.test_metrics_collector.add_timestamp(
                        self._cache_name,
                        rpc_method,
                        int(asyncio.get_event_loop().time() * 1000),  # Current time in milliseconds
                    )
        else:
            self._logger.debug("No cache name available. Timestamp will not be collected.")

        return request

    async def on_response_metadata(self, metadata: MiddlewareMetadata) -> MiddlewareMetadata:
        return metadata

    async def on_response_body(self, response: MiddlewareMessage) -> MiddlewareMessage:
        return response

    async def on_response_status(self, status: MiddlewareStatus) -> MiddlewareStatus:
        return status

    @staticmethod
    def _set_grpc_metadata(metadata: Metadata, key: str, value: str) -> None:
        if value is not None:
            metadata[key] = value

    @staticmethod
    def _concatenate_rpcs(rpcs: List[MomentoRpcMethod]) -> str:
        return " ".join(rpc.metadata for rpc in rpcs)


class MomentoLocalAsyncMiddleware(Middleware):
    def __init__(self, args: MomentoLocalMiddlewareArgs):
        self._args = args

    async def on_new_request(self, context: MiddlewareRequestHandlerContext) -> MiddlewareRequestHandler:
        return MomentoLocalAsyncMiddlewareRequestHandler(self._args)
