from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import List, Optional

import momento.config.middleware.aio
from momento.retry import RetryStrategy

from .middleware import Middleware
from .transport.transport_strategy import TransportStrategy


class Configuration:
    """Configuration options for Momento Cache Client."""

    def __init__(
        self,
        transport_strategy: TransportStrategy,
        retry_strategy: RetryStrategy,
        middlewares: Optional[List[Middleware]] = None,
    ):
        """Instantiate a Configuration.

        Args:
            transport_strategy (TransportStrategy): Configuration options for networking with
            the Momento service.
            retry_strategy (RetryStrategy): the strategy to use when determining whether to retry a grpc call.
            middlewares: Middleware that can intercept Momento calls. May be aio or synchronous.
        """
        self._transport_strategy = transport_strategy
        self._retry_strategy = retry_strategy
        self._middlewares: List[Middleware] = list(middlewares or [])

    def get_retry_strategy(self) -> RetryStrategy:
        """Access the retry strategy.

        Returns:
            RetryStrategy: the strategy to use when determining whether to retry a grpc call.
        """
        return self._retry_strategy

    def with_retry_strategy(self, retry_strategy: RetryStrategy) -> Configuration:
        """Copy constructor for overriding RetryStrategy.

        Args:
            retry_strategy (RetryStrategy): the new RetryStrategy.

        Returns:
            Configuration: the new Configuration with the specified RetryStrategy.
        """
        return Configuration(self._transport_strategy, retry_strategy, self._middlewares)

    def get_transport_strategy(self) -> TransportStrategy:
        """Access the transport strategy.

        Returns:
            TransportStrategy: the current configuration options for wire interactions with the Momento service.
        """
        return self._transport_strategy

    def with_transport_strategy(self, transport_strategy: TransportStrategy) -> Configuration:
        """Copy constructor for overriding TransportStrategy.

        Args:
            transport_strategy (TransportStrategy): the new TransportStrategy.

        Returns:
            Configuration: the new Configuration with the specified TransportStrategy.
        """
        return Configuration(transport_strategy, self._retry_strategy, self._middlewares)

    def with_client_timeout(self, client_timeout: timedelta) -> Configuration:
        """Copies the Configuration and sets the new client-side timeout in the copy's TransportStrategy.

        Args:
            client_timeout (timedelta): the new client-side timeout.

        Return:
            Configuration: the new Configuration.
        """
        return Configuration(
            self._transport_strategy.with_client_timeout(client_timeout),
            self._retry_strategy,
            self._middlewares,
        )

    def with_root_certificates_pem(self, root_certificates_pem_path: Path) -> Configuration:
        """Copies the Configuration and sets the new root certificates in the copy's TransportStrategy.

        Args:
            root_certificates_pem_path (Path): the new root certificates.

        Returns:
            Configuration: the new Configuration.
        """
        grpc_configuration = self._transport_strategy.get_grpc_configuration().with_root_certificates_pem(
            root_certificates_pem_path
        )
        transport_strategy = self._transport_strategy.with_grpc_configuration(grpc_configuration)
        return self.with_transport_strategy(transport_strategy)

    def with_middlewares(self, middlewares: List[Middleware]) -> Configuration:
        """Copies the Configuration and replaces the middleware with the given middleware list.

        Args:
            middlewares: the new middleware list. It can contain async or synchronous middleware.

        Returns:
            Configuration: the new Configuration.
        """
        return Configuration(self._transport_strategy, self._retry_strategy, middlewares)

    def add_middleware(self, middleware: Middleware) -> Configuration:
        """Copies the Configuration and adds the new middleware to the end of the list.

        Args:
            middleware: the middleware to be appended to the Configuration's existing middleware. This can be an async
            or synchronous middleware.

        Returns:
            Configuration: the new Configuration.
        """
        new_middlewares = self._middlewares.copy() + [middleware]
        return Configuration(self._transport_strategy, self._retry_strategy, new_middlewares)

    def get_middlewares(self) -> List[Middleware]:
        """Access the middleware list.

        Returns:
            the configuration's list of middleware.
        """
        return self._middlewares.copy()

    def get_async_middlewares(self) -> List[momento.config.middleware.aio.Middleware]:
        """Access the async middleware from the middleware list.

        Returns:
            the configuration's list of async middleware.
        """
        return [m for m in self._middlewares if isinstance(m, momento.config.middleware.aio.Middleware)]

    def get_sync_middlewares(self) -> List[momento.config.middleware.synchronous.Middleware]:
        """Access the synchronous middleware from the middleware list.

        Returns:
            the configuration's list of synchronous middleware.
        """
        return [m for m in self._middlewares if isinstance(m, momento.config.middleware.synchronous.Middleware)]
