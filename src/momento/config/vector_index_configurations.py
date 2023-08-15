from __future__ import annotations

from datetime import timedelta

from momento.retry import FixedCountRetryStrategy

from .configuration import Configuration
from .transport.transport_strategy import (
    StaticGrpcConfiguration,
    StaticTransportStrategy,
)


class VectorIndexConfigurations:
    """Container class for pre-built configurations."""

    class Default(Configuration):
        """Laptop config provides defaults suitable for a medium-to-high-latency dev environment.

        Permissive timeouts, retries, and relaxed latency and throughput targets.
        """

        @staticmethod
        def latest() -> VectorIndexConfigurations.Default:
            """Provides the latest recommended configuration for a laptop development environment.

            This configuration will be updated every time there is a new version of the laptop configuration.
            """
            return VectorIndexConfigurations.Default(
                # This is high to account for time-intensive upserts.
                StaticTransportStrategy(StaticGrpcConfiguration(timedelta(seconds=120))),
                FixedCountRetryStrategy(max_attempts=3),
            )
