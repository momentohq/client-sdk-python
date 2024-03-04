from __future__ import annotations

from datetime import timedelta

from .transport.transport_strategy import (
    StaticGrpcConfiguration,
    StaticTransportStrategy,
)
from .vector_index_configuration import VectorIndexConfiguration


class VectorIndexConfigurations:
    """Container class for pre-built configurations."""

    class Default(VectorIndexConfiguration):
        """Laptop config provides defaults suitable for a medium-to-high-latency dev environment.

        Permissive timeouts, retries, and relaxed latency and throughput targets.
        """

        @staticmethod
        def latest() -> VectorIndexConfigurations.Default:
            """Provides the latest recommended configuration for a laptop development environment.

            This configuration will be updated every time there is a new version of the laptop configuration.
            """
            return VectorIndexConfigurations.Default(
                # The deadline is high to account for time-intensive adds.
                StaticTransportStrategy(StaticGrpcConfiguration(deadline=timedelta(seconds=120)))
            )
