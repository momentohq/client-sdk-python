from __future__ import annotations

from datetime import timedelta

from momento.retry import FixedCountRetryStrategy

from .configuration import Configuration
from .transport.transport_strategy import (
    StaticGrpcConfiguration,
    StaticTransportStrategy,
)


class Configurations:
    """Container class for pre-built configurations."""

    class Laptop(Configuration):
        """Laptop config provides defaults suitable for a medium-to-high-latency dev environment.

        Permissive timeouts, retries, and relaxed latency and throughput targets.
        """

        @staticmethod
        def latest() -> Configurations.Laptop:
            """Provides the latest recommended configuration for a laptop development environment.

            This configuration will be updated every time there is a new version of the laptop configuration.
            """
            return Configurations.Laptop.v1()

        @staticmethod
        def v1() -> Configurations.Laptop:
            """Provides the v1 recommended configuration for a laptop development environment.

            This configuration is guaranteed not to change in future releases of the Momento Python SDK.
            """
            return Configurations.Laptop(
                StaticTransportStrategy(StaticGrpcConfiguration(timedelta(seconds=15))),
                FixedCountRetryStrategy(max_attempts=3),
            )

    class InRegion:
        """Default for application running in the same region as the Momento service.

        InRegion provides defaults suitable for an environment where your client is running in the same region as the
        Momento service.  It has more aggressive timeouts and retry behavior than the Laptop config.
        """

        class Default(Configuration):
            """Prioritizes throughput and client resource utilization.

            It has a slightly relaxed client-side timeout setting to maximize throughput.
            """

            @staticmethod
            def latest() -> Configurations.InRegion.Default:
                """Provides the latest recommended configuration for a typical in-region environment.

                This configuration will be updated every time there is a new version of the in-region configuration.
                """
                return Configurations.InRegion.Default.v1()

            @staticmethod
            def v1() -> Configurations.InRegion.Default:
                """Provides the v1 recommended configuration for a typical in-region environment.

                This configuration is guaranteed not to change in future releases of the Momento Python SDK.
                """
                return Configurations.InRegion.Default(
                    StaticTransportStrategy(StaticGrpcConfiguration(timedelta(milliseconds=1100))),
                    FixedCountRetryStrategy(max_attempts=3),
                )

        class LowLatency(Configuration):
            """Prioritizes keeping p99.9 latencies as low as possible.

            It potentially sacrifices some throughput to achieve this.  It has a very aggressive client-side timeout.
            Use this configuration if the most important factor is to ensure that cache unavailability doesn't
            force unacceptably high latencies for your application.
            """

            @staticmethod
            def latest() -> Configurations.InRegion.LowLatency:
                """Provides the latest recommended configuration for an environment with low-latency requirements.

                This configuration will be updated every time there is a new version of the low-latency configuration.
                """
                return Configurations.InRegion.LowLatency.v1()

            @staticmethod
            def v1() -> Configurations.InRegion.LowLatency:
                """Provides the v1 recommended configuration for an environment with low-latency requirements.

                This configuration is guaranteed not to change in future releases of the Momento Python SDK.
                """
                return Configurations.InRegion.LowLatency(
                    StaticTransportStrategy(StaticGrpcConfiguration(timedelta(milliseconds=500))),
                    FixedCountRetryStrategy(max_attempts=3),
                )
