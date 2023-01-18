from __future__ import annotations

from datetime import timedelta

from .configuration import Configuration
from .transport.transport_strategy import (
    StaticGrpcConfiguration,
    StaticTransportStrategy,
)


class Laptop(Configuration):
    """Laptop config provides defaults suitable for a medium-to-high-latency dev environment.  Permissive timeouts,
    retries, and relaxed latency and throughput targets.
    """

    @staticmethod
    def latest() -> Laptop:
        return Laptop(StaticTransportStrategy(StaticGrpcConfiguration(timedelta(seconds=15))))


class InRegion:
    """InRegion provides defaults suitable for an environment where your client is running in the same region as the
    Momento service.  It has more aggressive timeouts and retry behavior than the Laptop config.
    """

    class Default(Configuration):
        """This config prioritizes throughput and client resource utilization.  It has a slightly relaxed client-side
        timeout setting to maximize throughput."""

        @staticmethod
        def latest() -> InRegion.Default:
            return InRegion.Default(StaticTransportStrategy(StaticGrpcConfiguration(timedelta(milliseconds=1100))))

    class LowLatency(Configuration):
        """This config prioritizes keeping p99.9 latencies as low as possible, potentially sacrificing some throughput
        to achieve this.  It has a very aggressive client-side timeout.  Use this configuration if the most important
        factor is to ensure that cache unavailability doesn't force unacceptably high latencies for your own
        application."""

        @staticmethod
        def latest() -> InRegion.LowLatency:
            return InRegion.LowLatency(StaticTransportStrategy(StaticGrpcConfiguration(timedelta(milliseconds=500))))
