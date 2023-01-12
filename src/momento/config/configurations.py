from __future__ import annotations
from datetime import timedelta

from .configuration import Configuration
from .transport.transport_strategy import StaticTransportStrategy, StaticGrpcConfiguration


class Laptop(Configuration):
    @staticmethod
    def latest() -> Laptop:
        return Laptop(StaticTransportStrategy(StaticGrpcConfiguration(timedelta(seconds=15))))


class InRegion:
    class Default(Configuration):
        @staticmethod
        def latest() -> InRegion.Default:
            return InRegion.Default(StaticTransportStrategy(StaticGrpcConfiguration(timedelta(milliseconds=1100))))

    class LowLatency(Configuration):
        @staticmethod
        def latest() -> InRegion.LowLatency:
            return InRegion.LowLatency(StaticTransportStrategy(StaticGrpcConfiguration(timedelta(milliseconds=500))))
