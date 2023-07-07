from __future__ import annotations

from abc import ABC, abstractmethod


class TopicConfigurationBase(ABC):

    @abstractmethod
    def get_max_subscriptions(self) -> int:
        pass

    @abstractmethod
    def with_max_subscriptions(self, max_subscriptions: int) -> TopicConfiguration:
        pass


class TopicConfiguration(TopicConfigurationBase):

    def __init__(self, max_subscriptions: int = 0):
        self._max_subscriptions = max_subscriptions

    def get_max_subscriptions(self) -> int:
        return self._max_subscriptions

    def with_max_subscriptions(self, max_subscriptions: int) -> TopicConfiguration:
        return TopicConfiguration(max_subscriptions)
