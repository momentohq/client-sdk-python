from __future__ import annotations

from .topic_configuration import TopicConfiguration


class TopicConfigurations:

    class Default(TopicConfiguration):

        @staticmethod
        def latest() -> TopicConfigurations.Default:
            pass

        @staticmethod
        def v1() -> TopicConfigurations.Default:
            return TopicConfigurations.Default(
                max_subscriptions=0
            )
