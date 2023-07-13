from __future__ import annotations

from .topic_configuration import TopicConfiguration


class TopicConfigurations:
    class Default(TopicConfiguration):
        """Provides the recommended default configuration for topic clients."""

        @staticmethod
        def latest() -> TopicConfigurations.Default:
            """Provides the latest recommended configuration for topics.

            This configuration will be updated every time there is a new version of the laptop configuration.
            """
            return TopicConfigurations.Default.v1()

        @staticmethod
        def v1() -> TopicConfigurations.Default:
            """Provides the v1 recommended configuration for topics.

            This configuration is guaranteed not to change in future releases of the Momento Python SDK.
            """
            return TopicConfigurations.Default(max_subscriptions=0)
