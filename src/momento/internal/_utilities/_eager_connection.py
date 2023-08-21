import threading
from datetime import timedelta
from typing import Any

import grpc

from momento.config import Configuration

"""
    This method tries to connect to Momento's server eagerly in async fashion until
    EAGER_CONNECTION_TIMEOUT elapses.
"""


def _eagerly_connect(self: Any, configuration: Configuration) -> None:  # type: ignore
    eager_connection_timeout: timedelta = (
        configuration.get_transport_strategy().get_grpc_configuration().get_eager_connection_timeout()
    )

    def on_timeout() -> None:
        self._logger.debug(  # type: ignore
            "We could not establish an eager connection within %d seconds",
            eager_connection_timeout.seconds,
        )
        # the subscription is no longer needed; it was only meant to watch if we could connect eagerly
        self._secure_channel.unsubscribe(on_state_change)  # type: ignore

    """
    A callback that is triggered whenever a connection's state changes. We explicitly subscribe to
    to the channel to notify us of state transitions. This method essentially handles unsubscribing
    as soon as we reach the desired state (or an unexpected one). In theory this callback isn't needed
    to eagerly connect, but we still need it to not have a lurking subscription.
    """

    def on_state_change(state: grpc.ChannelConnectivity) -> None:
        ready: grpc.ChannelConnectivity = grpc.ChannelConnectivity.READY  # type: ignore
        connecting: grpc.ChannelConnectivity = grpc.ChannelConnectivity.CONNECTING  # type: ignore
        idle: grpc.ChannelConnectivity = grpc.ChannelConnectivity.IDLE  # type: ignore

        if state == ready:  # type: ignore
            self._logger.debug("Connected to Momento's server!")  # type: ignore
            # we successfully connected within the timeout and we no longer need this subscription
            timer.cancel()
            self._secure_channel.unsubscribe(on_state_change)  # type: ignore
        elif state == idle:  # type: ignore
            self._logger.debug("State is idle; waiting to transition to CONNECTING")  # type: ignore
        elif state == connecting:  # type: ignore
            self._logger.debug("State transitioned to CONNECTING; waiting to get READY")  # type: ignore
        else:
            self._logger.debug(f"Unexpected connection state: {state}.")  # type: ignore
            # we could not connect within the timeout and we no longer need this subscription
            timer.cancel()
            self._secure_channel.unsubscribe(on_state_change)  # type: ignore

    # on_timeout is automatically called once eager_connection_timeout has passed
    timer = threading.Timer(eager_connection_timeout.seconds, on_timeout)
    timer.start()

    # we subscribe to the channel that notifies us of state transitions, and the timer above will take care
    # of unsubscribing from the channel incase the timeout has elapsed.
    self._secure_channel.subscribe(on_state_change, try_to_connect=True)  # type: ignore
