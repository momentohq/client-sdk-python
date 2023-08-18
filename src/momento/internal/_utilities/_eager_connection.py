import datetime

import grpc
import threading

EAGER_CONNECTION_TIMEOUT = datetime.timedelta(seconds=30)

'''
    This method tries to connect to Momento's server eagerly in async fashion until 
    EAGER_CONNECTION_TIMEOUT elapses. 
'''
def _eagerly_connect(self, configuration) -> None:
    eager_connection_timeout = configuration.get_transport_strategy() \
        .get_grpc_configuration().get_eager_connection_timeout()
    if eager_connection_timeout is None:
        eager_connection_timeout = EAGER_CONNECTION_TIMEOUT

    def on_timeout():
        self._logger.debug("We could not establish an eager connection within %d seconds",
                           eager_connection_timeout)
        # the subscription is no longer needed; it was only meant to watch if we could connect eagerly
        self._secure_channel.unsubscribe(on_state_change)

    '''
        A callback that is triggered whenever a connection's state changes. We explicitly subscribe to
        to the channel to notify us of state transitions. This method essentially handles unsubscribing 
        as soon as we reach the desired state (or an unexpected one). In theory this callback isn't needed
        to eagerly connect, but we still need it to not have a lurking subscription.
    '''
    def on_state_change(state: grpc.ChannelConnectivity) -> None:
        ready = grpc.ChannelConnectivity.READY
        connecting = grpc.ChannelConnectivity.CONNECTING
        idle = grpc.ChannelConnectivity.IDLE

        if state == ready:
            self._logger.debug("Connected to Momento's server!")
            # we successfully connected within the timeout and we no longer need this subscription
            timer.cancel()
            self._secure_channel.unsubscribe(on_state_change)
        elif state == idle:
            self._logger.debug("State is idle; waiting to transition to CONNECTING")
        elif state == connecting:
            self._logger.debug("State transitioned to CONNECTING; waiting to get READY")
        else:
            self._logger.debug(f"Unexpected connection state: {state}. Please contact Momento if this persists.")
            # we could not connect within the timeout and we no longer need this subscription
            timer.cancel()
            self._secure_channel.unsubscribe(on_state_change)

    # on_timeout is automatically called once eager_connection_timeout has passed
    timer = threading.Timer(eager_connection_timeout, on_timeout)
    timer.start()

    # we subscribe to the channel that notifies us of state transitions, and the timer above will take care
    # of unsubscribing from the channel incase the timeout has elapsed.
    self._secure_channel.subscribe(on_state_change, try_to_connect=True)
