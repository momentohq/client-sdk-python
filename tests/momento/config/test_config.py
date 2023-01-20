from datetime import timedelta

from momento.config.configuration import Configuration


def test_configuration_client_timeout_copy_constructor(configuration: Configuration) -> None:
    def snag_deadline(config: Configuration) -> timedelta:
        return config.get_transport_strategy().get_grpc_configuration().get_deadline()  # type: ignore

    original_deadline: timedelta = snag_deadline(configuration)
    assert original_deadline.total_seconds() == 15
    configuration = configuration.with_client_timeout(timedelta(seconds=600))
    assert snag_deadline(configuration).total_seconds() == 600
