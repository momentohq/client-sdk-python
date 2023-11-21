from typing import List, Optional

import grpc.aio
import pytest
from momento.errors import InvalidArgumentException
from momento.internal.aio._add_header_client_interceptor import (
    sanitize_client_call_details,
)


def test_sanitize_client_grpc_request() -> None:
    class MockGrpcRequestDetails:
        method: str
        timeout: Optional[float]
        # Only difference in this test class from grpc.aio.Metadata is the metadata property which can be any so in
        # tests we can replicate what we are seeing in the wild.
        metadata: grpc.aio.Metadata
        credentials: Optional[grpc.CallCredentials]
        wait_for_ready: Optional[bool]

    def build_test_client_request(metadata_to_set: object) -> MockGrpcRequestDetails:
        return_request = MockGrpcRequestDetails()
        return_request.method = ""
        return_request.timeout = 1
        return_request.metadata = metadata_to_set
        return_request.method = "test"
        return_request.credentials = None
        return_request.wait_for_ready = True
        return return_request

    happy_path_expected_output = grpc.aio.ClientCallDetails(
        timeout=1,
        metadata=grpc.aio.Metadata(),  # important! we want grpc.io.Metadata object in response
        method="test",
        credentials=None,
        wait_for_ready=True,
    )

    class TestCase:
        client_input: Optional[MockGrpcRequestDetails] = None
        expected_output: grpc.aio.ClientCallDetails = None
        expected_err: Optional[Exception] = None

        def __init__(
            self,
            client_input: MockGrpcRequestDetails,
            expected_output: Optional[MockGrpcRequestDetails],
            expected_err: Optional[Exception],
        ):
            self.client_input = client_input
            self.expected_output = expected_output
            self.expected_err = expected_err

    test_cases: List[TestCase] = [
        TestCase(
            client_input=build_test_client_request(metadata_to_set=None),  # Test None
            expected_output=happy_path_expected_output,
            expected_err=None,
        ),
        TestCase(
            client_input=build_test_client_request(metadata_to_set=[]),  # Test Basic List
            expected_output=happy_path_expected_output,
            expected_err=None,
        ),
        TestCase(
            # Test with Metadata() set properly
            client_input=build_test_client_request(metadata_to_set=grpc.aio.Metadata()),
            expected_output=happy_path_expected_output,
            expected_err=None,
        ),
        TestCase(
            # Test with unknown generic dict passed as metadata
            client_input=build_test_client_request(metadata_to_set={}),
            expected_output=None,
            # Make sure we throw on unkown metadata input
            expected_err=InvalidArgumentException,  # type: ignore[arg-type]
        ),
    ]

    for test in test_cases:
        if test.expected_err is not None:
            with pytest.raises(test.expected_err):  # type: ignore
                sanitize_client_call_details(test.client_input)
        else:
            assert sanitize_client_call_details(test.client_input).metadata == test.expected_output.metadata
