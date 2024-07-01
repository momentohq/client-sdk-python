"""Python runtime version information.

Used to populate the `runtime-version` header in gRPC requests.
"""
import sys

PYTHON_RUNTIME_VERSION = (
    f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro} "
    f"({sys.version_info.releaselevel} {sys.version_info.serial})"
)
