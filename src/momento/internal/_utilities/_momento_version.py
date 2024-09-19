"""Detect the version of momento installed.

This module is used to detect the version of momento installed. It is used
internally to add the version to the gRPC headers.
"""

from importlib.metadata import PackageNotFoundError

momento_version = ""

try:
    try:
        # For python < 3.8
        import importlib_metadata

        momento_version = importlib_metadata.Distribution.from_name("momento").version  # type: ignore[no-untyped-call,misc]
    except ImportError:
        # For python >= 3.8
        from importlib.metadata import version  # type: ignore[import]

        momento_version = version("momento")
except (ImportError, ModuleNotFoundError, PackageNotFoundError):
    # Fall back to setting to the version manually
    momento_version = "1.21.2"
