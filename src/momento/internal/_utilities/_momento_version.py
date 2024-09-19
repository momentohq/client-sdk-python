"""Detect the version of momento installed.

This module is used to detect the version of momento installed. It is used
internally to add the version to the gRPC headers.
"""

momento_version = ""
DEFAULT_MOMENTO_VERSION = "1.21.2"


def set_momento_version(version: str) -> None:
    """Set the version of momento installed.

    Args:
        version: The version of momento installed.
    """
    global momento_version
    momento_version = version


try:
    # For python < 3.8
    import importlib_metadata

    set_momento_version(importlib_metadata.Distribution.from_name("momento").version)  # type: ignore[no-untyped-call,misc]
except ModuleNotFoundError:
    # Fall back to setting to the version manually
    set_momento_version(DEFAULT_MOMENTO_VERSION)
except ImportError:
    # For python >= 3.8
    from importlib.metadata import PackageNotFoundError, version  # type: ignore[import]

    try:
        set_momento_version(version("momento"))
    except PackageNotFoundError:
        # Fall back to setting to the version manually
        set_momento_version(DEFAULT_MOMENTO_VERSION)
