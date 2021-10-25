import setuptools
import os
import time

version = os.getenv("PYPI_MOMENTO_WIRE_TYPE_VERSION")

if [version == None]:
    version = '0.0.dev'

# version is the only dynamic configuration
setuptools.setup(
    version=version,
)
