import setuptools
import os

version = os.getenv("MOMENTO_SDK_VERSION")

if(version is None):
    version = '0.0.localdev0'

# version is the only dynamic configuration
setuptools.setup(
    version=version,
)
