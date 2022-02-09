import setuptools
import os

version = os.getenv("MOMENTO_SDK_VERSION")

if(version is None):
    version = '0.0.0+localdev0'

with open("README.md", "r") as fh:
    long_description = fh.read()

# version is the only dynamic configuration
setuptools.setup(
    version=version,
    long_description=long_description,
    long_description_content_type="text/markdown",
)
