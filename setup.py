import setuptools
import os

version = os.getenv("MOMENTO_SDK_VERSION")

if(version is None):
    version = '0.0.0+localdev0'

with open("README.md", "r") as fh:
    long_description = fh.read()

# version is the only dynamic configuration
setuptools.setup(
    description='Python SDK for Momento',
    name='momento',
    version=version,
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='Apache License 2.0',
    author="Momento",
    packages=setuptools.find_packages('src'),
    package_dir={'':'src'}, 
    python_requires='>=3.7',
)
