#!/bin/bash
# Configure Git Hub Workflow environment for Uploading to JFrog
set -e
set -x
echo "[distutils]"
echo "index-servers = local"
echo "[local]"
echo "repository: https://momento.jfrog.io/artifactory/api/pypi/pypi-local"
echo "username: $1"
echo "password: $2"
echo ""
