#!/bin/bash
set -e
set -x
echo "[distutils]"
echo "index-servers = pypi"
echo "[pypi]"
echo "username: $1"
echo "password: $2"
echo ""
