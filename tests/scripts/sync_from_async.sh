#!/bin/bash

# Use this script to generate synchronous client tests
# from async tests.
# 
# Usage: ./sync_from_async.sh test_file_async.py test_file.py

async_file=$1
sync_file=$2

cp $async_file $sync_file

sed -i -e 's/async //g' $sync_file
sed -i -e 's/await //g' $sync_file
sed -i -e 's/momento.aio./momento./g' $sync_file
black $sync_file
