#!/bin/bash

# Since the synchronous client delegates all functionality
# to the asynchronous client, we generate the tests for the
# sychronous client from the asynchronous ones.
#
# Why bother with synchronous tests at all then? Because there
# is still room for error (typos for example) when delegating.
# Synchronous tests will catch that.
# 
# Yes, this has happened before.
# 
# Usage: ./sync_from_async.sh

function async_to_sync() {
    async_file=$1
    sync_file=$2

    cp $async_file $sync_file

    sed -i -e 's/async //g' $sync_file
    sed -i -e 's/await //g' $sync_file
    sed -i -e 's/momento.aio./momento./g' $sync_file
    sed -i -e 's/momento.incubating.aio.simple/momento.incubating.simple/g' $sync_file
    sed -i -e 's/client_async/client/g' $sync_file
    black $sync_file
}

for async_file in tests/*async.py tests/incubating/*async.py
do
    sync_file=`echo $async_file | sed -e s/_async//`
    echo Translating $async_file to $sync_file
    async_to_sync $async_file $sync_file
done
