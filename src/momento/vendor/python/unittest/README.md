# Vendored async testcase

This directory vendors some test utility code that was introduced in
python 3.8, for testing asyncio.  We need this in order to run our
test matrix against python 3.7.

We should remove this as soon as our minimum supported python version
is 3.8.

Vendored from:

https://github.com/python/cpython/blob/7c5b01b5101923fc38274c491bd55239ee9f0416/Lib/unittest/async_case.py
