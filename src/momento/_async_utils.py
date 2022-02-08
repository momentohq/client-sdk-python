import asyncio
from typing import Any


# NOTES:
#
# 1. it would be really nice to put some more meaningful type hints here but I'm not familiar
#    enough with asyncio / coroutines to understand their types.
# 2. Kenny believes that we should be able to do this without a direct dependency on asyncio,
#    by writing a loop that calls `send` on the coroutine and then returns on StopIteration.
#    I was not able to get this working during my timeboxed window so left it like this for now.
def wait_for_coroutine(coroutine: Any) -> Any:  # type: ignore[misc]
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(coroutine)  # type: ignore[misc]
