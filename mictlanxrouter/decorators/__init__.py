from functools import wraps
from fastapi import Request, HTTPException
import asyncio
from mictlanx.logger import Log
import os

LOG_PATH                    = os.environ.get("LOG_PATH","/log")

log                         = Log(
        name                   = __name__,
        console_handler_filter = lambda x: True,
        interval               = 24,
        when                   = "h",
        path                   = LOG_PATH
)
def disconnect_protected():
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            request = kwargs.get("request", None)
            log.debug(str(request))
            if request is None:
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break
            if request is None:
                e ="Request object not found in handler."
                log.error(e)
                raise RuntimeError(e)

            task = asyncio.create_task(monitor_disconnect(request))
            try:
                return await func(*args, **kwargs)
            finally:
                task.cancel()

        return wrapper
    return decorator

async def monitor_disconnect(request: Request):
    while True:
        if await request.is_disconnected():
            log.debug("Cancel is disconnected")
            raise HTTPException(status_code=499, detail="Client disconnected")
        await asyncio.sleep(0.5)