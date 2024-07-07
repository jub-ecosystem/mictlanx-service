import humanfriendly as HF
import asyncio
import os
import time as T
from mictlanxrouter.peer_manager import StoragePeerManager
from mictlanx.logger.log import Log
import queue

MICTLANX_ROUTER_LOG_NAME     = os.environ.get("MICTLANX_ROUTER_LOG_NAME","mictlanx-spm-0")
MICTLANX_ROUTER_LOG_INTERVAL = int(os.environ.get("MICTLANX_ROUTER_LOG_INTERVAL","24"))
MICTLANX_ROUTER_LOG_WHEN     = os.environ.get("MICTLANX_ROUTER_LOG_WHEN","h")
MICTLANX_ROUTER_LOG_SHOW     = bool(int(os.environ.get("MICTLANX_ROUTER_LOG_SHOW","1")))
LOG_PATH                     = os.environ.get("LOG_PATH","/log")
log                          = Log(
        name                   = MICTLANX_ROUTER_LOG_NAME,
        console_handler_filter = lambda x: MICTLANX_ROUTER_LOG_SHOW,
        interval               = MICTLANX_ROUTER_LOG_INTERVAL,
        when                   = MICTLANX_ROUTER_LOG_WHEN,
        path                   = LOG_PATH
)

def run_async_healer(
        ph:StoragePeerManager,
        # heartbeat:str="10s",
        max_idle_time:str ="10s",
        queue_tick_timeout:str="5s",
):
    async def __run_async_healer():
        # _heartbeat = HF.parse_timespan(heartbeat)
        last_time = T.time()
        _queue_tick_timeout = HF.parse_timespan(queue_tick_timeout)
        _max_idle_time = HF.parse_timespan(max_idle_time)
        while True:
            try:
                event = ph.q.get_nowait()
                result = await ph.run()
                if result.is_ok:
                    log.info({
                        "event":"ASYNC.PEER.MANAGER",
                        "total_disk":HF.format_size(ph.global_stats.total_disk),
                        "used_disk":HF.format_size(ph.global_stats.used_disk),
                        "available_disk":HF.format_size(ph.global_stats.available_disk),
                        "disk_uf":ph.global_stats.disk_uf,
                        "balls":len(ph.global_stats.balls),
                        "peers_ids":await ph.get_available_peers_ids(),
                        "unavailable_peers_ids":await ph.get_unavailable_peers_ids(),
                    })
                else:
                    log.error({
                        "event":"ASYNC.PEER.MANAGER.FAILED",
                        "health_nodes":len(await ph.get_available_peers_ids()),
                        "err":str(result.unwrap_err())
                    })

                    # ph.check_peers_availability()
                    
            except asyncio.QueueEmpty as e:
                elapsed_time = T.time() - last_time
                log.warn({
                    "event":"SPM.QUEUE.EMPTY",
                    "elapsed_time":HF.format_timespan(elapsed_time),
                    "max_idle_time": max_idle_time
                })
                if elapsed_time >= _max_idle_time:
                    last_time = T.time()
                    await ph.q.put(1)
                
                await asyncio.sleep(_queue_tick_timeout)

            except Exception as e:
                log.error({
                    "event":"ASYNC.ERROR",
                    "error":str(e)
                })
            # finally:
                # await asyncio.sleep(_heartbeat)
    return __run_async_healer()