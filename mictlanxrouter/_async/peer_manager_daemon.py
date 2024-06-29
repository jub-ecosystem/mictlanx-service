import humanfriendly as HF
import asyncio
import os
from mictlanxrouter.peer_manager import StoragePeerManager
from mictlanx.logger.log import Log

MICTLANX_ROUTER_LOG_NAME     = os.environ.get("MICTLANX_ROUTER_LOG_NAME","mictlanx-peer-manager-0")
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

def run_async_healer(ph:StoragePeerManager,heartbeat:str="10s"):
    async def __run_async_healer():
        _heartbeat = HF.parse_timespan(heartbeat)
        while True:
            try:
                # async with peer_healer_rwlock.writer_lock:
                result = await ph.run()
                if result.is_ok:
                    log.info({
                        "event":"ASYNC.PEER.MANAGER",
                        "total_disk":ph.global_stats.total_disk,
                        "used_disk":ph.global_stats.used_disk,
                        "available_disk":ph.global_stats.available_disk,
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
                    
            except Exception as e:
                log.error({
                    "event":"ASYNC.ERROR",
                    "error":str(e)
                })
            finally:
                await asyncio.sleep(_heartbeat)
    return __run_async_healer()