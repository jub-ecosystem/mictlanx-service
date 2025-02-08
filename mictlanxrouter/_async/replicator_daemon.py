import os 
import asyncio
import time as T
from mictlanxrouter.replication_manager import DataReplicator,ReplicationEvent
from mictlanx.logger import Log
import humanfriendly as HF
from option import Result,Err,Ok

MICTLANX_ROUTER_REPLICATOR_QUEUE_MAXSIZE = int(os.environ.get("MICTLANX_ROUTER_REPLICATOR_QUEUE_MAXSIZE","100"))
# MICTLANX_ROUTER_REPLICATOR_QUEUE_TIMEOUT = os.environ.get("MICTLANX_ROUTER_REPLICATOR_QUEUE_TIMEOUT","30s")
MICTLANX_ROUTER_MAX_TTL                  = int(os.environ.get("MICTLANX_ROUTER_MAX_TTL","3"))
MICTLANX_ROUTER_LOG_NAME                 = os.environ.get("MICTLANX_ROUTER_LOG_NAME","mictlanx-data-replicator-0")
MICTLANX_ROUTER_LOG_INTERVAL             = int(os.environ.get("MICTLANX_ROUTER_LOG_INTERVAL","24"))
MICTLANX_ROUTER_LOG_WHEN                 = os.environ.get("MICTLANX_ROUTER_LOG_WHEN","h")
MICTLANX_ROUTER_LOG_SHOW                 = bool(int(os.environ.get("MICTLANX_ROUTER_LOG_SHOW","1")))

LOG_PATH                     = os.environ.get("LOG_PATH","/log")
log                          = Log(
        name                   = MICTLANX_ROUTER_LOG_NAME,
        console_handler_filter = lambda x: MICTLANX_ROUTER_LOG_SHOW,
        interval               = MICTLANX_ROUTER_LOG_INTERVAL,
        when                   = MICTLANX_ROUTER_LOG_WHEN,
        path                   = LOG_PATH
)
async def run_data_replicator(
        # q: asyncio.Queue,
        data_replicator:DataReplicator,
        max_idle_timeout:str = "10s",
        heartbeat:str = "2s"
)->Result[bool,Exception]:
    _heartbeat        = HF.parse_timespan(heartbeat)
    _max_idle_timeout = HF.parse_timespan(max_idle_timeout)
    last_tick         = T.time()
    try:
        while True:
            try:
                element:ReplicationEvent =  data_replicator.q.get_nowait()
                res = await data_replicator.run(event=element)
                if res.is_err:
                    log.error({
                        "event":"DATA.REPLICATION.FAILED",
                        "bucket_id":element.bucket_id,
                        "key":element.key,
                        "detail":str(res.unwrap_err()), 
                    })
            except asyncio.QueueEmpty  as e:
                log.warning({
                    "detail":"Data replicator queue timeout. No replication event has been arrived.",
                    "heartbeat":heartbeat
                })
            except Exception as e:
                log.error({
                    "event":"UKNOWN.EXCEPTION",
                    "msg":str(e)
                })
                continue
            finally:
                elapsed = T.time() - last_tick
                if elapsed >= _max_idle_timeout:
                    await data_replicator.drain_tasks()
                    log.debug({
                        "event":"CHECK.PENDING.DATA.REPLICATION.TASKS",
                        "elapsed":HF.format_timespan(elapsed),
                        "max_idle_timeout":max_idle_timeout
                    })
                    last_tick = T.time()
                await asyncio.sleep(_heartbeat)
    except Exception as e:
        return Err(e)