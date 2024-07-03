from mictlanx.logger.log import Log
import asyncio
import humanfriendly as HF
import time as T
from typing import List
from mictlanxrouter.peer_manager import StoragePeerManager
from mictlanxrouter.replication import ReplicaManager
import mictlanx.v4.interfaces as InterfaceX
import os 


MICTLANX_ROUTER_LOG_NAME     = os.environ.get("MICTLANX_ROUTER_LOG_NAME","mictlanx-rm-0")
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




async def __get_stats(peer:InterfaceX.Peer, iteration:int =0, batch_size:int  = 100):
    return peer.get_stats(start=  iteration*batch_size, end= (iteration+1)*batch_size)
async def get_stats_from_peers(peers:List[InterfaceX.Peer],batch_size:int =100,iteration:int =0):
    for peer in peers:
        yield asyncio.create_task(__get_stats(peer=peer, iteration=iteration, batch_size=batch_size))

async def run_rm(
    rm:ReplicaManager,
    queue_max_idle_time:str = "30s",
    queue_tick_timeout:str = "5s",
    batch_size:int = 100
):
    last_time = T.time()
    _queue_max_idle_time = HF.parse_timespan(queue_max_idle_time)
    _queue_tick_timeout  = HF.parse_timespan(queue_tick_timeout)
    iteration = 0
    latest_n_balls = -1
    while True:
        try:
            event = rm.q.get_nowait()
            peers = await rm.spm.get_available_peers()
            tasks = [x async for x in get_stats_from_peers(peers=peers, batch_size=batch_size, iteration=iteration)]
            stats = map(lambda x: x.unwrap(),filter(lambda x: x.is_ok,await asyncio.gather(*tasks)))
            xs = {}
            n_balls = 0
            for stat in stats:
                n_balls += len(stat.balls)
                ys = dict([ ("{}.{}.{}".format(stat.peer_id,b.bucket_id,b.key),0) for b in stat.balls])
                await rm.extend_access_map(access_map=ys)
                for b in stat.balls:
                    combined_key = "{}@{}".format(b.bucket_id, b.key)
                    if not combined_key in xs:
                        xs[combined_key] = []
                    xs[combined_key].append(stat.peer_id)
                log.debug({
                    "event":"RM.PEER.STATS",
                    "peer_id":stat.peer_id,
                    "balls":len(stat.balls),
                    "total_disk":HF.format_size(stat.total_disk),
                    "used_disk":HF.format_size(stat.used_disk),
                    "available_disk":HF.format_size(stat.available_disk),
                    "disk_uf":stat.disk_uf,
                    "iteration":iteration
                })
            if latest_n_balls == n_balls:
                log.debug({
                    "event":"REPLICATION.SKIPPED",
                    "n_balls":n_balls
                })
                continue
            latest_n_balls = n_balls

            # ________________________________
            
            for combined_key, replicas in xs.items():
                bucket_id, key = combined_key.split("@")
                await rm.create_replicas(
                    bucket_id= bucket_id,
                    key = key,
                    peer_ids=replicas,
                    rf=len(replicas)
                )
            iteration += 1

        except asyncio.QueueEmpty as e:
            elapsed = T.time() - last_time
            log.warn({
                "event":"RM.QUEUE.EMPTY",
                "elapsed":HF.format_timespan(elapsed)
            })
            if elapsed >= _queue_max_idle_time:
                last_time = T.time()
                await rm.q.put(1)
            await asyncio.sleep(_queue_tick_timeout)
        except Exception as e:
            log.error(str(e))