from mictlanx.logger.log import Log
import asyncio
import humanfriendly as HF
import time as T
from typing import List,Dict
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
    batch_size:int = 1000
):
    last_time = T.time()
    _queue_max_idle_time = HF.parse_timespan(queue_max_idle_time)
    _queue_tick_timeout  = HF.parse_timespan(queue_tick_timeout)
    iteration = 0
    latest_n_balls = -1
    map_n_balls:Dict[str,int] = {}
    peers              = await rm.spm.get_available_peers()
    local_n_balls_map = dict([ (p.peer_id,p.get_balls_len().unwrap_or(0)) for p in peers])
    while True:
        elapsed = T.time() - last_time
        try:
            max_n_balls = ((iteration +1)*batch_size)
            peers              = await rm.spm.get_available_peers()
            event              = rm.q.get_nowait()
            current_total_balls_map:Dict[str,int]      = dict([ (p.peer_id,p.get_balls_len().unwrap_or(0)) for p in peers])
            filtered_peers_ids:List[str] = []
            for p in peers:
                current_n_balls = current_total_balls_map.setdefault(p.peer_id, 0)
                local_n_balls   = local_n_balls_map.setdefault(p.peer_id,0)
                if current_n_balls !=  local_n_balls or max_n_balls < current_n_balls:
                    local_n_balls_map[p.peer_id]=  max_n_balls
                    filtered_peers_ids.append(p.peer_id)
                
            
            filtered_peers     = filter(lambda x: x.peer_id in filtered_peers_ids, peers)
            log.debug({
                "event":"RUN.RM",
                "peers_n_balls":current_total_balls_map,
                "filtered_peers":filtered_peers_ids
                
            })
            tasks   = [x async for x in get_stats_from_peers(peers=filtered_peers, batch_size=batch_size, iteration=iteration)]
            stats   = map(lambda x: x.unwrap(),filter(lambda x: x.is_ok,await asyncio.gather(*tasks)))
            xs      = {}
            n_balls = 0
            
            for stat in stats:
                current_n_balls = len(stat.balls)
                n_balls += current_n_balls
                if not stat.peer_id in map_n_balls:
                    map_n_balls[stat.peer_id] = 0

                if current_n_balls == map_n_balls.get(stat.peer_id,0) and event != 0:
                    log.debug({
                        "peer_id":stat.peer_id,
                        "event":"REPLICATION.SKIPPED",
                        "n_balls":current_n_balls
                    })
                    continue
                else:
                    map_n_balls[stat.peer_id] = current_n_balls

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