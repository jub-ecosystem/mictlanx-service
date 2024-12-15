from mictlanx.logger.log import Log
import asyncio
import math
import humanfriendly as HF
import json as J
import time as T
from typing import List,Dict
from option import Option, NONE, Some
from mictlanxrouter.peer_manager import StoragePeerManager
from mictlanxrouter.replication import ReplicaManager,Pagination
import mictlanx.v4.interfaces as InterfaceX
import os 
from logging import LogRecord


MICTLANX_RM_LOG_NAME     = os.environ.get("MICTLANX_RM_LOG_NAME","mictlanx-rm-0")
MICTLANX_RM_LOG_INTERVAL = int(os.environ.get("MICTLANX_RM_LOG_INTERVAL","24"))
MICTLANX_RM_LOG_WHEN     = os.environ.get("MICTLANX_RM_LOG_WHEN","h")
MICTLANX_RM_LOG_SHOW     = bool(int(os.environ.get("MICTLANX_RM_LOG_SHOW","1")))
MICTLANX_RM_LOG_SHOW_WARM     = bool(int(os.environ.get("MICTLANX_RM_LOG_SHOW_WARM","0")))
LOG_PATH                     = os.environ.get("LOG_PATH","/log")

def log_filter(x:LogRecord):
    if x.levelname == "WARNING" and MICTLANX_RM_LOG_SHOW_WARM:
        return True
    elif x.levelname == "WARNING" and not MICTLANX_RM_LOG_SHOW_WARM:
        return False
    return MICTLANX_RM_LOG_SHOW
    # elif MICTLANX_RM_LOG_NAME :
    #     return True
    # else:
    #     return False

log                          = Log(
        name                   = MICTLANX_RM_LOG_NAME,
        console_handler_filter = log_filter,
        interval               = MICTLANX_RM_LOG_INTERVAL,
        when                   = MICTLANX_RM_LOG_WHEN,
        path                   = LOG_PATH
)




async def __get_stats(peer:InterfaceX.Peer, start:int =0, end:int  = 100):
    return peer.get_stats(start=  start, end= end)


async def get_stats_from_peers(peers:List[InterfaceX.Peer],pagination:Dict[str,Pagination]={}):
    for i,peer in enumerate(peers):
        # start,end,completed = 0 ,100,False
        if peer.peer_id in pagination:
            start,end, completed = pagination.get(peer.peer_id).soft_next()
            yield asyncio.create_task(__get_stats(peer=peer,  start=start,end=end ))




    # def check(self,rmp:'ReplicaManagerParams')->Option['ReplicaManagerParams']:
    #     _queue_max_idle_timeout_predicate = self.queue_max_idle_timeout != rmp.queue_max_idle_timeout 
    #     queue_max_idle_timeout = rmp.queue_max_idle_timeout if _queue_max_idle_timeout_predicate else self.queue_max_idle_timeout

        

async def run_rm(
    rm:ReplicaManager,
):
    try:
        params               = await rm.get_params()
        last_time            = T.time()
        _queue_max_idle_time = HF.parse_timespan(params.queue_max_idle_timeout)
        _queue_tick_timeout  = HF.parse_timespan(params.heartbeat_timeout)
        peers                = await rm.spm.get_available_peers()
        peers_ids            = list(map(lambda x :x.peer_id, peers))
        local_n_balls_map    = dict([ (p.peer_id,p.get_balls_len().unwrap_or(0)) for p in peers])
        paginations:Dict[str,Pagination] = dict([(peer_id,Pagination(n = n, batch_size=params.batch_size)) for peer_id,n in local_n_balls_map.items()])
        await rm.update_params(paginations = paginations)
        
        log.debug({
            "event":"REPLICA.MANAGEMENT.STARTED",
            "peers":len(peers),
        })
        print("*"*100)
    except Exception as e:
        log.error({
            "event":"RUN.RM.FAILED",
            "error":str(e)
        })

    
    while True:
        params                 = await rm.get_params()
        # paginations = params.paginations
        # for k,v in paginations.items():
        #     if v.is_completed():
        #         v.reset()
        # await rm.update_params(paginations=paginations)
        elapsed = T.time() - last_time
        try:
            event               = rm.q.get_nowait()
            # current_start_index =  params.batch_index*params.batch_size 
            # current_end_index   = current_start_index + params.batch_size 
            peers               = await rm.spm.get_available_peers()
            current_total_balls_map:Dict[str,int]      = dict([ (p.peer_id,p.get_balls_len().unwrap_or(0)) for p in peers])

            total_n_balls = sum(list(current_total_balls_map.values()))
            
            filtered_peers_ids:List[str] = []
            for p in peers:
                current_n_balls = current_total_balls_map.setdefault(p.peer_id, 0)
                local_n_balls   = local_n_balls_map.setdefault(p.peer_id,0)

                if current_n_balls !=  local_n_balls :
                    paginations[p.peer_id] = Pagination(n= current_n_balls , batch_size= params.batch_size)
                    await rm.update_params(paginations = paginations)
                    local_n_balls_map[p.peer_id]=  current_n_balls
                    filtered_peers_ids.append(p.peer_id)
                
            
            remaining_balls_peers_ids = list(
                dict(
                    filter(lambda x: not x[1].soft_next()[2], paginations.items())
                ).keys()
            )
            filtered_peers_ids    = list(set(filtered_peers_ids + remaining_balls_peers_ids))
            filtered_peers        = filter(lambda x: x.peer_id in filtered_peers_ids, peers)
                                            #  + remaining_balls_peers))
            log.debug({
                "event":"REPLICA.MANAGER.STATS",
                "last_time":last_time,
                "elapsed":HF.format_timespan(elapsed),
                "batch_index":params.batch_index,
                "batch_size":params.batch_size,
                "peers":peers_ids,
                "filtered_peers":filtered_peers_ids,
                "local_n_balls_map":local_n_balls_map,
                "current_total_n_balls":current_total_balls_map,
                "total_n_balls":total_n_balls,
                "queue_max_idle_time":params.queue_max_idle_timeout,
                "heartbeat_timeout":params.heartbeat_timeout,
                "paginations":dict([ (k,v.to_dict()) for k,v in paginations.items()])
            })
            tasks   = [x async for x in get_stats_from_peers(
                peers=filtered_peers, 
                pagination=paginations
            )]
            stats   = map(lambda x: x.unwrap(),filter(lambda x: x.is_ok,await asyncio.gather(*tasks)))
            current_replicas_map      = {}
            
            for stat in stats:
                ys = dict([ ("{}.{}.{}".format(stat.peer_id,b.bucket_id,b.key),0) for b in stat.balls])
                await rm.extend_access_map(access_map=ys)
                for b in stat.balls:
                    combined_key = "{}@{}".format(b.bucket_id, b.key)
                    if not combined_key in current_replicas_map:
                        current_replicas_map[combined_key] = []
                    current_replicas_map[combined_key].append(stat.peer_id)
                log.debug({
                    "event":"RM.PEER.STATS",
                    "peer_id":stat.peer_id,
                    "balls":len(stat.balls),
                    "total_disk":HF.format_size(stat.total_disk),
                    "used_disk":HF.format_size(stat.used_disk),
                    "available_disk":HF.format_size(stat.available_disk),
                    "disk_uf":stat.disk_uf,
                    "iteration":params.batch_index
                })
      

            # ________________________________
            
            for combined_key, replicas in current_replicas_map.items():
                bucket_id, key = combined_key.split("@")
                await rm.create_replicas(
                    bucket_id= bucket_id,
                    key = key,
                    peer_ids=replicas,
                    rf=len(replicas)
                )
            

            for k,v in paginations.items():
                if v.is_completed():
                    log.debug({
                        "event":"PAGINATION.RESET",
                        "peer_id":k,
                    })
                    v.reset()
                else:
                    v.next()
                    log.debug({
                        "event":"PAGINATION.NEXT",
                        "peeR_id":k
                    })
                
            await rm.update_params(
                batch_index = params.batch_index + 1, 
                paginations = paginations,
                # current_start_index = current_start_index,
                # current_end_index = current_end_index,
                total_n_balls = total_n_balls,
                local_n_balls_map  = local_n_balls_map,
            )
            # batch_index += 1
            last_time = T.time()
        except asyncio.QueueEmpty as e:
            _queue_max_idle_time   = HF.parse_timespan(params.queue_max_idle_timeout)
            _queue_tick_timeout    = HF.parse_timespan(params.heartbeat_timeout)
            log.warning({
                "event":"RM.QUEUE.EMPTY",
                "elapsed":HF.format_timespan(elapsed),
                "tick_timeout":params.heartbeat_timeout,
                "max_idle_time":params.queue_max_idle_timeout
            
            })
            if elapsed >= _queue_max_idle_time:
                await rm.q.put(1)
            await asyncio.sleep(_queue_tick_timeout)
        except Exception as e:
            log.error(str(e))
        # finally: