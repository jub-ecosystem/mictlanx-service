import os 
import asyncio
import time as T
from mictlanxrouter.replication import DataReplicator,ReplicationEvent
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
async def run_replicator(
        # q: asyncio.Queue,
        data_replicator:DataReplicator,
        max_idle_timeout:str = "10s",
        heartbeat:str = "2s"
)->Result[bool,Exception]:
    _heartbeat = HF.parse_timespan(heartbeat)
    _max_idle_timeout = HF.parse_timespan(max_idle_timeout)
    last_tick = T.time()
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
#         replicator_queue:asyncio.Queue
# ):
#     # global log
#     # global replicator
#     # global ph
#     # global replicas_map
#     # global replicator_queue
#     # global peer_healer_rwlock
#     print("AAAAAAAAAAAAAAAAAAAAHHHH")
#     timeout = HF.parse_timespan(MICTLANX_ROUTER_REPLICATOR_QUEUE_TIMEOUT)
#     counter = 0
#     while True:
#         try:
#             element:ReplicationEvent = replicator_queue.get_nowait()


#             if element.ttl >=  MICTLANX_ROUTER_MAX_TTL:
#                 log.error({
#                     "msg":"Replication max ttl reached",
#                     "bucket_id":element.bucket_id,
#                     "key":element.key,
#                     "from_peer_id":element.from_peer_id,
#                     "current_ttl":element.ttl,
#                     "max_ttl":MICTLANX_ROUTER_MAX_TTL
#                 })
#                 continue

#             # Check if the replication type if data
#             if element.rtype == "DATA":
#                 # 
#                 async with peer_healer_rwlock.reader_lock:
#                     result = await replicator.run(event=element)
#                     current_peers = ph.peers.copy()
#                     if result.left_replicas > 0 and len(current_peers) < element.rf :
#                         current_peers = ph.peers.copy()
#                         available_replica_peers =   len(current_peers) - len(result.replicas) 
#                         target_pool_size = (available_replica_peers+(result.left_replicas - available_replica_peers) if available_replica_peers < result.left_replicas else result.left_replicas) + len(current_peers)
#                         log.info({
#                             "event":"ELASTICITY",
#                             "bucket_id": element.bucket_id,
#                             "key": element.key,
#                             "current_pool_size":len(current_peers),
#                             "left_replicas": result.left_replicas,
#                             "available_peers":available_replica_peers,
#                             "target_pool_size":target_pool_size
#                         })
#                     else:
#                         target_pool_size = len(ph.peers)
                        
#                 async with replica_map_rwlock.writer_lock:
#                     curent_replicas:Set[str] = replicas_map.setdefault(result.combined_key_str,set([]))
#                     curent_replicas.union(set(result.replicas))
#                     replicas_map[result.combined_key_str] = curent_replicas
#                 element.ttl +=1
#                 await replicator_queue.put(ReplicationEvent(rtype="SYSTEM", rf=target_pool_size , ttl= element.ttl ))
#                 await asyncio.sleep(MICTLANX_ROUTER_MAX_AFTER_ELASTICITY)
#                 await replicator_queue.put(element)
#                 log.debug({
#                     "event":"DATA.REPLICATION.EVENT",
#                     "bucket_id":element.bucket_id,
#                     "key": element.key,
#                     "left_replicas":result.left_replicas
#                 })
#             else:
#                 start_time = T.time()
#                 async with peer_healer_rwlock.reader_lock:
#                     current_peers = ph.peers.copy()
#                     current_peers_len = len(current_peers)
#                     diff = element.rf - current_peers_len

#                 if current_peers_len>= MICTLANX_ROUTER_MAX_PEERS:
#                     log.error({
#                         "msg":"MAX_PEERS reached",
#                         "max_peers":MICTLANX_ROUTER_MAX_PEERS,
#                         "current_peers":current_peers_len
#                     })
#                     continue
#                 if current_peers_len >= element.rf:
#                     response_time = T.time()- start_time
#                     log.debug({
#                         "event":"POOL.CHECK",
#                         "pool_size": current_peers_len,
#                         "rf":element.rf
#                     })
#                     continue
        
#                 failed                      = 0
#                 tasks                       = []
#                 deployed_peer_info          = []
#                 for task_index in range(diff):
#                     selected_node           = int(MICTLANX_ROTUER_AVAILABLE_NODES[counter%len(MICTLANX_ROTUER_AVAILABLE_NODES)])
#                     peer_index              = task_index + current_peers_len
#                     current_container_id    = "mictlanx-peer-{}".format(peer_index)
#                     port                    = MICTLANX_ROUTER_PEER_BASE_PORT+peer_index
#                     deployed_peer_info.append((current_container_id,port))
#                     task                    = deploy_peer(
#                         container_id=current_container_id,
#                         port=port,
#                         disk=element.disk,
#                         memory= element.memory,
#                         selected_node=selected_node,
#                         workers= element.workers,
#                         elastic=element.elastic
#                     )
#                     tasks.append(task)
#                     counter+=1
                
                
#                 async with peer_healer_rwlock.reader_lock:
#                     deployed_peer_info += list(ph.get_tupled_peers())
#                 responses:List[Result[Tuple[str,int, SummonContainerPayload],Exception]] = await asyncio.gather(*tasks)
                
#                 for (current_container_id, current_container_port, summon_result) in responses:
#                     # _start_time = T.time()
#                     if summon_result.is_err:
#                         failed+=1
#                         log.error({
#                             "detail":str(summon_result.unwrap_err())
#                         })
#                     else:
#                         response = summon_result.unwrap()
#                         async with peer_healer_rwlock.writer_lock:
#                             peer = Peer(
#                                     peer_id=current_container_id,
#                                     ip_addr=current_container_id,
#                                     port=current_container_port,
#                                     protocol=element.protocol
#                             )
#                             ph.add_peer(peer=peer)
#                         for (other_peer_id, other_peer_port) in deployed_peer_info:
#                             if not current_container_id == other_peer_id:
#                                 op_start_time = T.time()
                                

#                                 added_peer_result = peer.add_peer_with_retry(
#                                     id=other_peer_id,
#                                     disk=element.disk,
#                                     memory= element.memory,
#                                     ip_addr= other_peer_id,
#                                     port=other_peer_port,
#                                     weight=1,
#                                     delay=MICTLANX_ROUTER_DELAY,
#                                     tries= MICTLANX_ROUTER_MAX_TRIES
#                                 )
                                
#                                 rt = T.time() - op_start_time
#                                 log.info({
#                                     "event":"ADDED.OTHER.PEER",
#                                     "peer_id":peer.peer_id,
#                                     "added_peer_id":other_peer_id,
#                                     "added_peer_port":other_peer_port, 
#                                     "response_time":rt
#                                 })
#                 response_time = T.time()- start_time
#                 log.info({
#                     "event":"SYSTEM.REPLICATION.EVENT",
#                     "rf":element.rf,
#                     "pool_size":len(current_peers)+(diff-failed),
#                     "diff":diff,
#                     "failed":failed,
#                     "response_time":response_time
#                 })

#         except asyncio.QueueEmpty  as e:
#             log.warn({
#                 "detail":"Replicator queue timeout. No replication event has been arrived.",
#                 "timeout":MICTLANX_ROUTER_REPLICATOR_QUEUE_TIMEOUT
#             })
#         except Exception as e:
#             log.error({
#                 "msg":str(e)
#             })
#         finally:
#             await asyncio.sleep(timeout)