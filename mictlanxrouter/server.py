import os
import sys
import requests as R
# import uvicorn
import time as T
import humanfriendly as HF
import random as RND
import aiorwlock
import signal
from asyncio.queues import Queue
import asyncio
from fastapi import FastAPI,Response,HTTPException,Header,UploadFile,Request
from contextlib import asynccontextmanager
from fastapi.responses import StreamingResponse
from fastapi.openapi.utils import get_openapi
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from typing import List,Dict,Tuple,Union,Set,Iterator
from typing_extensions import Annotated
from option import Result,NONE,Some,Ok,Err
from fastapi.middleware.gzip import GZipMiddleware
from ipaddress import IPv4Network
# 
from mictlanxrouter.replication import LoadBalancer
from mictlanxrouter.interfaces.dto.metadata import Metadata
from mictlanxrouter.interfaces.dto.index import Peer as PeerPayload,DeletedByBallIdResponse,DeletedByKeyResponse
from mictlanxrouter.peer_manager.healer import StoragePeerManager
from mictlanxrouter.replication import Replicator,ReplicationEvent
from mictlanxrouter.interfaces.dto.index import PeerElasticPayload
# 
from mictlanx.v4.interfaces.responses import GetMetadataResponse,PutMetadataResponse
from mictlanx.v4.interfaces.index import PeerStats
from mictlanx.logger.log import Log
from mictlanx.v4.interfaces.index import Peer
from mictlanx.interfaces.responses import SummonContainerResponse
from mictlanx.logger.tezcanalyticx.tezcanalyticx import TezcanalyticXHttpHandler
from mictlanx.interfaces.payloads import SummonContainerPayload,ExposedPort
from mictlanx.v4.summoner.summoner import  Summoner
from mictlanx.interfaces.payloads import MountX
from mictlanx.utils.index import Utils
from mictlanxrouter._async import run_async_healer

TEZCANALYTICX_ENABLED       = bool(int(os.environ.get("TEZCANALYTICX_ENABLE","0")))
TEZCANALYTICX_FLUSH_TIMEOUT = os.environ.get("TEZCANALYTICX_FLUSH_TIMEOUT","10s")
TEZCANALYTICX_BUFFER_SIZE   = int(os.environ.get("TEZCANALYTICX_BUFFER_SIZE","100"))
TEZCANALYTICX_PATH          = os.environ.get("TEZCANALYTICX_PATH","/api/v4/events/")
TEZCANALYTICX_PORT          = int(os.environ.get("TEZCANALYTICX_PORT","45000"))
TEZCANALYTICX_HOSTNAME      = os.environ.get("TEZCANALYTICX_HOSTNAME","localhost")
TEZCANALYTICX_LEVEL         = int(os.environ.get("TEZCANALYTICX_LEVEL","0"))
TEZCANALYTICX_PROTOCOL      = os.environ.get("TEZCANALYTICX_PROTOCOL","http")
LOG_PATH                    = os.environ.get("LOG_PATH","/log")
# 
MICTLANX_ROUTER_HOST                     = os.environ.get("MICTLANX_ROUTER_HOST","localhost")
MICTLANX_ROUTER_PORT                     = int(os.environ.get("MICTLANX_ROUTER_PORT","60666"))
MICTLANX_ROUTER_RELOAD                   = bool(int(os.environ.get("MICTLANX_ROUTER_RELOAD","1")))
MICTLANX_ROUTER_MAX_WORKERS              = int(os.environ.get("MAX_WORKERS","4"))
MICTLANX_ROUTER_LB_ALGORITHM             = os.environ.get("MICTLANX_ROUTER_LB_ALGORITHM","ROUND_ROBIN") # ROUND_ROBIN
MICTLANX_ROUTER_LOG_NAME                 = os.environ.get("MICTLANX_ROUTER_LOG_NAME","mictlanx-router-0")
MICTLANX_ROUTER_LOG_INTERVAL             = int(os.environ.get("MICTLANX_ROUTER_LOG_INTERVAL","24"))
MICTLANX_ROUTER_LOG_WHEN                 = os.environ.get("MICTLANX_ROUTER_LOG_WHEN","h")
MICTLANX_ROUTER_LOG_SHOW                 = bool(int(os.environ.get("MICTLANX_ROUTER_LOG_SHOW","1")))
MICTLANX_ROUTER_REPLICATOR_QUEUE_MAXSIZE = int(os.environ.get("MICTLANX_ROUTER_REPLICATOR_QUEUE_MAXSIZE","100"))
MICTLANX_ROUTER_PEER_HEALER_HEARTBEAT    = os.environ.get("MICTLANX_ROUTER_PEER_HEALER_HEARTBEAT","5s")
MICTLANX_ROUTER_REPLICATOR_QUEUE_TIMEOUT = os.environ.get("MICTLANX_ROUTER_REPLICATOR_QUEUE_TIMEOUT","30s")
MICTLANX_ROUTER_SYNC_FAST                = bool(int(os.environ.get("MICTLANX_ROUTER_SYNC_FAST","1")))
MICTLANX_ROUTER_PEER_BASE_PORT           = int(os.environ.get("MICTLANX_PEER_BASE_PORT","25000"))
MICTLANX_ROUTER_MAX_PEERS                = int(os.environ.get("MICTLANX_ROUTERMAX_PEERS","10"))
MICTLANX_ROUTER_MAX_TTL                  = int(os.environ.get("MICTLANX_ROUTER_MAX_TTL","3"))
MICTLANX_ROTUER_AVAILABLE_NODES          = os.environ.get("MICTLANX_ROUTER_AVAILABLE_NODES","0;1;2;3;4;5;6;7;8;9;10").split(";")
MICTLANX_ROUTER_MAX_AFTER_ELASTICITY     = int(os.environ.get("MICTLANX_ROUTER_MAX_AFTER_ELASTICITY","10"))
MICTLANX_ROUTER_SHOW_PEER_HEALER_LOGS    = bool(int(os.environ.get("MICTLANX_ROUTER_SHOW_PEER_HEALER_LOGS","1")))
MICTLANX_ROUTER_SHOW_REPLICATOR_LOGS     = bool(int(os.environ.get("MICTLANX_ROUTER_SHOW_REPLICATOR_LOGS","1")))
MICTLANX_ROUTER_DELAY                    = HF.parse_timespan(os.environ.get("MICTLANX_ROUTER_DELAY","2s"))
MICTLANX_ROUTER_MAX_TRIES                = int(os.environ.get("MICTLANX_ROUTER_MAX_TRIES","60"))
MICTLANX_ROUTER_MAX_TASKS                = int(os.environ.get("MICTLANX_ROUTER_MAX_TASKS","100"))
MICTLANX_ROUTER_MAX_CONCURRENCY          = int(os.environ.get("MICTLANX_ROUTER_MAX_CONCURRENCY","5"))

MICTLANX_PEERS_STR                = os.environ.get("MICTLANX_PEERS","mictlanx-peer-0:localhost:25000 mictlanx-peer-1:localhost:25001 mictlanx-peer-2:localhost:25002")
MICTLANX_PROTOCOL                 = os.environ.get("MICTLANX_PROCOTOL","http")
MICTLANX_PEERS                    = [] if MICTLANX_PEERS_STR == "" else list(Utils.peers_from_str_v2(peers_str=MICTLANX_PEERS_STR,separator=" ", protocol=MICTLANX_PROTOCOL,) )
MICTLANX_API_VERSION              = os.environ.get("MICTLANX_API_VERSION","4")
MICTLANX_TIMEOUT                  = int(os.environ.get("MICTLANX_TIMEOUT","3600"))

MICTLANX_SUMMONER_API_VERSION = Some(int(os.environ.get("MICTLANX_SUMMONER_API_VERSION","3")))
MICTLANX_SUMMONER_IP_ADDR     = os.environ.get("MICTLANX_SUMMONER_IP_ADDR","localhost")
MICTLANX_SUMMONER_PORT        = int(os.environ.get("MICTLANX_SUMMONER_PORT","15000"))
MICTLANX_SUMMONER_MODE        = os.environ.get("MICTLANX_SUMMONER_MODE","docker")
MICTLANX_SUMMONER_SUBNET      = os.environ.get("MICTLANX_SUMMONER_SUBNET","10.0.0.0/25")
# _____________________________________________________
def signal_handler(sig, frame):
    global log
    log.warn('Shutting down gracefully...')
    # for task in asyncio.all_tasks(loop):
        # task.cancel()
    # loop.stop()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
log                         = Log(
        name                   = MICTLANX_ROUTER_LOG_NAME,
        console_handler_filter = lambda x: MICTLANX_ROUTER_LOG_SHOW,
        interval               = MICTLANX_ROUTER_LOG_INTERVAL,
        when                   = MICTLANX_ROUTER_LOG_WHEN,
        path                   = LOG_PATH
)
if TEZCANALYTICX_ENABLED :
    tezcanalytix_handler =  TezcanalyticXHttpHandler(
        flush_timeout = TEZCANALYTICX_FLUSH_TIMEOUT,
        buffer_size   = TEZCANALYTICX_BUFFER_SIZE,
        path          = TEZCANALYTICX_PATH,
        port          = TEZCANALYTICX_PORT,
        hostname      = TEZCANALYTICX_HOSTNAME,
        level         = TEZCANALYTICX_LEVEL,
        protocol      = TEZCANALYTICX_PROTOCOL
        # os.environ.get("TEZCANALYTICX_PROTOCOL","http")
    )
    log.addHandler(tezcanalytix_handler)

# _________________________________________



replicator_queue = Queue(maxsize= MICTLANX_ROUTER_REPLICATOR_QUEUE_MAXSIZE )
replicas_map:Dict[str, Set[str]] = {}
# task_manager = DefaultTaskManager(max_tasks= MICTLANX_ROUTER_MAX_TASKS, max_concurrency=MICTLANX_ROUTER_MAX_CONCURRENCY)
# lock = Lock()
replica_map_rwlock = aiorwlock.RWLock(fast=MICTLANX_ROUTER_SYNC_FAST )
peer_healer_rwlock = aiorwlock.RWLock(fast=MICTLANX_ROUTER_SYNC_FAST )


# 

 
log.debug({
    "event":"MICTLANX_ROUTER_STARTED",
    "MICTLANX_ROUTER_HOST":MICTLANX_ROUTER_HOST,
    "MICTLANX_ROUTER_PORT":MICTLANX_ROUTER_PORT,
    "MICTLANX_ROUTER_RELOAD":MICTLANX_ROUTER_RELOAD,
    "TEZCANALYTICX_ENABLED": TEZCANALYTICX_ENABLED,
    "TEZCANALYTICX_FLUSH_TIMEOUT": TEZCANALYTICX_FLUSH_TIMEOUT,
    "TEZCANALYTICX_BUFFER_SIZE": TEZCANALYTICX_BUFFER_SIZE,
    "TEZCANALYTICX_PATH": TEZCANALYTICX_PATH,
    "TEZCANALYTICX_PORT": TEZCANALYTICX_PORT,
    "TEZCANALYTICX_HOSTNAME": TEZCANALYTICX_HOSTNAME,
    "TEZCANALYTICX_LEVEL": TEZCANALYTICX_LEVEL,
    "TEZCANALYTICX_PROTOCOL": TEZCANALYTICX_PROTOCOL,
    "LOG_PATH": LOG_PATH,
    "MICTLANX_ROUTER_LOG_NAME": MICTLANX_ROUTER_LOG_NAME,
    "MICTLANX_ROUTER_LOG_INTERVAL": MICTLANX_ROUTER_LOG_INTERVAL,
    "MICTLANX_ROUTER_LOG_WHEN": MICTLANX_ROUTER_LOG_WHEN,
    "MICTLANX_ROUTER_LOG_SHOW": MICTLANX_ROUTER_LOG_SHOW,
    "MICTLANX_ROUTER_REPLICATOR_QUEUE_MAXSIZE": MICTLANX_ROUTER_REPLICATOR_QUEUE_MAXSIZE,
    "MICTLANX_ROUTER_PEER_HEALER_HEARTBEAT": MICTLANX_ROUTER_PEER_HEALER_HEARTBEAT,
    "MICTLANX_ROUTER_REPLICATOR_QUEUE_TIMEOUT": MICTLANX_ROUTER_REPLICATOR_QUEUE_TIMEOUT,
    "MICTLANX_ROUTER_SYNC_FAST": MICTLANX_ROUTER_SYNC_FAST,
    "MICTLANX_ROUTER_LB_ALGORITHM": MICTLANX_ROUTER_LB_ALGORITHM,
    "MICTLANX_ROUTER_PEER_BASE_PORT": MICTLANX_ROUTER_PEER_BASE_PORT,
    "MICTLANX_ROUTER_MAX_PEERS":MICTLANX_ROUTER_MAX_PEERS,
    "MICTLANX_ROUTER_MAX_TTL": MICTLANX_ROUTER_MAX_TTL,
    "MICTLANX_ROUTER_AVAILABLE_NODES": ";".join(MICTLANX_ROTUER_AVAILABLE_NODES),
    "MICTLANX_ROUTER_MAX_AFTER_ELASTICITY": MICTLANX_ROUTER_MAX_AFTER_ELASTICITY,
    "MICTLANX_ROUTER_SHOW_PEER_HEALER_LOGS": MICTLANX_ROUTER_SHOW_PEER_HEALER_LOGS,
    "MICTLANX_ROUTER_SHOW_REPLICATOR_LOGS": MICTLANX_ROUTER_SHOW_REPLICATOR_LOGS,
    "MICTLANX_ROUTER_DELAY": MICTLANX_ROUTER_DELAY,
    "MICTLANX_ROUTER_MAX_TRIES": MICTLANX_ROUTER_MAX_TRIES,
    "MICTLANX_PEERS": MICTLANX_PEERS_STR,
    "MICTLANX_PROTOCOL": MICTLANX_PROTOCOL,
    "MICTLANX_API_VERSION": MICTLANX_API_VERSION,
    "MICTLANX_TIMEOUT": MICTLANX_TIMEOUT,
})


print("BEGORE_SUMKMONER_INSTANCe")

summoner        = Summoner(
    ip_addr     = MICTLANX_SUMMONER_IP_ADDR, 
    port        = MICTLANX_SUMMONER_PORT, 
    api_version = MICTLANX_SUMMONER_API_VERSION,
    network= Some(IPv4Network(
        MICTLANX_SUMMONER_SUBNET
        # os.environ.get("MICTLANX_SUMMONER_SUBNET","10.0.0.0/25")
     ))
)
print("BEGORE SPM")
ph = StoragePeerManager(
    q = Queue(maxsize=100),
    peers=MICTLANX_PEERS,
    name="mictlanx-peer-healer-0",
    show_logs=MICTLANX_ROUTER_SHOW_PEER_HEALER_LOGS
)
# replicator = Replicator(queue= replicator_queue,ph=ph, show_logs=MICTLANX_ROUTER_SHOW_REPLICATOR_LOGS)
# gc = MictlanXFMGarbageCollector(ph=ph, api_version=MICTLANX_API_VERSION)
# .openapi()

       
# class LoadBalancer(object):
#     def __init__(self, algorithm,peer_healer:StoragePeerManager):
#         self.algorithm = algorithm
#         self.peer_healer = peer_healer
#         self.counter = {
#             "get":0,
#             "put":0
#         }
#         self.counter_per_node:Dict[str,int] = {}
    
    
#     def lb_round_robin(self,operation_type:str="get"):
#         try:
#             total_requests = self.counter.get(operation_type,0)
#             n = len(self.peer_healer.peers)
#             current_peer_index =total_requests%n
#             return self.peer_healer.peers[current_peer_index]
#         except Exception as e:
#             raise e
#         finally:
#             self.counter[operation_type] = self.counter.setdefault(operation_type,0) +1 

#     def lb_sort_uf(self,size:int,key:str="",operation_type:str="get"):
#         try:
#             stats = ph.get_stats()
#             min_uf = -1
#             min_uf_peer = None
#             min_stats = None
#             n = len(list(stats.keys()))
#             if n == 0 :
#                 return None
            
#             for peer_id,stats in stats.items():
#                 uf = Utils.calculate_disk_uf(total=stats.total_disk,used=stats.used_disk , size=size)
#                 if min_uf == -1:
#                     min_uf = uf
#                     min_uf_peer = peer_id
#                     min_stats = stats
#                 else:
#                     if uf < min_uf:
#                         min_uf = uf
#                         min_uf_peer = peer_id
#                         min_stats = stats
#             x = next(filter(lambda p: p.peer_id==min_uf_peer,self.peer_healer.peers),None)
#             min_stats.put(key= key,size=size)
#             return  x
#         except Exception as e:
#             return None
    
#     def lb_2c_uf(self,size:int,key:str="", operation_type:str="put"):
#         stats = ph.get_stats()
#         keys = list(stats.keys())
#         if len(keys) == 0 :
#             return None
        
#         if len(keys) == 1: 
#             x:PeerStats = stats[keys[0]]
#             x.put(key= "",size=size)
#             selected = next(filter(lambda y: x.get_id() == y.peer_id, self.peer_healer.peers), None)
#             return selected
#         else:
#             xs = RND.sample(keys,2)
#             ys = list(map(lambda x: stats[x], xs))
#             y1 = ys[0].calculate_disk_uf(size=size)
#             y2 = ys[1].calculate_disk_uf(size=size)
#             if y1 < y2:
#                 ys[0].put(key= key,size=size)
#                 selected = next(filter(lambda y: ys[0].get_id() == y.peer_id, self.peer_healer.peers), None)
#             else:
#                 ys[1].put(key= key,size=size)
#                 selected = next(filter(lambda y: ys[1].get_id() == y.peer_id, self.peer_healer.peers), None)
#             return selected
        
        

    
#     # def lb_put(self, key:str ="", size:int =0 ): 
#     #     try:
#     #         pass
#     #     except Exception as e:
#     #         return NONE
#     def lb(self,operation_type:str="get",algorithm:str ="",key:str ="",size:int=0):
#         _algorithm = self.algorithm if algorithm == "" else algorithm
#         if _algorithm == "SORTING_UF":
#             return self.lb_sort_uf(size=size, operation_type=operation_type,key=key)
#         elif _algorithm =="ROUND_ROBIN":
#             return self.lb_round_robin(operation_type=operation_type)
#         elif _algorithm == "2C_UF":
#             return self.lb_2c_uf(operation_type=operation_type,size=size,key=key)
#         else:
#             return self.lb_sort_uf(size=size, operation_type=operation_type,key=key)


lb = LoadBalancer(algorithm=MICTLANX_ROUTER_LB_ALGORITHM, peer_healer=ph)

# ______________________________

@asynccontextmanager
async def lifespan(app:FastAPI):
    # t0 = asyncio.create_task(run_rm())
    t1 = asyncio.create_task(
        run_async_healer(
            ph=ph,
            heartbeat= MICTLANX_ROUTER_PEER_HEALER_HEARTBEAT
        )
    )
    # t2 = asyncio.create_task(run_replicator())
    yield
    # t0.cancel()
    t1.cancel()
    # t2.cancel()

app = FastAPI(
    root_path=os.environ.get("OPENAPI_PREFIX","/mictlanx-router-0"),
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)
app.add_middleware(GZipMiddleware, minimum_size=1000)

def generate_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="MictlanX Router",
        version="0.0.1",
        summary="MictlanX Router: Virtual storage spaces management.",
        description="",
        routes=app.routes,
    )
    openapi_schema["info"]["x-logo"] = {
        "url": os.environ.get("OPENAPI_LOGO","")
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema
app.openapi = generate_openapi




async def deploy_peer(container_id:str,port:int,disk:int,memory:int,workers:int, selected_node:int=0,elastic:str="false")->Tuple[str, int,Result[SummonContainerResponse,Exception]]:
    # selected_node = 0
    # container_id    = "mictlanx-peer-{}".format(i)
    
    _start_time = T.time()
    payload         = SummonContainerPayload(
        container_id=container_id,
        image=os.environ.get("MICTLANX_PEER_IMAGE","nachocode/mictlanx:peer"),
        hostname    = container_id,
        exposed_ports=[ExposedPort(NONE,port,port,NONE)],
        envs= {
            "USER_ID":"6666",
            "GROUP_ID":"6666",
            "BIN_NAME":"peer",
            "NODE_ID":container_id,
            "NODE_PORT":str(port),
            "IP_ADDRESS":container_id,
            "SERVER_IP_ADDR":"0.0.0.0",
            "NODE_DISK_CAPACITY":str(disk),
            "NODE_MEMORY_CAPACITY":str(memory),
            "BASE_PATH":"/app/mictlanx",
            "LOCAL_PATH":"/app/mictlanx/local",
            "DATA_PATH":"/app/mictlanx/data",
            "LOG_PATH":"/app/mictlanx/log",
            "MIN_INTERVAL_TIME":"15",
            "MAX_INTERVAL_TIME":"60",
            "WORKERS":str(workers),
            "ELASTIC":elastic
        },
        memory=1000000000,
        cpu_count=1,
        mounts=[
            MountX(source="{}-data".format(container_id), target="/app/mictlanx/data", mount_type=1),
            MountX(source="{}-log".format(container_id), target="/app/mictlanx/log", mount_type=1),
            MountX(source="{}-local".format(container_id), target="/app/mictlanx/local", mount_type=1),
        ],
        network_id="mictlanx",
        selected_node=Some(str(selected_node)),
        force=Some(True)
    )
    # print(payload.to_dict())
    response        = summoner.summon(
        mode= MICTLANX_SUMMONER_MODE,
        payload=payload, 
    )
    rt = T.time() - _start_time
    log.info({
        "event":"ADD.PEER",
        "peer_id":container_id,
        "disk":HF.format_size(disk),
        "memory":HF.format_size(memory),
        "selected_agent":selected_node,
        "port":port,
        "response_time":rt
    })
    return (container_id,port ,response)
    # if response.is_err:
    #     failed+=1 
    #     log.error({
    #         "detail":str(response.unwrap_err())
    #     })



@app.get("/api/v4/peers/stats")
async def get_peers_stats(
    start:int =0 ,
    end:int = 100
):
    try:
        responses = []
        # async with peer_healer_rwlock.reader_lock:
        peers = await ph.get_available_peers()
        failed = 0 
        for peer in peers:
            try:
                # headers = {}
                # url = "{}/api/v{}/stats?start={}&end={}".format(peer.base_url(),MICTLANX_API_VERSION,start,end)
                # # print(url)
                # response = R.get(url,headers=headers , timeout=MICTLANX_TIMEOUT)
                # response.raise_for_status()
                # res_json = response.json()
                # responses.append(res_json)
                result = peer.get_stats(timeout=10)
                if result.is_ok:
                    stats_response = result.unwrap()
                    responses.append(stats_response.__dict__)
                    log.debug({
                        "event":"STATS",
                        "peer_id":peer.peer_id,
                        "start":start,
                        "skip":end,
                        "used_disk":stats_response.used_disk,
                        "total_disk":stats_response.total_disk,
                        "available_disk":stats_response.available_disk,
                        "disk_uf":stats_response.disk_uf,
                        "balls":len(stats_response.balls)
                        # "balls":len(res_json["balls"])
                    })
            except R.exceptions.ConnectionError as e:
                detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
                log.error({
                    "detail":detail,
                    "status_code":500
                })
                failed+=1
            except Exception as e:
                failed+=1
                log.error(str(e))

        return JSONResponse(
            content=jsonable_encoder(responses),
            headers={
                "N-Peers-Failed":str(failed)
            }
        )
    except R.exceptions.HTTPError as e:
        detail = str(e.response.content.decode("utf-8") )
        # e.response.reason
        status_code = e.response.status_code
        log.error({
            "detail":detail,
            "status_code":status_code,
            "reason":e.response.reason,
        })
        raise HTTPException(status_code=status_code, detail=detail  )
    except R.exceptions.ConnectionError as e:
        detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
        log.error({
            "detail":detail,
            "status_code":500
        })
        raise HTTPException(status_code=500, detail=detail  )

@app.get("/api/v4/replicamap")
async def get_replica_map():
    global replicas_map
    async with replica_map_rwlock.reader_lock:
        return JSONResponse(content=jsonable_encoder(replicas_map))


@app.post("/api/v4/elastic/async")
async def elastic_async(
    replication_event:ReplicationEvent
):
    global replicator_queue
    global log
    try:
        start_time = T.time()
        await replicator_queue.put(item= replication_event)
        response_time = T.time() - start_time
        return JSONResponse(
            content=jsonable_encoder(
                {
                    "replication_event_id":replication_event.id,
                    "response_time":response_time
                }
            )
        )
    except Exception as e:
        log.error({
            "msg":str(e)
        })
        raise e

@app.post("/api/v4/elastic")
async def elastic(
    elastic_payload:PeerElasticPayload
):
    start_time = T.time()
    async with peer_healer_rwlock.reader_lock:
        current_peers = len(ph.peers)

    if current_peers >= elastic_payload.rf:
        response_time = T.time()- start_time
        return JSONResponse(content=jsonable_encoder({
            "pool_size":current_peers,
            "response_time":response_time
        }))
    

    diff = elastic_payload.rf - current_peers

    failed = 0
    tasks= []
    deployed_peer_info = []
    for task_index in range(diff):
        selected_node = 0
        peer_index = task_index + current_peers
        current_container_id    = "mictlanx-peer-{}".format(peer_index)
        port = MICTLANX_ROUTER_PEER_BASE_PORT+peer_index
        deployed_peer_info.append((current_container_id,port))
        task = deploy_peer(
            container_id=current_container_id,
            port=port,
            disk=elastic_payload.disk,
            memory= elastic_payload.memory,
            selected_node=selected_node,
            workers= elastic_payload.workers,
            elastic= elastic_payload.elastic
        )
        tasks.append(task)
    
    
    async with peer_healer_rwlock.writer_lock:
        deployed_peer_info += list(await ph.get_tupled_peers())
    responses:List[Tuple[str,int, Result[SummonContainerResponse, Exception]]]  = await asyncio.gather(*tasks)
    # print("RESPONSES_LIST", responses)
    
    for (current_container_id, current_container_port, summon_result) in responses:
        # _start_time = T.time()
        if summon_result.is_err:
            failed+=1
            log.error({
                "detail":str(summon_result.unwrap_err())
            })
        else:
            response = summon_result.unwrap()
            async with peer_healer_rwlock.writer_lock:
                peer = Peer(
                        peer_id=current_container_id,
                        ip_addr=current_container_id,
                        port=current_container_port,
                        protocol=elastic_payload.protocol
                )
                for (other_peer_id, other_peer_port) in deployed_peer_info:
                    if not current_container_id == other_peer_id:
                        op_start_time = T.time()
                        added_peer_result = peer.add_peer(
                            id=other_peer_id,
                            disk=elastic_payload.disk,
                            memory= elastic_payload.memory,
                            ip_addr= other_peer_id,
                            port=other_peer_port,
                            weight=1
                        )
                        rt = T.time() - op_start_time
                        log.info({
                            "event":"ADDED.OTHER.PEER",
                            "peer_id":peer.peer_id,
                            "added_peer_id":other_peer_id,
                            "added_peer_port":other_peer_port, 
                            "response_time":rt
                        })
                res = await ph.add_peer(peer=peer)
                # print("FINISHED", peer,res)
    response_time = T.time()- start_time
    log.info({
        "event":"ELASTIC",
        "pool_size":current_peers+(diff-failed),
        "failed":failed,
        "response_time":response_time
    })
    return JSONResponse(
        content=jsonable_encoder({
            "pool_size":current_peers+(diff-failed),
            "response_time":response_time
        })
    )


@app.post("/api/v4/replication")
async def replication(replication_event:ReplicationEvent):
    global replicator_queue
    global log
    try:
        start_time = T.time()
        await replicator_queue.put(item= replication_event)
        response_time = T.time() - start_time
        return JSONResponse(
            content=jsonable_encoder(
                {
                    "replication_event_id":replication_event.id,
                    "response_time":response_time
                }
            )
        )
    except Exception as e:
        log.error({
            "msg":str(e)
        })
        raise e



@app.get("/api/v4/buckets/{bucket_id}/metadata")
async def get_bucket_metadata(bucket_id):
    try:
        start_time = T.time()
        async with peer_healer_rwlock.reader_lock:
            peers = ph.peers.copy()
        
        response = {
            "bucket_id":bucket_id,
            "peers_ids":[],
            "balls":[]
        }
        current_balls:Dict[str,Tuple[int, Metadata]] = {}

        for peer in peers:
            # headers = {}
            result = peer.get_bucket_metadata(bucket_id=bucket_id,headers={})
            if result.is_err:
                continue
            metadata = result.unwrap()
            response["peers_ids"].append(peer.peer_id)
            for ball in metadata.balls:
                updated_at = int(ball.tags.get("updated_at","-1"))
                if not ball.key in current_balls :
                    current_balls.setdefault(ball.key, (updated_at,ball))
                    continue
                (current_updated_at,current_ball) = current_balls.get(ball.key)
                if  updated_at > current_updated_at:
                    current_balls[ball.key] = (updated_at,ball)
                
            filtered_balls = list(map(lambda x: x[1][1], current_balls.items()))
            response["balls"]+= filtered_balls
        log.info({
            "event":"GET.METADATA",
            "bucket_id":bucket_id,
            "peers_ids":response["peers_ids"],
            "balls":len(response["balls"]),
            "response_time":T.time() - start_time
        })
        return JSONResponse(content=jsonable_encoder(response))
    except R.exceptions.HTTPError as e:
        detail = str(e.response.content.decode("utf-8") )
        # e.response.reason
        status_code = e.response.status_code
        log.error({
            "detail":detail,
            "status_code":status_code,
            "reason":e.response.reason,
        })
        raise HTTPException(status_code=status_code, detail=detail  )
    except R.exceptions.ConnectionError as e:
        detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
        log.error({
            "detail":detail,
            "status_code":500
        })
        raise HTTPException(status_code=500, detail=detail  )

@app.get("/api/v4/buckets/{bucket_id}/metadata/{ball_id}/chunks")
async def get_metadata_chunks(
    bucket_id:str,
    ball_id:str,
):
    try:
        start_time = T.time()
        async with peer_healer_rwlock.reader_lock:
            peers = ph.peers
        responses:List[Metadata] = []
        for peer in peers:
            result = peer.get_chunks_metadata(bucket_id = bucket_id, key = ball_id)

            if result.is_err:
                continue
            response = list(result.unwrap())
            responses += response
        xs = sorted(responses,key=lambda x : int(x.tags.get("updated_at","-1")))
        log.info({
            "event":"GET.METADATA.CHUNKS",
            "bucket_id":bucket_id,
            "ball_id":ball_id,
            "response_time":T.time() - start_time
        })
        return xs


    except R.exceptions.HTTPError as e:
        detail = str(e.response.content.decode("utf-8") )
        # e.response.reason
        status_code = e.response.status_code
        log.error({
            "detail":detail,
            "status_code":status_code,
            "reason":e.response.reason,
        })
        raise HTTPException(status_code=status_code, detail=detail  )
    except R.exceptions.ConnectionError as e:
        detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
        log.error({
            "detail":detail,
            "status_code":500
        })
        raise HTTPException(status_code=500, detail=detail  )

@app.get("/api/v4/buckets/{bucket_id}/metadata/{key}")
async def get_metadata(
    bucket_id:str,
    key:str,
    consistency_model:Annotated[Union[str,None], Header()]="LB",
    peer_id:Annotated[Union[str,None], Header()]=None,
    
):
    # peers = ph.get_stats
    try:
        start_time = T.time()
        headers = {}
        fails = 0    
        # metadatas:List[GetMetadataResponse] = []
        max_last_updated_at=  -1
        most_recent_metadata = None

        async with peer_healer_rwlock.reader_lock:
            peers = ph.peers
            max_peers = len(ph.peers)

            if not peer_id is None:
                peer = ph.get_peer(peer_id=peer_id)
                if peer.is_none:
                    log.error({
                        "msg":"{} not found".format(peer_id),
                        "code":"404"
                    })
                    raise HTTPException(detail="{} not found".format(peer_id),status_code=404)
                else:
                    peers = [peer.unwrap()]

            

            # print("SELECTED_PEEER ", peer)
            if not consistency_model == "STRONG" and not consistency_model == "EVENTUAL" and peer_id == None:
                # async with peer_healer_rwlock.reader_lock:
                maybe_peer = await ph.next_peer(key=key,operation="GET")
                if maybe_peer.is_none:
                    raise HTTPException(status_code=404, detail="No peers available. Contact me get more information about the elasticity plan.")
                
                _peer = maybe_peer.unwrap()
                # if replicas_map.get()
                peers = [_peer]
        
        for peer in peers:
            metadata_result = peer.get_metadata(bucket_id=bucket_id, key=key, headers=headers)
            if metadata_result.is_err:
                fails+=1
            else:
                metadata = metadata_result.unwrap()
                if  consistency_model == "EVENTUAL":
                    most_recent_metadata = metadata
                    break
                    # return JSONResponse(content=jsonable_encoder(metadata) )
                elif consistency_model == "STRONG":
                    current_updated_at  = int(metadata.metadata.tags.get("updated_at","-1"))
                    if max_last_updated_at <= -1 or max_last_updated_at < current_updated_at:
                        max_last_updated_at = current_updated_at
                        most_recent_metadata = metadata
                else:
                    log.info({
                        "event":"GET.METADATA.COMPLETED",
                        "bucket_id":bucket_id,
                        "key":key,
                        "consistency_model":consistency_model,
                        "peer_id":metadata.peer_id,
                        "local_peer_id":metadata.local_peer_id,
                        "hit": int(metadata.local_peer_id == metadata.peer_id),
                        "size":metadata.metadata.size,
                        "response_time":T.time()- start_time
                    })
                    return JSONResponse(content=jsonable_encoder(metadata) )
        

        peer_ids = list(map(lambda x: x.peer_id, peers))
        if fails >= max_peers  or most_recent_metadata is None:
            raise HTTPException(status_code=404, detail="{}@{} not found".format(bucket_id,key))
        else:
            log.info({
                "event":"GET.METADATA.COMPLETED",
                "bucket_id":bucket_id,
                "key":key,
                "consistency_model":consistency_model,
                "peer_id":most_recent_metadata.peer_id,
                "local_peer_id":most_recent_metadata.local_peer_id,
                "size":most_recent_metadata.metadata.size,
                "peers":peer_ids,
                "response_time":T.time()- start_time
            })
            return JSONResponse(content=jsonable_encoder(most_recent_metadata) )
    except R.exceptions.HTTPError as e:
        detail = str(e.response.content.decode("utf-8") )
        status_code = e.response.status_code
        log.error({
            "event":"HTTP.ERROR",
            "detail":detail,
            "status_code":status_code,
            "reason":e.response.reason,
        })
        raise HTTPException(status_code=status_code, detail=detail  )
    except R.exceptions.ConnectionError as e:
        detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
        log.error({
            "event":"CONNECTION.ERROR",
            "detail":detail,
            "status_code":500
        })
        raise HTTPException(status_code=500, detail=detail  )
    except Exception as e:
        detail = str(e)
        log.error({
            "event":"EXCEPTION",
            "detail":detail,
            "status_code":500
        })
        raise HTTPException(status_code=500, detail=detail  )

    

async def content_generator(response:R.Response,chunk_size:str="5mb"):
    _chunk_size = HF.parse_size(chunk_size)
    for chunk in response.iter_content(chunk_size=_chunk_size):
        yield chunk  
        
             
@app.get("/api/v4/buckets/{bucket_id}/{key}")
async def get_data(
    bucket_id:str,
    key:str,
    content_type:str = "application/octet-stream",
    consistency_model:Annotated[Union[str,None], Header()]="STRONG"  ,
    chunk_size:Annotated[Union[str,None], Header()]="10mb",
    peer_id:Annotated[Union[str,None], Header()]=None,
    local_peer_id:Annotated[Union[str,None], Header()]=None,
    filename:Annotated[Union[str,None],Header()]=None
):
    # peers = ph.get_stats
    try:
        start_time = T.time()
      
        headers = {}
        fails = 0    
        # metadatas:List[GetMetadataResponse] = []
        max_last_updated_at=  -1
        most_recent_metadata:Tuple[GetMetadataResponse,Peer] = None
        # content_type = "application/octet-stream"
        
        async with peer_healer_rwlock.reader_lock:

            peers = await ph.get_available_peers()
            max_peers = len(peers)
            # peers     = ph.peers
            
            if not peer_id is None:
                peer = ph.get_peer(peer_id=peer_id)
                if peer.is_none:
                    raise HTTPException(detail="{} not found".format(peer_id),status_code=404)
                else:
                    peers = [peer.unwrap()]

        # print("PEERS",peers)
        for peer in peers:
            metadata_result = peer.get_metadata(bucket_id=bucket_id, key=key, headers=headers)
            if metadata_result.is_err:
                log.error({
                    "event":"GET.METADATA.FAILED",
                    "bucket_id":bucket_id,
                    "key":key,
                    "peer_id":peer.peer_id,
                })
                fails+=1
            else:
                metadata = metadata_result.unwrap()
                if  consistency_model == "EVENTUAL":
                    most_recent_metadata = (metadata,peer)
                    break
   
                elif consistency_model == "STRONG":
                    current_updated_at  = int(metadata.metadata.tags.get("updated_at","-1"))
                    if max_last_updated_at <= -1 or max_last_updated_at < current_updated_at:
                        max_last_updated_at = current_updated_at
                        most_recent_metadata = (metadata,peer)
                else:
                    most_recent_metadata = (metadata,peer)
                    break
        
        if fails >= max_peers  or most_recent_metadata is None:
            raise HTTPException(status_code=404, detail="{}@{} not found".format(bucket_id,key))
        else:
            (metadata, peer) = most_recent_metadata
            result = peer.get_streaming(bucket_id=bucket_id,key=key,headers=headers)
            if result.is_err:
                raise HTTPException(status_code=404, detail="{}@{} not found".format(bucket_id,key))
            else:
                response = result.unwrap()
                cg = content_generator(response=response,chunk_size=chunk_size)
                media_type = response.headers.get('Content-Type',content_type)
                _peer_id = metadata.peer_id if not peer_id else peer_id
                _local_peer_id = metadata.local_peer_id if not local_peer_id else local_peer_id

                print("PEER.ID",peer.peer_id)
                log.info({
                    "event":"GET.DATA.COMPLETED",
                    "bucket_id":bucket_id,
                    "key":key,
                    "size":metadata.metadata.size,
                    "peer_id":_peer_id,
                    "local_peer_id":_local_peer_id,
                    "hit":int(_local_peer_id==_peer_id),
                    "response_time":T.time() - start_time 
                })
                return StreamingResponse(cg, media_type=media_type)

            # return JSONResponse(content=jsonable_encoder(most_recent_metadata) )
            
    except R.exceptions.HTTPError as e:
        detail = str(e.response.content.decode("utf-8") )
        # e.response.reason
        status_code = e.response.status_code
        log.error({
            "event":"HTTP.ERROR",
            "detail":detail,
            "status_code":status_code,
            "reason":e.response.reason,
        })
        raise HTTPException(status_code=status_code, detail=detail  )
    except R.exceptions.ConnectionError as e:
        detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
        log.error({
            "event":"CONNECTION.ERROR",
            "detail":detail,
            "status_code":500
        })
        raise HTTPException(status_code=500, detail=detail  )
    except Exception as e:
        detail = str(e)
        log.error({
            "event":"EXCEPTION",
            "detail":detail,
            "status_code":500
        })
        raise HTTPException(status_code=500, detail=detail  )
        



@app.delete("/api/v4/buckets/{bucket_id}/{key}")
async def delete_data_by_key(
    bucket_id:str,
    key:str,
):
    _bucket_id = Utils.sanitize_str(x=bucket_id)
    _key = Utils.sanitize_str(x=key)
    try:
        async with peer_healer_rwlock.reader_lock:
            peers = ph.peers
        combined_key = "{}@{}".format(_bucket_id,_key)
        
        default_delete_by_key_response = DeletedByKeyResponse(n_deletes=0,key=_key)
        for peer in  peers:
            start_time= T.time()
            result = peer.delete(bucket_id=_bucket_id, key= _key)
            if result.is_ok:
                res = result.unwrap()
                # print("PEER.DELETE.RESULT", res)
                log.debug({
                    "event":"PEER.DELETE",
                    "peer_id":peer.peer_id,
                    "bucket_id":_bucket_id,
                    "key":_key,
                    "n_deletes":res.n_deletes
                })

                if res.n_deletes>=0:
                    default_delete_by_key_response.n_deletes+= res.n_deletes
                

        # print("PEER_RES", n_deletes)
        
        response_time = T.time() - start_time
        async with replica_map_rwlock.writer_lock:
            if combined_key in replicas_map:
                deleted_replicas = replicas_map.pop(combined_key)
        
        log.info({
            "event":"DELETE.BY.KEY",
            "bucket_id":_bucket_id,
            "key":_key,
            "n_deletes":default_delete_by_key_response.n_deletes,
            "response_time":response_time,
        })
        
        return JSONResponse(content=jsonable_encoder(default_delete_by_key_response.model_dump()))
        # return Response(content=None, status_code=200)
    
    except R.exceptions.HTTPError as e:
            detail = str(e.response.content.decode("utf-8") )
            status_code = e.response.status_code
            log.error({
                "detail":detail,
                "status_code":status_code,
                "reason":e.response.reason,
            })
            raise HTTPException(status_code=status_code, detail=detail  )
    except R.exceptions.ConnectionError as e:
        detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
        log.error({
            "detail":detail,
            "status_code":500
        })
        raise HTTPException(status_code=500, detail=detail  )



@app.delete("/api/v4/buckets/{bucket_id}/bid/{ball_id}")
async def delete_data_by_ball_id(
    bucket_id:str,
    ball_id:str,
):
    _bucket_id = Utils.sanitize_str(x= bucket_id)
    _ball_id = Utils.sanitize_str(x= ball_id)

    global replicas_map
    async with peer_healer_rwlock.reader_lock:
        peers = ph.peers.copy()
    headers = {}
    combined_key = ""
    _start_time = T.time()
    try:
        default_del_by_ball_id_response = DeletedByBallIdResponse(n_deletes=0, ball_id=_ball_id)
        for peer in  peers:
            start_time = T.time()
            chunks_metadata_result:Result[Iterator[Metadata],Exception] = peer.get_chunks_metadata(
                key=_ball_id,
                bucket_id=_bucket_id,
                headers=headers
            )
            # print("CHUNK_M_RESULT",chunks_metadata_result)
            if chunks_metadata_result.is_ok:
                response = chunks_metadata_result.unwrap()
                for i,metadata in enumerate(response):
                    if i ==0:
                        combined_key = "{}@{}".format(metadata.bucket_id,metadata.key)
                    del_result = peer.delete(bucket_id=_bucket_id, key=metadata.key)
                    service_time = T.time() - start_time
                    if del_result.is_ok:
                        del_response = del_result.unwrap()
                        if del_response.n_deletes>=0:
                            default_del_by_ball_id_response.n_deletes+= del_response.n_deletes
                    log.debug({
                        "event":"DELETE.BY.BALL_ID",
                        "bucket_id":_bucket_id,
                        "ball_id":_ball_id,
                        "key":metadata.key,
                        "peer_id":peer.peer_id,
                        "status":int(del_result.is_ok)-1,
                        "service_time":service_time,
                    })


        if combined_key == "" or len(combined_key) ==0:
            log.error({
                "event":"NOT.FOUND",
                "bucket_id":_bucket_id,
                "ball_id":_ball_id,
            })
            return JSONResponse(content=jsonable_encoder(default_del_by_ball_id_response.model_dump()))
        # Response(content=b"0",status_code=200)
            # raise HTTPException(status_code=404, detail="{}@{} not found".format(bucket_id,ball_id))
        
        async with replica_map_rwlock.writer_lock:
            deleted_replicas = []
            if combined_key in replicas_map:
                deleted_replicas = replicas_map.pop(combined_key)
            log.info({
                "event":"DELETED.REPLICA.BY.BALL_ID",
                "bucket_id":_bucket_id,
                "ball_id":_ball_id,
                "deleted_replicas": len(deleted_replicas),
                "response_time":T.time() - _start_time,
            })
        return JSONResponse(content=jsonable_encoder(default_del_by_ball_id_response))
    # Response(content=deleted_replicas, status_code=200)
    except R.exceptions.HTTPError as e:
        detail = str(e.response.content.decode("utf-8") )
        status_code = e.response.status_code
        log.error({
            "detail":detail,
            "status_code":status_code,
            "reason":e.response.reason,
        })
        raise HTTPException(status_code=status_code, detail=detail  )
    except R.exceptions.ConnectionError as e:
        detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
        log.error({
            "detail":detail,
            "status_code":500
        })
        raise HTTPException(status_code=500, detail=detail  )



@app.post("/api/v4/buckets/{bucket_id}/{key}/disable")
async def disable(bucket_id:str, key:str,):
    try:
        headers = {}
        start_time = T.time()
        async with peer_healer_rwlock.reader_lock:
            peers = ph.peers
        for peer in peers:
            res = peer.disable(bucket_id=bucket_id,key=key,headers=headers)
        log.info({
            "event":"DISABLE.COMPLETED",
            "bucket_id":bucket_id,
            "key":key,
            "service_time":T.time()-start_time
        })
        return Response(content=None, status_code=204)

    except R.exceptions.HTTPError as e:
        detail = str(e.response.content.decode("utf-8") )
        status_code = e.response.status_code
        log.error({
            "detail":detail,
            "status_code":status_code,
            "reason":e.response.reason,
        })
        raise HTTPException(status_code=status_code, detail=detail  )
    except R.exceptions.ConnectionError as e:
        detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
        log.error({
            "detail":detail,
            "status_code":500
        })
        raise HTTPException(status_code=500, detail=detail  )
        


async def fx(peer_payload:PeerPayload)->Result[str,Exception]:
    async with peer_healer_rwlock.writer_lock:
        try:
            status = await ph.add_peer(peer_payload.to_v4peer())
            log.debug({
                "event":"PEER.ADDED",
                "peer_id":peer_payload.peer_id,
                "protocol":peer_payload.protocol,
                "hostname":peer_payload.hostname,
                "port":peer_payload.port,
                "status":status
            })
            return Ok(peer_payload.peer_id)
        except R.exceptions.HTTPError as e:
            detail = str(e.response.content.decode("utf-8") )
            # e.response.reason
            status_code = e.response.status_code
            log.error({
                "detail":detail,
                "status_code":status_code,
                "reason":e.response.reason,
            })
            return Err(e)
            # raise HTTPException(status_code=status_code, detail=detail  )
        except R.exceptions.ConnectionError as e:
            # detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
            log.error({
                "detail":detail,
                "status_code":500
            })
            return Err(e)
            # raise HTTPException(status_code=500, detail=detail  )

        # print(ph.peers)

@app.post("/api/v4/xpeers")
async def add_peers(peers:List[PeerPayload]):
    try:
        start_time = T.time()
        peers_ids = list(map(lambda p: p.peer_id, peers))
        tasks = [ fx(peer_payload=p) for p in peers]
        res = list(filter(lambda x: len(x)>0,map(lambda r: r.unwrap_or(""),await asyncio.gather(*tasks))))
            # fx(peer_payload=peer)
        log.info({
            "event":"PEERS.ADDED",
            "added_peers":res,
            "response_time": T.time() - start_time
        })
        return Response(content=None, status_code=204)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail="ADD.PEERS.FAILED",

        )
    
@app.post("/api/v4/peers")
async def add_peer(peer:PeerPayload):
    try:
        start_time = T.time()
        asyncio.gather(
            fx(peer_payload=peer)
        )
        log.info({
            "event":"PEER.ADDED",
            "peer_id":peer.peer_id,
            "response_time": T.time() - start_time
        })
        return Response(content=None, status_code=204)
    except R.exceptions.HTTPError as e:
        detail = str(e.response.content.decode("utf-8") )
        # e.response.reason
        status_code = e.response.status_code
        log.error({
            "detail":detail,
            "status_code":status_code,
            "reason":e.response.reason,
        })
        raise HTTPException(status_code=status_code, detail=detail  )
    except R.exceptions.ConnectionError as e:
        detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
        log.error({
            "detail":detail,
            "status_code":500
        })
        raise HTTPException(status_code=500, detail=detail  )


@app.delete("/api/v4/peers/{peer_id}")
async def remove_peer(peer_id:str):
    try:
        start_time = T.time()
        async with peer_healer_rwlock.writer_lock:
            ph.remove_peer(peer_id=peer_id)
        log.info({
            "event":"REMOVE.PEER",
            "peer_id":peer_id,
            "response_time":T.time() - start_time
        })
        return Response(content=None, status_code=204)
    except R.exceptions.HTTPError as e:
        detail = str(e.response.content.decode("utf-8") )
        # e.response.reason
        status_code = e.response.status_code
        log.error({
            "detail":detail,
            "status_code":status_code,
            "reason":e.response.reason,
        })
        raise HTTPException(status_code=status_code, detail=detail  )
    except Exception as e:
        log.error({
            "msg":str(e),
        })
        raise HTTPException(status_code=500, detail="Something went wrong removin a peer {}".format(peer_id))



@app.post("/api/v4/buckets/{bucket_id}/metadata/{key}")
async def put_metadatas(
    bucket_id:str,
    key:str,
    metadata:Metadata, 
):
    start_time = T.time()
    headers = {"Update":"1"}
    combined_key = "{}@{}".format(bucket_id,key)
    if (not bucket_id == metadata.bucket_id) or not(metadata.key == key):
        raise HTTPException(
            status_code=400,
            detail="bucket_id o key are inconsistent with metadata"
        )
    # log.debug({
    #     "event":"PUT.METADATA",
    #     # "update":update,
    #     "combined_key":combined_key
    # })
    try: 
        # async with lock:
            # curent_replicas:Set[str] = replicas_map.setdefault(combined_key,set([]))
            # if len(curent_replicas)>=1 and not update =="1":
                # detail = "{}/{} already exists.".format(metadata.bucket_id,metadata.key)
                # raise HTTPException(status_code=500, detail=detail ,headers={} )

        failed_ops = []
        for peer in ph.peers:
            # print("UPDATE ", peer.peer_id)
            result:Result[PutMetadataResponse,Exception] = peer.put_metadata(
                bucket_id=metadata.bucket_id,
                key=metadata.key,
                ball_id=metadata.ball_id,
                size= metadata.size,
                checksum=metadata.checksum,
                producer_id=metadata.producer_id,
                content_type=metadata.content_type,
                tags=metadata.tags,
                is_disable=metadata.is_disabled,
                headers=headers
            )
            if result.is_err:
                failed_ops.append(peer)
        # async with lock:
        #     curent_replicas:Set[str] = replicas_map.setdefault(combined_key,set([]))
        #     curent_replicas.add(peer.peer_id)
        #     replicas_map[combined_key] = curent_replicas
        # response = result.unwrap()

        log.info({
            "event":"UPDATE.METADATA",
            "bucket_id":bucket_id,
            "key":metadata.key,
            "size":metadata.size,
            "failed":len(failed_ops),
            "service_time":T.time()- start_time
        })
        failed_len = len(failed_ops)
        response = {
            "bucket_id":bucket_id,
            "key":metadata.key,
            "size":metadata.size,
            "failed_operations":failed_len,
            "ok":failed_len == 0
        }
        return JSONResponse(content=jsonable_encoder(response))
        # return JSONResponse(content=jsonable_encoder(response))
    except R.exceptions.HTTPError as e:
        detail = str(e.response.content.decode("utf-8") )
        # e.response.reason
        status_code = e.response.status_code
        log.error({
            "detail":detail,
            "status_code":status_code,
            "reason":e.response.reason,
        })
        raise HTTPException(status_code=status_code, detail=detail  )
    except R.exceptions.ConnectionError as e:
        detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
        log.error({
            "detail":detail,
            "status_code":500
        })
        raise HTTPException(status_code=500, detail=detail  )

@app.post("/api/v4/buckets/{bucket_id}/metadata")
async def put_metadata(
    bucket_id:str,
    metadata:Metadata, 
    peer_id:Annotated[Union[str,None], Header()]=None,
    update:Annotated[Union[str,None], Header()] = "0"
):
    start_time = T.time()
    headers = {"Update":update}
    combined_key = "{}@{}".format(metadata.bucket_id,metadata.key)
    log.debug({
        "event":"PUT.METADATA",
        "update":update,
        "combined_key":combined_key,
    })
    try: 
        async with replica_map_rwlock.writer_lock:
            curent_replicas:Set[str] = replicas_map.setdefault(combined_key,set([]))

            if len(curent_replicas)>=1 and not update =="1":
                detail = "{}/{} already exists.".format(metadata.bucket_id,metadata.key)
                raise HTTPException(status_code=409, detail=detail ,headers={} )

        # async with lock: 
        
        if not peer_id:
            async with peer_healer_rwlock.writer_lock:
                peer = lb.lb(operation_type="put",algorithm=MICTLANX_ROUTER_LB_ALGORITHM,size=metadata.size)
            if peer == None:
                raise HTTPException(status_code=404, detail="No peers available.")
            
        else:
            with peer_healer_rwlock.writer_lock:
                maybe_peer = ph.get_peer(peer_id=peer_id)
            if maybe_peer.is_none:
                peer = lb.lb(operation_type="put",algorithm=MICTLANX_ROUTER_LB_ALGORITHM,size=metadata.size)
                if peer == None:
                    raise HTTPException(status_code=404, detail="No peers available")
            else:
                peer = maybe_peer.unwrap()
        # failed = []
        # for peer in peers:
        result:Result[PutMetadataResponse,Exception] = peer.put_metadata(
            bucket_id=metadata.bucket_id,
            key=metadata.key,
            ball_id=metadata.ball_id,
            size= metadata.size,
            checksum=metadata.checksum,
            producer_id=metadata.producer_id,
            content_type=metadata.content_type,
            tags=metadata.tags,
            is_disable=metadata.is_disabled,
            headers=headers
        )
        if result.is_err:
            raise result.unwrap_err()
        
        async with replica_map_rwlock.writer_lock:
            curent_replicas:Set[str] = replicas_map.setdefault(combined_key,set([]))
            curent_replicas.add(peer.peer_id)
            replicas_map[combined_key] = curent_replicas
        put_metadata_response = result.unwrap()

        log.info({
            "event":"PUT.METADATA",
            "bucket_id":bucket_id,
            "key":metadata.key,
            "size":metadata.size,
            "peer_id":peer.peer_id,
            "update":update,
            "response_time":T.time()- start_time
        })
        # task = Task(
        #     task_id=put_metadata_response.task_id,
        #     operation="PUT",
        #     bucket_id=bucket_id,
        #     key=metadata.key,
        #     peer_id=peer.peer_id,
        #     size=metadata.size
        # )
        # add_task_result = await task_manager.add_task(
            # task= task
        # )
        return JSONResponse(content=jsonable_encoder(put_metadata_response))
    except R.exceptions.HTTPError as e:
        detail = str(e.response.content.decode("utf-8") )
        # e.response.reason
        status_code = e.response.status_code
        log.error({
            "detail":detail,
            "status_code":status_code,
            "reason":e.response.reason,
        })
        raise HTTPException(status_code=status_code, detail=detail  )
    except R.exceptions.ConnectionError as e:
        detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
        status_code = e.response.status_code
        log.error({
            "detail":detail,
            "status_code":status_code
        })
        raise HTTPException(status_code=status_code, detail=detail  )
    
@app.post("/api/v4/buckets/data/{task_id}")
async def put_data(
    task_id:str,
    data:UploadFile, 
    peer_id:Annotated[Union[str,None], Header()]=None
):
    start_time = T.time()
    # log.debug({
    #     "event":"PUT.DATA",
    #     "task_id":task_id,
    #     "peer_id":peer_id
    # })
    async with peer_healer_rwlock.writer_lock: 
        if not peer_id:
            peer = lb.lb(operation_type="put",algorithm=MICTLANX_ROUTER_LB_ALGORITHM)
            if peer == None:
                raise HTTPException(status_code=404, detail="No peers available")
        else:
            maybe_peer = ph.get_peer(peer_id=peer_id)
            if maybe_peer.is_none:
                peer = lb.lb(operation_type="put",algorithm=MICTLANX_ROUTER_LB_ALGORITHM)
                if peer == None:
                    raise HTTPException(status_code=404, detail="No peers available")
            else:
                peer = maybe_peer.unwrap()
    try:

            
        value = await data.read()
        # _headers= {**}
        result = peer.put_data(task_id=task_id,key="",value=value,content_type="application/octet-stream")
        if result.is_err:
            raise result.unwrap_err()
        # response = result.unwrap()
        log.info({
            "event":"PUT.DATA",
            "task_id":task_id,
            "peer_id":peer.peer_id,
            "size":len(value),
            "service_time":T.time()- start_time
        })
        return Response(content=None, status_code=201)

    except R.exceptions.HTTPError as e:
        detail = str(e.response.content.decode("utf-8") )
        # e.response.reason
        status_code = e.response.status_code
        log.error({
            "detail":detail,
            "status_code":status_code,
            "reason":e.response.reason,
        })
        raise HTTPException(status_code=status_code, detail=detail  )
    except R.exceptions.ConnectionError as e:
        detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
        log.error({
            "detail":detail,
            "status_code":500
        })
        raise HTTPException(status_code=500, detail=detail  )
    

async def data_by_chunks(data:UploadFile,chunk_size:str="1MB"):
    _chunk_size= HF.parse_size(chunk_size)
    while True:
        chunk = await data.read(_chunk_size)
        if not chunk:
            break
        yield chunk

@app.post("/api/v4/buckets/data/{task_id}/chunked")
async def put_data_chunked(
    task_id:str,
    request: Request,
    # data:UploadFile,
    # chunk_size:Annotated[Union[str,None], Header()]="10mb",
    peer_id:Annotated[Union[str,None], Header()]=None,
    # update:Annotated[Union[int,None], Header()] = 0
):
    # if update:
        
    start_time = T.time()
    try:
        # task_maybe = await task_manager.get_task(task_id=task_id)
        # if task_maybe.is_err:
            # raise task_maybe.unwrap_err()
        
        # task = task_maybe.unwrap()

        async with peer_healer_rwlock.writer_lock: 
            if not peer_id:
                peer = lb.lb(operation_type="put",algorithm=MICTLANX_ROUTER_LB_ALGORITHM)
                if peer == None:
                    raise HTTPException(status_code=404, detail="No peers available")
            else:
                maybe_peer = ph.get_peer(peer_id=peer_id)
                if maybe_peer.is_none:
                    peer = lb.lb(operation_type="put",algorithm=MICTLANX_ROUTER_LB_ALGORITHM)
                    if peer == None:
                        raise HTTPException(status_code=404, detail="No peers available")

                else:
                    peer = maybe_peer.unwrap()

        headers= {}
        # chunks = data_by_chunks(data=data,chunk_size=chunk_size)
        # chunks = request.str
        # chunks = data_by_chunksv2(data=request,chunk_size=chunk_size)
        chunks = request.stream()
        result = await peer.put_chuncked_async(task_id=task_id, chunks = chunks,headers=headers)
        # result = peer.put_data(task_id=task_id,key="",value=value,content_type="application/octet-stream",headers=headers)
        # res:task_idult = None
        if result.is_err:
            raise result.unwrap_err()

        response = result.unwrap()
        log.info({
            "event":"PUT.CHUNKED.COMPLETED",
            "task_id":task_id,
            "peer_id":peer.peer_id,
            # "bucket_id":task.bucket_id,
            # "key":task.key,
            # "size":task.size,
            # "peer_id":task.peer_id,
            "service_time":T.time() - start_time
        })

        return JSONResponse(content=jsonable_encoder(response))

    except R.exceptions.HTTPError as e:
        detail = str(e.response.content.decode("utf-8") )
        # e.response.reason
        status_code = e.response.status_code
        log.error({
            "event":"HTTPError",
            "detail":detail,
            "status_code":status_code,
            "reason":e.response.reason,
        })
        raise HTTPException(status_code=status_code, detail=detail  )
    except R.exceptions.ConnectionError as e:
        detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
        log.error({
            "event":"CONNECTION.ERROR",
            "detail":detail,
            "status_code":500
        })
        raise HTTPException(status_code=500, detail=detail  )
    except Exception as e:
        log.error({
            "event":"EXCEPTION",
            "detail":str(e),
            "status_code":500
        })
        return HTTPException(status_code=500, detail = str(e))




@app.get("/api/v4/buckets/{bucket_id}/{key}/size")
async def get_size_by_key(bucket_id:str,key:str):
    global peer_healer_rwlock
    try:
        async with peer_healer_rwlock.reader_lock:
            peers = ph.peers
        

        responses = []
        for p in peers:
            result = p.get_size(bucket_id=bucket_id,key=key, timeout = MICTLANX_TIMEOUT)
            if result.is_ok:
                response = result.unwrap()
                responses.append(response)

        return JSONResponse(content = jsonable_encoder(responses))
    except Exception as e:
        raise HTTPException(status_code = 500, detail="Uknown error: {}@{}".format(bucket_id,key))



# if __name__ =="__main__":
#     uvicorn.run(
#         host=MICTLANX_ROUTER_HOST,
#         port=MICTLANX_ROUTER_PORT,
#         reload=MICTLANX_ROUTER_RELOAD,
#         app="server:app",
#         workers= MICTLANX_ROUTER_MAX_WORKERS
#     )