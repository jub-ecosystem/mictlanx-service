import os
from fastapi import FastAPI,Response,HTTPException,Header,UploadFile,Request
from contextlib import asynccontextmanager
from fastapi.responses import StreamingResponse
from fastapi.openapi.utils import get_openapi
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from mictlanx.v4.interfaces.responses import GetMetadataResponse,PutMetadataResponse,PutChunkedResponse
from mictlanx.logger.log import Log
from mictlanx.v4.interfaces.index import Peer
import asyncio
from asyncio import Lock
from queue import Queue
from typing import List,Dict,Any,Tuple,Union,Set,Iterator
from typing_extensions import Annotated
import requests as R
import uvicorn
import time as T
import humanfriendly as HF
from mictlanx.utils.index import Utils
from option import Result
from fastapi.middleware.gzip import GZipMiddleware
from mictlanx.logger.tezcanalyticx.tezcanalyticx import TezcanalyticXHttpHandler
from mictlanxrouter.interfaces.dto.metadata import Metadata
from mictlanxrouter.interfaces.dto.index import Peer as PeerPayload,DeletedByBallIdResponse,DeletedByKeyResponse
from mictlanxrouter.interfaces.healer import PeerHealer
from mictlanxrouter.interfaces.garbagcollector import MictlanXFMGarbageCollector
from mictlanxrouter.helpers.utils import Utils as LocalUtils
from mictlanxrouter.replication import Replicator
import random as RND
from mictlanx.v4.interfaces.index import PeerStats
import aiorwlock

LOGGER_NODE = os.environ.get("LOGGER_NAME","mictlanx-router-0")
TEZCANALYTICX_ENABLED = bool(int(os.environ.get("TEZCANALYTICX_ENABLE","0")))

log            = Log(
        name                   = LOGGER_NODE,
        console_handler_filter = lambda x: True,
        interval               = 24,
        when                   = "h",
        path                   = os.environ.get("LOG_PATH","/log")
)
# log.propagate = False
if TEZCANALYTICX_ENABLED :
    tezcanalytix_handler =  TezcanalyticXHttpHandler(
        flush_timeout= os.environ.get("TEZCANALYTICX_FLUSH_TIMEOUT","10s"),
        buffer_size= int(os.environ.get("TEZCANALYTICX_BUFFER_SIZE","100")),
        path= os.environ.get("TEZCANALYTICX_PATH","/api/v4/events/"),
        port= int(os.environ.get("TEZCANALYTICX_PORT","45000")),
        hostname= os.environ.get("TEZCANALYTICX_HOSTNAME"),
        level= int(os.environ.get("TEZCANALYTICX_LEVEL","0")),
        protocol=os.environ.get("TEZCANALYTICX_PROTOCOL","http")
    )
    log.addHandler(tezcanalytix_handler)

# _________________________________________
replicas_map:Dict[str, Set[str]] = {}
lock = Lock()
rwlock = aiorwlock.RWLock(fast=bool(int(os.environ.get("MICTLANX_ROUTER_SYNC_FAST","1"))))
rwlock2 = aiorwlock.RWLock(fast=bool(int(os.environ.get("MICTLANX_ROUTER_SYNC_FAST","1"))))

# peers_env = os.environ.get("MICTLANX_PEERS","mictlanx-peer-0:alpha.tamps.cinvestav.mx/v0/mictlanx/peer0:-1 mictlanx-peer-1:alpha.tamps.cinvestav.mx/v0/mictlanx/peer1:-1")
# peers_env = os.environ.get("MICTLANX_PEERS","peer-0:148.247.201.141:7000 peer-1:148.247.201.226:7001")
peers_env = os.environ.get("MICTLANX_PEERS","mictlanx-peer-0:localhost:7000 mictlanx-peer-1:localhost:7001 mictlanx-peer-2:localhost:7002")
protocol  = os.environ.get("MICTLANX_PROCOTOL","http")
peers     = list(Utils.peers_from_str_v2(peers_str=peers_env,separator=" ", protocol=protocol,) )
MICTLANX_API_VERSION = os.environ.get("MICTLANX_API_VERSION","4")
PEER_HEALER_HEARTBEAT = os.environ.get("MICTLANX_PEER_HEALER_HEARTBEAT","30sec")
GC_HEARTBEAT = os.environ.get("MICTLANX_GC_HEARTBEAT","30sec")
LB_ALGORITHM = os.environ.get("MICTLANX_LB_ALGORITHM","ROUND_ROBIN")
MICTLANX_TIMEOUT = int(os.environ.get("MICTLANX_TIMEOUT","3600"))

# 

log.debug({
    "event":"MICTLANX_ROUTER_STARTED",
    "peers_env":peers_env,
    "protocol":protocol,
    "api_version":MICTLANX_API_VERSION,
    "peer_healer_heartbeat":PEER_HEALER_HEARTBEAT,
    "gc_heartbeat":GC_HEARTBEAT,
    "load_balancing_algorithm":LB_ALGORITHM
})

@asynccontextmanager
async def lifespan(app:FastAPI):
    t1= asyncio.create_task(run_async_healer( heartbeat= PEER_HEALER_HEARTBEAT))
    # t2= asyncio.create_task(run_garbage_collector(GC_HEARTBEAT) )
    t2= asyncio.create_task(run_replicator(GC_HEARTBEAT) )
    yield
    t1.cancel()
    t2.cancel()

ph = PeerHealer(
    q = Queue(maxsize=100),
    peers=peers,
    name="mictlanx-fm-peer-healer-0",
    show_logs=False
)
replicator = Replicator(ph=ph, show_logs=True)
gc = MictlanXFMGarbageCollector(ph=ph, api_version=MICTLANX_API_VERSION)
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



# .openapi()

async def run_async_healer(heartbeat:str="10s"):
    _heartbeat = HF.parse_timespan(heartbeat)
    while True:
        try:
            async with rwlock2.writer_lock:
                log.debug({
                    "event":"ASYNC_HEALER",
                    "health_nodes":len(ph.peers),
                    "peers_ids":ph.peers_ids(),
                })
                ph.run()
                
        except Exception as e:
            pass
        finally:
            await asyncio.sleep(_heartbeat)

async def run_garbage_collector(heartbeat:str="30s"):
    _heartbeat = HF.parse_timespan(heartbeat)
    while True:
        try:
            async with rwlock2.writer_lock:
                await gc.run()
                log.debug({
                    "event":"GARBAGE_COLLECTOR"
                })
        except Exception as e:
            pass
        finally:
            await asyncio.sleep(_heartbeat)


async def run_replicator(heartbeat:str="1m"):
    _heartbeat = HF.parse_timespan(heartbeat)
    global replicator

    while True:
        try:
            await replicator.run()
        except Exception as e:
            print(e)
        finally:
            await asyncio.sleep(_heartbeat)
        


# class Pool(object):
#     def __init__(self,ph:PeerHealer,peers:List[Peer]=[]):
#         self.peers = peers
#         self.ph = ph
#     def delete_all(self,bucket_id:str, key:str,check_peers:bool=False)->List[Peer]:
#         if check_peers:
#             self.peers = self.ph.peers
#         failed_peers = []
#         for peer in self.peers:
#             result = peer.delete(bucket_id=bucket_id,key=key)
#             if result.is_err:
#                 failed_peers.append(peer)
#         return failed_peers




class LoadBalancer(object):
    def __init__(self, algorithm,peer_healer:PeerHealer):
        self.algorithm = algorithm
        self.peer_healer = peer_healer
        self.counter = {
            "get":0,
            "put":0
        }
        self.counter_per_node:Dict[str,int] = {}
    
    
    def lb_round_robin(self,operation_type:str="get"):
        try:
            total_requests = self.counter.get(operation_type,0)
            n = len(self.peer_healer.peers)
            current_peer_index =total_requests%n
            return self.peer_healer.peers[current_peer_index]
        except Exception as e:
            raise e
        finally:
            self.counter[operation_type] = self.counter.setdefault(operation_type,0) +1 

    def lb_sort_uf(self,size:int,key:str="",operation_type:str="get"):
        try:
            stats = ph.get_stats()
            min_uf = -1
            min_uf_peer = None
            min_stats = None
            n = len(list(stats.keys()))
            if n == 0 :
                return None
            
            for peer_id,stats in stats.items():
                uf = Utils.calculate_disk_uf(total=stats.total_disk,used=stats.used_disk , size=size)
                if min_uf == -1:
                    min_uf = uf
                    min_uf_peer = peer_id
                    min_stats = stats
                else:
                    if uf < min_uf:
                        min_uf = uf
                        min_uf_peer = peer_id
                        min_stats = stats
            x = next(filter(lambda p: p.peer_id==min_uf_peer,self.peer_healer.peers),None)
            min_stats.put(key= key,size=size)
            return  x
        except Exception as e:
            return None
    
    def lb_2c_uf(self,size:int,key:str="", operation_type:str="put"):
        stats = ph.get_stats()
        keys = list(stats.keys())
        if len(keys) == 0 :
            return None
        
        if len(keys) == 1: 
            x:PeerStats = stats[keys[0]]
            x.put(key= "",size=size)
            selected = next(filter(lambda y: x.get_id() == y.peer_id, self.peer_healer.peers), None)
            return selected
        else:
            xs = RND.sample(keys,2)
            ys = list(map(lambda x: stats[x], xs))
            y1 = ys[0].calculate_disk_uf(size=size)
            y2 = ys[1].calculate_disk_uf(size=size)
            if y1 < y2:
                ys[0].put(key= key,size=size)
                selected = next(filter(lambda y: ys[0].get_id() == y.peer_id, self.peer_healer.peers), None)
            else:
                ys[1].put(key= key,size=size)
                selected = next(filter(lambda y: ys[1].get_id() == y.peer_id, self.peer_healer.peers), None)
            return selected
        
        

    def lb(self,operation_type:str="get",algorithm:str ="SORTING_UF",key:str ="",size:int=0):
        if algorithm == "SORTING_UF":
            return self.lb_sort_uf(size=size, operation_type=operation_type,key=key)
        elif algorithm =="ROUND_ROBIN":
            return self.lb_round_robin(operation_type=operation_type)
        elif algorithm == "2C_UF":
            return self.lb_2c_uf(operation_type=operation_type,size=size,key=key)
        else:
            return self.lb_sort_uf(size=size, operation_type=operation_type,key=key)
lb = LoadBalancer(algorithm=os.environ.get("MICTLANX_FM_LB_ALGORITHM","ROUND_ROBIN"), peer_healer=ph)



@app.get("/api/v4/peers/stats")
async def get_peers_stats(
    start:int =0 ,
    end:int = 100
):
    try:
        responses = []
        async with rwlock2.reader_lock:
            peers = ph.peers.copy()
        
        for peer in peers:
            headers = {}
            url = "{}/api/v{}/stats?start={}&end={}".format(peer.base_url(),MICTLANX_API_VERSION,start,end)
            # print(url)
            response = R.get(url,headers=headers , timeout=MICTLANX_TIMEOUT)
            response.raise_for_status()
            res_json = response.json()
            responses.append(res_json)
            log.debug({
                "event":"STATS",
                "peer_id":peer.peer_id,
                "start":start,
                "skip":end,
                "balls":len(res_json["balls"])
            })
        return JSONResponse(content=jsonable_encoder(responses))
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


@app.get("/api/v4/buckets/{bucket_id}/metadata")
async def get_bucket_metadata(bucket_id):
    try:
        start_time = T.time()
        async with rwlock2.reader_lock:
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
        async with rwlock2.reader_lock:
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

        async with rwlock2.reader_lock:
            peers = ph.peers
            max_peers = len(ph.peers)

        if not peer_id is None:
            peer = ph.get_peer(peer_id=peer_id)
            if peer.is_none:
                raise HTTPException(detail="{} not found".format(peer_id),status_code=404)
            else:
                peers = [peer.unwrap()]

        if not consistency_model == "STRONG" and not consistency_model == "EVENTUAL":
            async with lock:
                maybe_peer = ph.next_peer(key=key,operation="GET")
                if maybe_peer.is_none:
                    raise HTTPException(status_code=404, detail="No peers available. Contact me get more information about the elasticity plan.")
                _peer = maybe_peer.unwrap()
                peers = [_peer]
        #     shuff
            # return 
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
                "peer_id":most_recent_metadata.node_id,
                "size":most_recent_metadata.metadata.size,
                "peers":peer_ids,
                "response_time":T.time()- start_time
            })
            return JSONResponse(content=jsonable_encoder(most_recent_metadata) )
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
    

async def content_generator(response:R.Response,chunk_size:str="5mb"):
    _chunk_size = HF.parse_size(chunk_size)
    for chunk in response.iter_content(chunk_size=_chunk_size):
        yield chunk  
        
             
@app.get("/api/v4/buckets/{bucket_id}/{key}")
async def get_data(
    bucket_id:str,
    key:str,
    consistency_model:Annotated[Union[str,None], Header()]="STRONG"  ,
    content_type:Annotated[Union[str,None], Header()]="application/octet-stream"  ,
    chunk_size:Annotated[Union[str,None], Header()]="10mb",
    peer_id:Annotated[Union[str,None], Header()]=None,
    filename:Annotated[Union[str,None],Header()]=None
):
    # peers = ph.get_stats
    try:
        start_time = T.time()
      
        headers = {}
        max_peers = len(ph.peers)
        fails = 0    
        # metadatas:List[GetMetadataResponse] = []
        max_last_updated_at=  -1
        most_recent_metadata:Tuple[GetMetadataResponse,Peer] = None
        # content_type = "application/octet-stream"
        
        async with rwlock2.reader_lock:
            peers = ph.peers

        if not peer_id is None:
            peer = ph.get_peer(peer_id=peer_id)
            if peer.is_none:
                raise HTTPException(detail="{} not found".format(peer_id),status_code=404)
            else:
                peers = [peer.unwrap()]

        for peer in peers:
            metadata_result = peer.get_metadata(bucket_id=bucket_id, key=key, headers=headers)
            # print(metadata_result)
            if metadata_result.is_err:
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
                log.info({
                    "event":"GET.DATA.COMPLETED",
                    "bucket_id":bucket_id,
                    "key":key,
                    "size":metadata.metadata.size,
                    "peer_id":peer.peer_id,
                    "response_time":T.time() - start_time 
                })
                return StreamingResponse(cg, media_type=media_type)

            # return JSONResponse(content=jsonable_encoder(most_recent_metadata) )
            
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



@app.delete("/api/v4/buckets/{bucket_id}/{key}")
async def delete_data_by_key(
    bucket_id:str,
    key:str,
):
    _bucket_id = Utils.sanitize_str(x=bucket_id)
    _key = Utils.sanitize_str(x=key)
    try:
        async with rwlock2.reader_lock:
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
        async with rwlock.writer_lock:
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
    async with rwlock2.reader_lock:
        peers = ph.peers
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
        
        async with rwlock.writer_lock:
            deleted_replicas = 0 
            if combined_key in replicas_map:
                deleted_replicas = replicas_map.pop(combined_key)
            log.info({
                "event":"DELETED.REPLICA.BY.BALL_ID",
                "bucket_id":_bucket_id,
                "ball_id":_ball_id,
                "deleted_replicas": list(deleted_replicas),
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
        async with rwlock2.reader_lock:
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
        


async def fx(peer:PeerPayload):
    async with rwlock2.writer_lock:
        status = ph.add_peer(peer.to_v4peer())
        log.debug({
            "event":"PEER.ADDED",
            "peer_id":peer.peer_id,
            "protocol":peer.protocol,
            "hostname":peer.hostname,
            "port":peer.port,
            "status":status
        })
        # print(ph.peers)

@app.post("/api/v4/peers")
async def add_peer(peer:PeerPayload):
    try:
        start_time = T.time()
        asyncio.gather(
            fx(peer=peer)
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
        async with rwlock2.writer_lock:
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
    log.debug({
        "event":"PUT.METADATA",
        # "update":update,
        "combined_key":combined_key
    })
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
        async with rwlock.writer_lock:
            curent_replicas:Set[str] = replicas_map.setdefault(combined_key,set([]))

            if len(curent_replicas)>=1 and not update =="1":
                detail = "{}/{} already exists.".format(metadata.bucket_id,metadata.key)
                raise HTTPException(status_code=409, detail=detail ,headers={} )

        # async with lock: 
        
        if not peer_id:
            async with rwlock2.writer_lock:
                peer = lb.lb(operation_type="put",algorithm=LB_ALGORITHM,size=metadata.size)
            if peer == None:
                raise HTTPException(status_code=404, detail="No peers available")
            
        else:
            with rwlock2.writer_lock:
                maybe_peer = ph.get_peer(peer_id=peer_id)
            if maybe_peer.is_none:
                peer = lb.lb(operation_type="put",algorithm=LB_ALGORITHM,size=metadata.size)
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
        
        async with rwlock.writer_lock:
            curent_replicas:Set[str] = replicas_map.setdefault(combined_key,set([]))
            curent_replicas.add(peer.peer_id)
            replicas_map[combined_key] = curent_replicas
        response = result.unwrap()

        log.info({
            "event":"PUT.METADATA",
            "bucket_id":bucket_id,
            "key":metadata.key,
            "size":metadata.size,
            "peer_id":peer.peer_id,
            "update":update,
            "service_time":T.time()- start_time
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
    log.debug({
        "event":"PUT.DATA",
        "task_id":task_id,
        "peer_id":peer_id
    })
    async with rwlock2.writer_lock: 
        if not peer_id:
            peer = lb.lb(operation_type="put",algorithm=LB_ALGORITHM)
            if peer == None:
                raise HTTPException(status_code=404, detail="No peers available")
        else:
            maybe_peer = ph.get_peer(peer_id=peer_id)
            if maybe_peer.is_none:
                peer = lb.lb(operation_type="put",algorithm=LB_ALGORITHM)
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
            "event":"PUT.DATA.COMPLETED",
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
        async with rwlock2.writer_lock: 
            if not peer_id:
                peer = lb.lb(operation_type="put",algorithm=LB_ALGORITHM)
                if peer == None:
                    raise HTTPException(status_code=404, detail="No peers available")
            else:
                maybe_peer = ph.get_peer(peer_id=peer_id)
                if maybe_peer.is_none:
                    peer = lb.lb(operation_type="put",algorithm=LB_ALGORITHM)
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
            "service_time":T.time() - start_time
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




@app.get("/api/v4/buckets/{bucket_id}/{key}/size")
async def get_size_by_key(bucket_id:str,key:str):
    global rwlock2
    try:
        async with rwlock2.reader_lock:
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

if __name__ =="__main__":
    uvicorn.run(
        host=os.environ.get("IP_ADDR","0.0.0.0"), 
        port=int(os.environ.get("MICTLANX_ROUTER_PORT","60666")),
        reload=bool(int(os.environ.get("REALOAD","1"))),
        app="server:app",
        workers= int(os.environ.get("MAX_WORKERS","4"))
    )