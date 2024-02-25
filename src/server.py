import os
from fastapi import FastAPI,Response,HTTPException,Header,UploadFile
from fastapi.responses import StreamingResponse
from fastapi.openapi.utils import get_openapi
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from mictlanx.v4.interfaces.responses import GetMetadataResponse,PutMetadataResponse,PutChunkedResponse
from mictlanx.logger.log import Log
from mictlanx.v4.interfaces.index import Peer,PeerStats
import asyncio
from asyncio import Lock
from queue import Queue
from threading import Thread
from typing import List,Dict,Any,Tuple,Union,Set,AsyncGenerator,Generator
from typing_extensions import Annotated
import requests as R
import uvicorn
import time as T
import humanfriendly as HF
from mictlanx.utils.index import Utils
from option import NONE,Some,Option,Result
from fastapi.middleware.gzip import GZipMiddleware
from mictlanx.logger.tezcanalyticx.tezcanalyticx import TezcanalyticXHttpHandler
from interfaces.dto.metadata import Metadata
from interfaces.dto.index import Peer as PeerPayload
from interfaces.healer import PeerHealer
from interfaces.garbagcollector import MictlanXFMGarbageCollector

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
        buffer_size= int(os.environ.get("TEZCANALYTICX_BUFFER_SIZe","100")),
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

# peers_env = os.environ.get("MICTLANX_PEERS","mictlanx-peer-0:alpha.tamps.cinvestav.mx/v0/mictlanx/peer0:-1 mictlanx-peer-1:alpha.tamps.cinvestav.mx/v0/mictlanx/peer1:-1")
# peers_env = os.environ.get("MICTLANX_PEERS","peer-0:148.247.201.141:7000 peer-1:148.247.201.226:7001")
peers_env = os.environ.get("MICTLANX_PEERS","peer-0:localhost:7000 peer-1:localhost:7001 peer-2:localhost:7002")
protocol  = os.environ.get("MICTLANX_PROCOTOL","http")
peers     = list(Utils.peers_from_str_v2(peers_str=peers_env,separator=" ", protocol=protocol,) )
MICTLANX_API_VERSION = os.environ.get("MICTLANX_API_VERSION","4")
PEER_HEALER_HEARTBEAT = os.environ.get("MICTLANX_PEER_HEALER_HEARTBEAT","30sec")
GC_HEARTBEAT = os.environ.get("MICTLANX_GC_HEARTBEAT","5sec")

ph = PeerHealer(
    q = Queue(maxsize=100),
    peers=peers,
    name="mictlanx-fm-peer-healer-0",
)
gc = MictlanXFMGarbageCollector(ph=ph, api_version=MICTLANX_API_VERSION)
app = FastAPI(root_path=os.environ.get("OPENAPI_PREFIX","/mictlanx-router-0"))

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
            async with lock:
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
            async with lock:
                await gc.run()
                log.debug({
                    "event":"GARBAGE_COLLECTOR"
                })
        except Exception as e:
            pass
        finally:
            await asyncio.sleep(_heartbeat)




class Pool(object):
    def __init__(self,ph:PeerHealer,peers:List[Peer]=[]):
        self.peers = peers
        self.ph = ph
    def delete_all(self,bucket_id:str, key:str,check_peers:bool=False)->List[Peer]:
        if check_peers:
            self.peers = self.ph.peers
        failed_peers = []
        for peer in self.peers:
            result = peer.delete(bucket_id=bucket_id,key=key)
            if result.is_err:
                failed_peers.append(peer)
        return failed_peers




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

lb = LoadBalancer(algorithm=os.environ.get("MICTLANX_FM_LB_ALGORITHM","ROUND_ROBIN"), peer_healer=ph)


async def fx(peer:PeerPayload):
    async with lock:
        status = ph.add_peer(peer.to_v4peer())
        log.debug({
            "event":"PEER.ADDED",
            "peer_id":peer.peer_id,
            "protocol":peer.protocol,
            "hostname":peer.hostname,
            "port":peer.port,
            "status":status
        })
        print(ph.peers)

@app.post("/api/v4/peers")
async def add_peer(peer:PeerPayload):
    try:
        asyncio.gather(
            fx(peer=peer)
        )
        return Response(content=None, status_code=204)
    except R.exceptions.HTTPError as e:
        return HTTPException(status_code=e.response.status_code, detail=str(e.response.content.decode("utf-8")))
    except Exception as e :
        return HTTPException(status_code=400, detail=str(e))


@app.get("/api/v4/peers/stats")
async def get_peers_stats():
    responses = []
    for peer in peers:
        headers = {}
        url = "{}/api/v{}/stats".format(peer.base_url(),MICTLANX_API_VERSION)
        response = R.get(url,headers=headers)
        response.raise_for_status()
        res_json = response.json()
        responses.append(res_json)
        log.debug({
            "event":"STATS",
            "peer_id":peer.peer_id,
            "balls":len(res_json["balls"])
        })
    return JSONResponse(content=jsonable_encoder(responses))



@app.post("/api/v4/buckets/{bucket_id}/metadata")
async def put_metadata(bucket_id:str,metadata:Metadata):
    headers = {}
    combined_key = "{}@{}".format(metadata.bucket_id,metadata.key)
    async with lock:
        curent_replicas:Set[str] = replicas_map.setdefault(combined_key,set([]))
        if len(curent_replicas)>=1:
            return HTTPException(status_code=500, detail="{}/{} already exists.".format(metadata.bucket_id,metadata.key),headers={})
        
    log.debug({
        "event":"PUT.METADATA",
        "bucket_id":bucket_id,
        "key":metadata.key,
        "size":metadata.size
    })

    peer = lb.lb_round_robin(operation_type="put")
        
    try: 
        result:Result[PutMetadataResponse,Exception] = peer.put_metadata(
            bucket_id=metadata.bucket_id,
            key=metadata.key,
            ball_id=metadata.ball_id,
            size= metadata.size,
            checksum=metadata.checksum,
            producer_id=metadata.producer_id,
            content_type=metadata.content_type,
            tags=metadata.tags,
            is_disable=metadata.is_disable,
            headers=headers
        )

        if result.is_err:
            raise result.unwrap_err()
        
        async with lock:
            curent_replicas:Set[str] = replicas_map.setdefault(combined_key,set([]))
            curent_replicas.add(peer.peer_id)
            replicas_map[combined_key] = curent_replicas
        response = result.unwrap()
        return JSONResponse(content=jsonable_encoder(response))
    except R.exceptions.HTTPError as e:
        return HTTPException(status_code=e.response.status_code, detail=str(e.response.content.decode("utf-8")))
    except R.exceptions.ConnectionError as e:
        return HTTPException(status_code=500, detail="Connection error - peers unavailable - {}".format(peer.peer_id) )
    except Exception as e :
        return HTTPException(status_code=400, detail=str(e))

#   .route("/buckets/data/{task_id}/chunked", web::post().to(handlers::put_chunked_data ))
    
@app.post("/api/v4/buckets/data/{task_id}")
async def put_data(task_id:str,data:UploadFile):
    try:
        log.debug({
            "event":"PUT.DATA",
            "task_id":task_id
        })
        peer = lb.lb_round_robin(operation_type="put")
        value = await data.read()
        headers= {}
        result = peer.put_data(task_id=task_id,key="",value=value,content_type="application/octet-stream",headers=headers)
        if result.is_err:
            raise result.unwrap_err()
        response = result.unwrap()
        return Response(content=None, status_code=201)
    except R.exceptions.HTTPError as e:
        return HTTPException(status_code=e.response.status_code, detail=str(e.response.content.decode("utf-8")))
    except Exception as e :
        return HTTPException(status_code=400, detail=str(e))
    

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
    data:UploadFile,
    chunk_size:Annotated[Union[str,None], Header()]="10mb",
    peer_id:Annotated[Union[str,None], Header()]=None,
    # update:Annotated[Union[bool,None], Header()]=False
):
    # if update:
        
    try:
        if not peer_id:
            peer = lb.lb_round_robin(operation_type="put")
        else:
            maybe_peer = ph.get_peer(peer_id=peer_id)
            if maybe_peer.is_none:
                peer = lb.lb_round_robin(operation_type="put")
            else:
                peer = maybe_peer.unwrap()

        headers= {}
        chunks = data_by_chunks(data=data,chunk_size=chunk_size)
        result = await peer.put_chuncked_async(task_id=task_id, chunks = chunks,headers=headers)
        # result = peer.put_data(task_id=task_id,key="",value=value,content_type="application/octet-stream",headers=headers)
        log.debug({
            "event":"PUT.CHUNKED",
            "task_id":task_id,
            "peer_id":peer.peer_id
        })
        if result.is_err:
            raise result.unwrap_err()
        response = result.unwrap()
        return JSONResponse(content=jsonable_encoder(response))
    except R.exceptions.HTTPError as e:
        return HTTPException(status_code=e.response.status_code, detail=str(e.response.content.decode("utf-8")))
    except Exception as e :
        return HTTPException(status_code=400, detail=str(e))




@app.get("/api/v4/buckets/{bucket_id}/metadata/{key}")
async def get_metadata(
    bucket_id:str,
    key:str,
    consistency_model:Annotated[Union[str,None], Header()]="STRONG",
    peer_id:Annotated[Union[str,None], Header()]=None
):
    # peers = ph.get_stats
    headers = {}
    max_peers = len(ph.peers)
    fails = 0    
    # metadatas:List[GetMetadataResponse] = []
    max_last_updated_at=  -1
    most_recent_metadata = None
    for peer in ph.peers:
        metadata_result = peer.get_metadata(bucket_id=bucket_id, key=key, headers=headers)
        if metadata_result.is_err:
            fails+=1
        else:
            metadata = metadata_result.unwrap()
            if  consistency_model == "EVENTUAL":
                return JSONResponse(content=jsonable_encoder(metadata) )
            elif consistency_model == "STRONG":
                current_updated_at  = int(metadata.metadata.tags.get("updated_at","-1"))
                if max_last_updated_at <= -1 or max_last_updated_at < current_updated_at:
                    max_last_updated_at = current_updated_at
                    most_recent_metadata = metadata
            else:
                return JSONResponse(content=jsonable_encoder(metadata) )
    if fails >= max_peers :
        return HTTPException(status_code=404, detail="{}@{} not found".format(bucket_id,key))
    else:
        return JSONResponse(content=jsonable_encoder(most_recent_metadata) )
    

def content_generator(response:R.Response,chunk_size:str="5mb"):
    _chunk_size = HF.parse_size(chunk_size)
    for chunk in response.iter_content(chunk_size=_chunk_size):
        yield chunk       
@app.get("/api/v4/buckets/{bucket_id}/{key}")
async def get_data(
    bucket_id:str,
    key:str,
    consistency_model:Annotated[Union[str,None], Header()]="STRONG"  ,
    content_type:Annotated[Union[str,None], Header()]="application/octet-stream"  ,
    chunk_size:Annotated[Union[str,None], Header()]="10mb"  
):
    # peers = ph.get_stats
    log.debug({
        "consistency_model":consistency_model,
        "chunk_size":chunk_size,
        "content_type":content_type
    })
    headers = {}
    max_peers = len(ph.peers)
    fails = 0    
    # metadatas:List[GetMetadataResponse] = []
    max_last_updated_at=  -1
    most_recent_metadata:Tuple[GetMetadataResponse,Peer] = None
    content_type = "application/octet-stream"
    

    for peer in ph.peers:
        metadata_result = peer.get_metadata(bucket_id=bucket_id, key=key, headers=headers)
        if metadata_result.is_err:
            fails+=1
        else:
            metadata = metadata_result.unwrap()
            if  consistency_model == "EVENTUAL":
                result = peer.get_streaming(bucket_id=bucket_id,key=key,headers=headers)
                if result.is_err:
                    return HTTPException(status_code=404, detail="{}@{} not found".format(bucket_id,key))
                else:
                    response = result.unwrap()
                    return StreamingResponse(content_generator(response=response,chunk_size=chunk_size), media_type=response.headers.get('Content-Type',content_type))
                    # return JSONResponse(content=jsonable_encoder(metadata) )
            elif consistency_model == "STRONG":
                current_updated_at  = int(metadata.metadata.tags.get("updated_at","-1"))
                if max_last_updated_at <= -1 or max_last_updated_at < current_updated_at:
                    max_last_updated_at = current_updated_at
                    most_recent_metadata = (metadata,peer)
            else:
                result = peer.get_streaming(bucket_id=bucket_id,key=key,headers=headers)
                if result.is_err:
                    return HTTPException(status_code=404, detail="{}@{} not found".format(bucket_id,key))
                else:
                    response = result.unwrap()
                    return StreamingResponse(content_generator(response=response, chunk_size=chunk_size), media_type=response.headers.get('Content-Type',content_type))
                # return JSONResponse(content=jsonable_encoder(metadata) )
    if fails >= max_peers :
        return HTTPException(status_code=404, detail="{}@{} not found".format(bucket_id,key))
    else:
        (metadata, peer) = most_recent_metadata
        result = peer.get_streaming(bucket_id=bucket_id,key=key,headers=headers)
        if result.is_err:
            return HTTPException(status_code=404, detail="{}@{} not found".format(bucket_id,key))
        else:
            response = result.unwrap()
            return StreamingResponse(content_generator(response=response,chunk_size=chunk_size), media_type=response.headers.get('Content-Type',content_type))

        # return JSONResponse(content=jsonable_encoder(most_recent_metadata) )
        



@app.on_event("startup")
async def startup_event():
    asyncio.create_task(run_async_healer( heartbeat= PEER_HEALER_HEARTBEAT))
    asyncio.create_task(run_garbage_collector(GC_HEARTBEAT) )

if __name__ =="__main__":
    uvicorn.run(
        host=os.environ.get("IP_ADDR","0.0.0.0"), 
        port=int(os.environ.get("PORT","60666")),
        reload=bool(int(os.environ.get("REALOAD","1"))),
        app="server:app"
    )