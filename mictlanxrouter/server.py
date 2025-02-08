import os
import sys
import requests as R
import time as T
import humanfriendly as HF
import aiorwlock
import signal
from asyncio.queues import Queue
import asyncio
from fastapi import FastAPI,Response,HTTPException,Header,UploadFile,Request
# import tempfile
# from secretsharing import PlaintextToHexSecretSharer

from contextlib import asynccontextmanager
from fastapi.openapi.utils import get_openapi
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from typing import Iterator
from typing_extensions import Annotated
from option import Result,Some,Ok,Err
from fastapi.middleware.gzip import GZipMiddleware
from ipaddress import IPv4Network
# 
import mictlanxrouter.caching as ChX
import mictlanxrouter.controllers as Cx
from mictlanxrouter.replication_manager import ReplicaManager,ReplicaManagerParams,DataReplicator
from mictlanxrouter.dto.metadata import Metadata
from mictlanxrouter.dto.index import DeletedByBallIdResponse,DeletedByKeyResponse
from mictlanxrouter.peer_manager.healer import StoragePeerManager,StoragePeerManagerParams
from mictlanxrouter.dto.index import PeerElasticPayload
# 
from mictlanx.v4.interfaces.responses import PutMetadataResponse
from mictlanx.logger.log import Log
from mictlanx.v4.summoner.summoner import  Summoner
from mictlanx.utils.index import Utils
from mictlanxrouter._async import run_async_healer,run_rm,run_data_replicator
from mictlanxrouter.tasksx import DefaultTaskManager
from mictlanxrouter.opentelemetry import NoOpSpanExporter
# ===========================================================
# Opentelemetry
# ===========================================================
# from opentelemetry.instrumentation.requests import RequestsInstrumentor
# Prometheus
from prometheus_client import Counter
from prometheus_fastapi_instrumentator import Instrumentator
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry import trace
from opentelemetry.trace import Span,Status,StatusCode

ACCESS_COUNTER = Counter('data_access', 'Total number of access', ['bucket_id', 'key'])
METADATA_ACCESS_COUNTER = Counter('metadata_access', 'Total number of metadata access', ['bucket_id', 'key'])

# from opentelemetry.span
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter
)
from opentelemetry.exporter.zipkin.json import ZipkinExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

MICTLANX_DEBUG                  = bool(int(os.environ.get("MICTLANX_DEBUG","0")))
MICTLANX_CACHE                  = bool(int(os.environ.get("MICTLANX_CACHE",1)))
MICTLANX_CACHE_EVICTION_POLICY  = os.environ.get("MICTLANX_CACHE_EVITION_POLICY","LRU")
MICTLANX_CACHE_CAPACITY         = int(os.environ.get("MICTLANX_CACHE_CAPACITY","100"))
MICTLANX_CACHE_CAPACITY_STORAGE = HF.parse_size(os.environ.get("MICTLANX_CACHE_CAPACITY_STORAGE","1GB"))

MICTLANX_ROUTER_SERVICE_NAME = os.environ.get("MICTLANX_ROUTER_SERVICE_NAME","mictlanx-router")
MICTLANX_JAEGER_ENDPOINT     = os.environ.get("MICTLANX_JAEGER_ENDPOINT","http://localhost:4318")
MICTLANX_ZIPKIN_ENDPOINT     = os.environ.get("MICTLANX_ZIPKIN_ENDPOINT","http://localhost:9411")
MICTLANX_ROUTER_OPENTELEMETRY = bool(int(os.environ.get("MICTLANX_ROUTER_OPENTELEMTRY",0)))

if MICTLANX_CACHE:
    cache = ChX.CacheFactory.create(
        eviction_policy=MICTLANX_CACHE_EVICTION_POLICY,
        capacity=MICTLANX_CACHE_CAPACITY,
        capacity_storage=MICTLANX_CACHE_CAPACITY_STORAGE
    )
else:
    cache = ChX.NoCache()
    # if MICTLANX_CACHE_EVICTION_POLICY =="LRU"

resource = Resource(
    attributes={
        SERVICE_NAME: MICTLANX_ROUTER_SERVICE_NAME
    }
)
trace_provider = TracerProvider(resource=resource)
trace.set_tracer_provider(trace_provider)
tracer = trace.get_tracer(MICTLANX_ROUTER_SERVICE_NAME)
if not MICTLANX_ROUTER_OPENTELEMETRY:
    trace_provider.add_span_processor(SimpleSpanProcessor(NoOpSpanExporter()))
else:
    # ===========================================================
    # JAEGER
    # ===========================================================
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="{}/v1/traces".format(MICTLANX_JAEGER_ENDPOINT)))
    trace_provider.add_span_processor(processor)

    # ===========================================================
    # Zipkin_EXPORTER
    # ===========================================================
    zipkin_exporter = ZipkinExporter(
        endpoint="{}/api/v2/spans".format(MICTLANX_ZIPKIN_ENDPOINT),
        # transport_format="json",
    )
    span_processor = BatchSpanProcessor(zipkin_exporter)
    trace_provider.add_span_processor(span_processor)


# ===========================================================
# CONSOLE_SPAN_EXPORTER
# ===========================================================
if MICTLANX_DEBUG:
    processor = BatchSpanProcessor(ConsoleSpanExporter())
    trace_provider.add_span_processor(processor)


# ===========================================================
# END OPEN 
# ===========================================================


LOG_PATH                    = os.environ.get("LOG_PATH","/log")
# 
MICTLANX_ROUTER_HOST                     = os.environ.get("MICTLANX_ROUTER_HOST","localhost")
MICTLANX_ROUTER_PORT                     = int(os.environ.get("MICTLANX_ROUTER_PORT","60666"))
MICTLANX_ROUTER_MAX_WORKERS              = int(os.environ.get("MAX_WORKERS","4"))
MICTLANX_ROUTER_LB_ALGORITHM             = os.environ.get("MICTLANX_ROUTER_LB_ALGORITHM","ROUND_ROBIN") # ROUND_ROBIN
MICTLANX_ROUTER_LOG_NAME                 = os.environ.get("MICTLANX_ROUTER_LOG_NAME","mictlanx-router-0")
MICTLANX_ROUTER_LOG_INTERVAL             = int(os.environ.get("MICTLANX_ROUTER_LOG_INTERVAL","24"))
MICTLANX_ROUTER_LOG_WHEN                 = os.environ.get("MICTLANX_ROUTER_LOG_WHEN","h")
MICTLANX_ROUTER_LOG_SHOW                 = bool(int(os.environ.get("MICTLANX_ROUTER_LOG_SHOW","1")))
MICTLANX_ROUTER_REPLICATOR_QUEUE_MAXSIZE = int(os.environ.get("MICTLANX_ROUTER_REPLICATOR_QUEUE_MAXSIZE","100"))
MICTLANX_ROUTER_SYNC_FAST                = bool(int(os.environ.get("MICTLANX_ROUTER_SYNC_FAST","1")))
MICTLANX_ROUTER_MAX_PEERS                = int(os.environ.get("MICTLANX_ROUTERMAX_PEERS","10"))
MICTLANX_ROUTER_MAX_TTL                  = int(os.environ.get("MICTLANX_ROUTER_MAX_TTL","3"))
MICTLANX_ROTUER_AVAILABLE_NODES          = os.environ.get("MICTLANX_ROUTER_AVAILABLE_NODES","0;1;2;3;4;5;6;7;8;9;10").split(";")
MICTLANX_ROUTER_SHOW_REPLICATOR_LOGS     = bool(int(os.environ.get("MICTLANX_ROUTER_SHOW_REPLICATOR_LOGS","1")))
MICTLANX_ROUTER_MAX_TRIES                = int(os.environ.get("MICTLANX_ROUTER_MAX_TRIES","60"))
MICTLANX_ROUTER_MAX_TASKS                = int(os.environ.get("MICTLANX_ROUTER_MAX_TASKS","100"))
MICTLANX_ROUTER_MAX_CONCURRENCY          = int(os.environ.get("MICTLANX_ROUTER_MAX_CONCURRENCY","5"))
MICTLANX_ROUTER_MAX_PEERS_RF             = int(os.environ.get("MICTLANX_ROUTER_MAX_PEERS_RF","5"))
MICTLANX_ROUTER_NETWORK_ID               = os.environ.get("MICTLANX_ROUTER_NETWORK_ID","mictlanx")
# Storage peer manager
MICTLANX_SPM_SHOW_LOGS                       = bool(int(os.environ.get("MICTLANX_SPM_SHOW_LOGS","1")))
MICTLANX_SPM_DEBUG                           = bool(int(os.environ.get("MICTLANX_SPM_DEBUG","1")))
MICTLANX_SPM_MAX_TRIES                       = int(os.environ.get("MICTLANX_SPM_MAX_TRIES","10"))
MICTLANX_SPM_MAX_TIMEOUT_TO_RECOVER          = os.environ.get("MICTLANX_SPM_MAX_TIMEOUT_TO_RECOVER","10s")
MICTLANX_SPM_PHYSICAL_NODES_SEPARATOR        = os.environ.get("MICTLANX_SPM_PHYSICAL_NODES_SEPARATOR",";")
MICTLANX_SPM_PHYSICAL_NODES_INDEXES          = list(map(int,os.environ.get("MICTLANX_SPM_PHYSICAL_NODES_INDEXES","0;2;3;4;5").split(MICTLANX_SPM_PHYSICAL_NODES_SEPARATOR)))
MICTLANX_SPM_MAX_IDLE_TIME                   = os.environ.get("MICTLANX_SPM_MAX_IDLE_TIME","5s")
MICTLANX_SPM_QUEUE_TICK_TIMEOUT              = os.environ.get("MICTLANX_SPM_QUEUE_TICK_TIMEOUT","2s")
MICTLANX_SPM_PEER_CONFIG_PATH                = os.environ.get("MICTLANX_SPM_PEER_CONFIG_PATH","/home/nacho/Programming/Python/mictlanx-router/peers-dev.json")
MICTLANX_SPM_PEER_CONFIG_MAX_TIMEOUT_TO_SAVE = os.environ.get("MICTLANX_SPM_PEER_CONFIG_MAX_TIMEOUT_TO_SAVE","5m")
MICTLANX_SPM_MAX_RECOVER_TIME_UTIL_RESTART   = os.environ.get("MICTLANX_SPM_MAX_RECOVER_TIME_UTIL_RESTART","10s")
MICTLANX_SPM_MAX_TIMEOUT_PEERS_HANDSHAKE     = os.environ.get("MICTLANX_SPM_MAX_TIMEOUT_PEERS_HANDSHAKE","1m")
MICTLANX_SPM_PEERS_BASE_PORT                 = int(os.environ.get("MICTLANX_SPM_PEERS_BASE_PORT","25000"))
MICTLANX_SPM_PEERS_BASE_PROTOCOL             = os.environ.get("MICTLANX_SPM_PEERS_BASE_PROTOCOL","http")
MICTLANX_SPM_PEERS_ELASTIC                   = bool(int(os.environ.get("MICTLANX_SPM_PEERS_ELASTIC","1")))
MICTLANX_SPM_PEERS_DOCKER_IMAGE              = os.environ.get("MICTLANX_SPM_PEERS_DOCKER_IMAGE","nachocode/mictlanx:peer-0.0.159")
# Replica manager
MICTLANX_RM_SHOW_LOGS           = bool(int(os.environ.get("MICTLANX_RM_SHOW_LOGS","1")))
MICTLANX_RM_ELASTIC             = bool(int(os.environ.get("MICTLANX_RM_ELASTIC","1")))
MICTLANX_RM_QUEUE_MAX_IDLE_TIME = os.environ.get("MICTLANX_RM_QUEUE_MAX_IDLE_TIME","1h")
MICTLANX_RM_BATCH_SIZE          = int(os.environ.get("MICTLANX_RM_BATCH_SIZE", "1000"))
MICTLANX_RM_HEARTBEAT           = os.environ.get("MICTLANX_RM_HEARTBEAT", "2s")
# Replicator
MICTLANX_DATA_REPLICATOR_QUEUE_HEARTBEAT  = os.environ.get("MICTLANX_REPLICATOR_QUEUE_HEARTBEAT","1min")
MICTLANX_DATA_REPLICATOR_MAX_IDLE_TIMEOUT = os.environ.get("MICTLANX_DATA_REPLICATOR_MAX_IDLE_TIMEOUT","1hr")
# Client
# MICTLANX_PEERS_STR   = os.environ.get("MICTLANX_PEERS","mictlanx-peer-0:localhost:25000 mictlanx-peer-1:localhost:25001 mictlanx-peer-2:localhost:25002")
MICTLANX_PEERS_STR   = os.environ.get("MICTLANX_PEERS","mictlanx-peer-0:localhost:24000")
MICTLANX_PROTOCOL    = os.environ.get("MICTLANX_PROCOTOL","http")
MICTLANX_PEERS       = [] if MICTLANX_PEERS_STR == "" else list(Utils.peers_from_str_v2(peers_str=MICTLANX_PEERS_STR,separator=" ", protocol=MICTLANX_PROTOCOL,) )
MICTLANX_API_VERSION = os.environ.get("MICTLANX_API_VERSION","4")
MICTLANX_TIMEOUT     = int(os.environ.get("MICTLANX_TIMEOUT","3600"))
# Summoner
MICTLANX_SUMMONER_API_VERSION = Some(int(os.environ.get("MICTLANX_SUMMONER_API_VERSION","3")))
MICTLANX_SUMMONER_IP_ADDR     = os.environ.get("MICTLANX_SUMMONER_IP_ADDR","localhost")
MICTLANX_SUMMONER_PORT        = int(os.environ.get("MICTLANX_SUMMONER_PORT","15000"))
MICTLANX_SUMMONER_MODE        = os.environ.get("MICTLANX_SUMMONER_MODE","docker")
MICTLANX_SUMMONER_SUBNET      = os.environ.get("MICTLANX_SUMMONER_SUBNET","10.0.0.0/25")
# ===========================================================
def signal_handler(sig, frame):
    global log
    log.warning('Shutting down gracefully...')
    # for task in asyncio.all_tasks(loop):
        # task.cancel()
    # loop.stop()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
# ===========================================================

log                         = Log(
        name                   = MICTLANX_ROUTER_LOG_NAME,
        console_handler_filter = lambda x: MICTLANX_ROUTER_LOG_SHOW,
        interval               = MICTLANX_ROUTER_LOG_INTERVAL,
        when                   = MICTLANX_ROUTER_LOG_WHEN,
        path                   = LOG_PATH
)
# _________________________________________

replica_map_rwlock = aiorwlock.RWLock(fast=MICTLANX_ROUTER_SYNC_FAST )
peer_healer_rwlock = aiorwlock.RWLock(fast=MICTLANX_ROUTER_SYNC_FAST )

tm = DefaultTaskManager()

summoner        = Summoner(
    ip_addr     = MICTLANX_SUMMONER_IP_ADDR, 
    port        = MICTLANX_SUMMONER_PORT, 
    api_version = MICTLANX_SUMMONER_API_VERSION,
    network= Some(IPv4Network(
        MICTLANX_SUMMONER_SUBNET
     ))
)

# ============= Storage Peer Manager ============================ 
storage_peer_manager = StoragePeerManager(
    tracer    = tracer,
    q         = Queue(maxsize=100),
    peers     = MICTLANX_PEERS,
    name      = "mictlanx-peer-manager-0",
    show_logs = MICTLANX_SPM_SHOW_LOGS,
    summoner  = summoner,
    params= Some(StoragePeerManagerParams(
        summoner_mode                   = MICTLANX_SUMMONER_MODE,
        base_port                       = MICTLANX_SPM_PEERS_BASE_PORT,
        base_protocol                   = MICTLANX_SPM_PEERS_BASE_PROTOCOL,
        debug                           = MICTLANX_SPM_DEBUG,
        max_idle_time                   = MICTLANX_SPM_MAX_IDLE_TIME,
        max_recover_time_until_restart  = MICTLANX_SPM_MAX_RECOVER_TIME_UTIL_RESTART,
        max_retries                     = MICTLANX_ROUTER_MAX_TRIES,
        max_timeout_to_recover          = MICTLANX_SPM_MAX_TIMEOUT_TO_RECOVER,
        max_timeout_to_save_peer_config = MICTLANX_SPM_PEER_CONFIG_MAX_TIMEOUT_TO_SAVE,
        peers_config_path               = MICTLANX_SPM_PEER_CONFIG_PATH,
        physical_nodes_indexes          = MICTLANX_SPM_PHYSICAL_NODES_INDEXES,
        queue_tick_timeout              = MICTLANX_SPM_QUEUE_TICK_TIMEOUT,
        peer_elastic                    = MICTLANX_SPM_PEERS_ELASTIC,
        peer_docker_image               = MICTLANX_SPM_PEERS_DOCKER_IMAGE,
        max_timeout_peers_handshake     = MICTLANX_SPM_MAX_TIMEOUT_PEERS_HANDSHAKE,
    ))
)
# ============= Replica Manager ============================ 

rm_q            = asyncio.Queue()
replica_manager = ReplicaManager(
    tracer    = tracer,
    q         = rm_q,
    spm       = storage_peer_manager,
    elastic   = MICTLANX_RM_ELASTIC,
    show_logs = MICTLANX_RM_SHOW_LOGS,
    params= Some(
        ReplicaManagerParams(
            batch_index=0,
            batch_size=MICTLANX_RM_BATCH_SIZE,
            current_end_index=-1,
            current_start_index=-1,
            elastic=MICTLANX_RM_ELASTIC,
            heartbeat_t=MICTLANX_RM_HEARTBEAT,
            local_n_balls_map={},
            queue_max_idle_timeout=MICTLANX_RM_QUEUE_MAX_IDLE_TIME,
            total_n_balls=-1,
        )
    )
)
# ============= Data replicator ============================ 
data_replicator_q = asyncio.Queue()
data_replicator = DataReplicator(tracer=tracer,queue= data_replicator_q, rm = replica_manager)




 
log.debug({
    "event":"MICTLANX_ROUTER_STARTED",
    "MICTLANX_ROUTER_HOST":MICTLANX_ROUTER_HOST,
    "MICTLANX_ROUTER_PORT":MICTLANX_ROUTER_PORT,
    "MICTLANX_JEAGER_ENDPOINT":MICTLANX_JAEGER_ENDPOINT,
    "MICTLANX_ZIPKIN_ENDPOINT":MICTLANX_ZIPKIN_ENDPOINT,
    "LOG_PATH": LOG_PATH,
    "MICTLANX_ROUTER_LOG_NAME": MICTLANX_ROUTER_LOG_NAME,
    "MICTLANX_ROUTER_LOG_INTERVAL": MICTLANX_ROUTER_LOG_INTERVAL,
    "MICTLANX_ROUTER_LOG_WHEN": MICTLANX_ROUTER_LOG_WHEN,
    "MICTLANX_ROUTER_LOG_SHOW": MICTLANX_ROUTER_LOG_SHOW,
    "MICTLANX_ROUTER_REPLICATOR_QUEUE_MAXSIZE": MICTLANX_ROUTER_REPLICATOR_QUEUE_MAXSIZE,
    "MICTLANX_ROUTER_SYNC_FAST": MICTLANX_ROUTER_SYNC_FAST,
    "MICTLANX_ROUTER_LB_ALGORITHM": MICTLANX_ROUTER_LB_ALGORITHM,
    "MICTLANX_ROUTER_PEER_BASE_PORT": MICTLANX_SPM_PEERS_BASE_PORT,
    "MICTLANX_ROUTER_MAX_PEERS":MICTLANX_ROUTER_MAX_PEERS,
    "MICTLANX_ROUTER_MAX_TTL": MICTLANX_ROUTER_MAX_TTL,
    "MICTLANX_ROUTER_AVAILABLE_NODES": ";".join(MICTLANX_ROTUER_AVAILABLE_NODES),
    "MICTLANX_ROUTER_SHOW_PEER_HEALER_LOGS": MICTLANX_SPM_SHOW_LOGS,
    "MICTLANX_ROUTER_SHOW_REPLICATOR_LOGS": MICTLANX_ROUTER_SHOW_REPLICATOR_LOGS,
    "MICTLANX_ROUTER_MAX_TRIES": MICTLANX_ROUTER_MAX_TRIES,
    "MICTLANX_PEERS": MICTLANX_PEERS_STR,
    "MICTLANX_PROTOCOL": MICTLANX_PROTOCOL,
    "MICTLANX_API_VERSION": MICTLANX_API_VERSION,
    "MICTLANX_TIMEOUT": MICTLANX_TIMEOUT,
})

# ______________________________

@asynccontextmanager
async def lifespan(app:FastAPI):
    t1 = asyncio.create_task(
        run_async_healer(
            ph=storage_peer_manager,
        )
    )
    t0 = asyncio.create_task(run_rm(
        rm = replica_manager,
    ))
    await storage_peer_manager.q.put(1)
    t2 = asyncio.create_task(run_data_replicator(
        data_replicator = data_replicator,
        heartbeat= MICTLANX_DATA_REPLICATOR_QUEUE_HEARTBEAT,
        max_idle_timeout= MICTLANX_DATA_REPLICATOR_MAX_IDLE_TIMEOUT
    ) )
    yield
    t0.cancel()
    t1.cancel()
    t2.cancel()

app = FastAPI(
    root_path=os.environ.get("OPENAPI_PREFIX","/mictlanx-router-0"),
    lifespan=lifespan
)
# Open telemetry
FastAPIInstrumentor.instrument_app(app)

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
peers_controller           = Cx.PeersController(log = log, storage_peer_manager=storage_peer_manager,tracer=tracer)
data_replicator_controller = Cx.DataReplicatorController(log =log, data_replicator=data_replicator)
tasks_controller           = Cx.TasksController(log = log, tm =tm)
rm_controller              = Cx.ReplicaManagerController(log = log, replica_manager=replica_manager)
buckets_controller         = Cx.BucketsController(log = log, tracer= tracer, replica_manager=replica_manager, storage_peer_manager=storage_peer_manager, tm = tm, cache = cache)
cache_controller = Cx.CacheControllers(log = log, cache =cache)
app.include_router(peers_controller.router)
app.include_router(data_replicator_controller.router)
app.include_router(tasks_controller.router)
app.include_router(rm_controller.router)
app.include_router(buckets_controller.router)
app.include_router(cache_controller.router)

# ======================= Replication ============================



@app.post("/api/v4/u/buckets/{bucket_id}/{key}")
async def update_metadata(
    bucket_id:str,
    key:str,
    metadata:Metadata, 
):
    with tracer.start_as_current_span("update.metadata") as span:
        span:Span      = span  
        current_replicas = await replica_manager.get_current_replicas_ids(bucket_id=bucket_id,key=key)
        xs       =  await storage_peer_manager.from_peer_ids_to_peers(current_replicas)
        success_replicas = len(xs)
        for peer in xs:
            result = peer.put_metadata(
                bucket_id=bucket_id,
                key=key,
                ball_id= metadata.ball_id,
                checksum=metadata.checksum,
                content_type=metadata.content_type,
                headers={"update":"1"},
                is_disable=metadata.is_disabled,
                producer_id=metadata.producer_id,
                size=metadata.size,
                tags=metadata.tags,
            )
            if result.is_err:
                success_replicas-= 1

        
        if success_replicas == 0:
            raise HTTPException(status_code=500,detail="")
        return Response(content=None, status_code=204)


            # print()


        
        # for peer in 



# ======================= PUT ============================

@app.post("/api/v4/buckets/data/{task_id}/chunked")
async def put_data_chunked(
    task_id:str,
    request: Request,
):
    with tracer.start_as_current_span("put.chunked") as span:
        span:Span = span
        start_time = T.time()
        try:
            # ======================= GET TASK ============================
            gt_start_time = T.time_ns()
            task_maybe = await tm.get_task(task_id=task_id)
            if task_maybe.is_err:
                raise task_maybe.unwrap_err()
            
            span.add_event(name="get.task",attributes={"task_id":task_id}, timestamp=gt_start_time)
            task = task_maybe.unwrap()
            span.set_attributes({
                "task_id":task_id,
                **task.__dict__
            })

            # ======================= GET PEER ============================
            gt_start_time = T.time_ns()
            maybe_peer = await storage_peer_manager.get_peer_by_id(peer_id=task.peers)
            if maybe_peer.is_none:
                raise HTTPException(status_code=404, detail="No peers available")
            else:
                peer = maybe_peer.unwrap()
            span.add_event(name="get.peer",attributes={"task_id":task_id, "peer_id":task.peers}, timestamp=gt_start_time)

            # ======================= PUT_CHUNKS ============================
            gt_start_time = T.time_ns()
            headers= {}
            chunks = request.stream()
            result = await peer.put_chuncked_async(task_id=task_id, chunks = chunks,headers=headers)
            if result.is_err:
                raise result.unwrap_err()

            response = result.unwrap()
            span.add_event(name="put.chunked",attributes={"task_id":task_id, "peer_id":task.peers}, timestamp=gt_start_time)
            # ======================= DELETE TASK ============================
            gt_start_time = T.time_ns()
            res = await tm.delete_task(task_id=task_id)
            span.add_event(name="delete.task",attributes={"task_id":task_id}, timestamp=gt_start_time)
            # 
            log.info({
                "event":"PUT.DATA",
                "x_timestamp":T.time_ns(),
                "bucket_id":task.bucket_id,
                "key":task.key,
                "size":task.size,
                "task_id":task_id,
                "peer_id":task.peers,
                "content_type":task.content_type,
                "deleted_tasks":res.is_ok,
                "response_time":T.time() - start_time
            })

            return JSONResponse(content=jsonable_encoder(response))

        except HTTPException as e:
            log.error({
                "event":"HTTP.EXCEPTION",
                "detail":e.detail,
                "status_code":e.status_code
            })
            raise HTTPException(status_code=500, detail = str(e))
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
            raise HTTPException(status_code=500, detail = str(e))



# > ======================= PUT ============================

# ======================= GET ============================

#> ======================= GET ============================
@app.post("/api/v4/elastic")
async def elastic(
    elastic_payload:PeerElasticPayload
):
    start_time      = T.time()
    current_n_peers = await storage_peer_manager.get_n_available_peers()
    _rf = 0 if current_n_peers >= elastic_payload.rf else elastic_payload.rf - current_n_peers
    if elastic_payload.strategy == "ACTIVE":
        res = await storage_peer_manager.active_deploy_peers(
            disk=elastic_payload.disk,
            cpu=elastic_payload.cpu,
            memory=elastic_payload.memory,
            network_id= MICTLANX_ROUTER_NETWORK_ID,
            rf  = _rf, 
            # elastic_payload.rf if elastic_payload.rf <= MICTLANX_ROUTER_MAX_PEERS_RF else MICTLANX_ROUTER_MAX_PEERS_RF,
            workers=elastic_payload.workers,
            elastic=elastic_payload.elastic,
        )
    elif elastic_payload.strategy == "PASSIVE":
        pass
    else:
        res = await storage_peer_manager.active_deploy_peers(
            disk=elastic_payload.disk,
            cpu=elastic_payload.cpu,
            memory=elastic_payload.memory,
            network_id= MICTLANX_ROUTER_NETWORK_ID,
            rf= elastic_payload.rf if elastic_payload.rf <= MICTLANX_ROUTER_MAX_PEERS_RF else MICTLANX_ROUTER_MAX_PEERS_RF,
            workers=elastic_payload.workers,
            elastic=elastic_payload.elastic,
        )
        
    response_time = T.time() - start_time
    log.info({
        "event":"PEER.REPLICATION",
        **elastic_payload.__dict__,
        "rf":_rf,
        "response_time":response_time
    })
    return JSONResponse(
        content=jsonable_encoder(res.__dict__)
    )




@app.delete("/api/v4/buckets/{bucket_id}/{key}")
async def delete_data_by_key(
    bucket_id:str,
    key:str,
):
    with tracer.start_as_current_span("delete") as span:
        span:Span = span
        span.set_attributes({"bucket_id":bucket_id, "key":key})
        _bucket_id = Utils.sanitize_str(x=bucket_id)
        _key = Utils.sanitize_str(x=key)
        try:

            start_time= T.time()
            gcr_timestamp = T.time_ns()
            peers = await replica_manager.get_current_replicas(bucket_id=bucket_id,key=key)
            span.add_event(name="get.replicas",attributes={},timestamp=gcr_timestamp)
            default_delete_by_key_response = DeletedByKeyResponse(n_deletes=0,key=_key)
            for peer in  peers:
                timestamp = T.time_ns()
                result = peer.delete(bucket_id=_bucket_id, key= _key, timeout=30)
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
                        span.add_event(name="{}.delete".format(peer.peer_id), timestamp=timestamp)
            response_time = T.time() - start_time
            timestamp     = T.time_ns()
            res           = await replica_manager.remove_replicas(bucket_id=bucket_id,key=key)
            span.add_event(name="remove.replicas", attributes={}, timestamp=timestamp)
            log.info({
                "event":"DELETED.BY.KEY",
                "bucket_id":_bucket_id,
                "key":_key,
                "replicas":res,
                "n_deletes":default_delete_by_key_response.n_deletes,
                "response_time":response_time,
            })
            
            return JSONResponse(content=jsonable_encoder(default_delete_by_key_response.model_dump()))
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

    with tracer.start_as_current_span("delete") as span:
        span:Span = span 
        _bucket_id = Utils.sanitize_str(x= bucket_id)
        _ball_id = Utils.sanitize_str(x= ball_id)

        start_time= T.time()

        gcr_timestamp = T.time_ns()
        peers = await replica_manager.get_current_replicas(bucket_id=bucket_id,key=_ball_id)
        span.add_event(name="get.replicas",attributes={},timestamp=gcr_timestamp)
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
                        timestamp = T.time_ns()
                        if i ==0:
                            combined_key = "{}@{}".format(metadata.bucket_id,metadata.key)

                        del_result = peer.delete(bucket_id=_bucket_id, key=metadata.key)
                        service_time = T.time() - start_time
                        if del_result.is_ok:
                            del_response = del_result.unwrap()
                            if del_response.n_deletes>=0:
                                default_del_by_ball_id_response.n_deletes+= del_response.n_deletes
                        span.add_event(
                            name       = "{}.{}.delete".format(peer.peer_id, metadata.key),
                            attributes = {"bucket_id":bucket_id, "key":metadata.key},
                            timestamp  = timestamp
                        )
                        
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
            log.info({
                "event":"DELETED.BY.BALL_ID",
                "bucket_id":_bucket_id,
                "ball_id":_ball_id,
                "n_deletes": default_del_by_ball_id_response.n_deletes,
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
        # async with peer_healer_rwlock.reader_lock:
            # peers = storage_peer_manager.peers
        peers = await replica_manager.get_current_replicas(bucket_id=bucket_id,key=key)
        
        for peer in peers:
            res = peer.disable(bucket_id=bucket_id,key=key,headers=headers)
        log.info({
            "event":"DISABLE.COMPLETED",
            "bucket_id":bucket_id,
            "key":key,
            "replicas":len(peers),
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
        ps = await storage_peer_manager.get_available_peers()
        for peer in os:
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


async def data_by_chunks(data:UploadFile,chunk_size:str="1MB"):
    _chunk_size= HF.parse_size(chunk_size)
    while True:
        chunk = await data.read(_chunk_size)
        if not chunk:
            break
        yield chunk

@app.get("/api/v4/buckets/{bucket_id}/{key}/size")
async def get_size_by_key(bucket_id:str,key:str):
    global peer_healer_rwlock
    try:
        # async with peer_healer_rwlock.reader_lock:
        #     peers = storage_peer_manager.peers
        
        peers = await replica_manager.get_current_replicas(bucket_id=bucket_id,key=key)
        responses = []
        for p in peers:
            result = p.get_size(bucket_id=bucket_id,key=key, timeout = MICTLANX_TIMEOUT)
            if result.is_ok:
                response = result.unwrap()
                responses.append(response)

        return JSONResponse(content = jsonable_encoder(responses))
    except Exception as e:
        raise HTTPException(status_code = 500, detail="Uknown error: {}@{}".format(bucket_id,key))

@app.get("/api/v4/accessmap")
async def get_access_map(start:int=0, end:int =0,no_access:bool = False):
    try:
        x = await replica_manager.get_access_replica_map()
        if end <=0:
            _end = len(x)
        else:
            _end = end
        
        if start >= _end:
            _start=0
        else :
            _start = start
        

        filtered_x = filter(lambda b: b[1] > 0 or no_access ,x.items())
        xs = dict(list(  filtered_x   )[_start:_end] )
        xs = dict(sorted(xs.items(), key=lambda x:x[1]))
        
        return JSONResponse(
            content=jsonable_encoder( xs  )
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))





# Support endpoints
@app.post("/mtx/u/rm")
async def rm_update_params(req:Request):
    try:
        json_body = await req.json()
        await replica_manager.update_params(**json_body)
        params = await replica_manager.get_params()
        return JSONResponse(
            content= jsonable_encoder(
                params
            )
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))   
@app.post("/mtx/u/spm")
async def spm_update_params(req:Request):
    try:
        json_body = await req.json()
        await storage_peer_manager.update_params(**json_body)
        params = await storage_peer_manager.get_params()
        return JSONResponse(
            content=jsonable_encoder(
                params
            )
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))   







Instrumentator().instrument(app).expose(app, endpoint="/metrics")
