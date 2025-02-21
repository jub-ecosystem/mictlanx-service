import os
import sys
import requests as R
import time as T
import humanfriendly as HF
import aiorwlock
import signal
from asyncio.queues import Queue
import asyncio
from fastapi import FastAPI
#
from contextlib import asynccontextmanager
from fastapi.openapi.utils import get_openapi
from fastapi.middleware.cors import CORSMiddleware
from option import Some
from fastapi.middleware.gzip import GZipMiddleware
from ipaddress import IPv4Network
# 
import mictlanxrouter.caching as ChX
import mictlanxrouter.controllers as Cx
from mictlanxrouter.replication_manager import ReplicaManager,ReplicaManagerParams,DataReplicator
from mictlanxrouter.peer_manager.healer import StoragePeerManager,StoragePeerManagerParams
# 
from mictlanxrouter.garbage_collector import GarbageCollectorX
from mictlanx.logger.log import Log
from mictlanx.v4.summoner.summoner import  Summoner
from mictlanx.utils.index import Utils
from mictlanx.interfaces import AsyncPeer
from mictlanxrouter._async import run_async_storage_peer_manager,run_rm,run_data_replicator
from mictlanxrouter.tasksx import DefaultTaskManager
from mictlanxrouter.opentelemetry import NoOpSpanExporter

# ===========================================================
# Opentelemetry
# ===========================================================
# Prometheus
from prometheus_client import Counter
from prometheus_fastapi_instrumentator import Instrumentator
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry import trace


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
MICTLANX_PEERS_STR   = os.environ.get("MICTLANX_PEERS","mictlanx-peer-0:localhost:24000 mictlanx-peer-1:localhost:24001")
MICTLANX_PROTOCOL    = os.environ.get("MICTLANX_PROCOTOL","http")
MICTLANX_PEERS       = [] if MICTLANX_PEERS_STR == "" else list(Utils.peers_from_str_v2(peers_str=MICTLANX_PEERS_STR,separator=" ", protocol=MICTLANX_PROTOCOL,) )
MICTLANX_PEERS = list(map(lambda x:AsyncPeer(peer_id=x.peer_id, ip_addr=x.ip_addr, port=x.port, protocol=x.protocol) , MICTLANX_PEERS))
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
    network= Some(
        IPv4Network(
            MICTLANX_SUMMONER_SUBNET
        )
     )
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
        recover_tick_timeout= os.environ.get("MICTLANX_RECOVER_TICK_TIMEOUT","1m")
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


gc = GarbageCollectorX(rm = replica_manager,storage_peer_manager=storage_peer_manager,timeout= os.environ.get("MICTLANX_GC_TIMEOUT","5m"))

 
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


import fcntl
def try_acquire_lock(lock_file: str):
    """Try to acquire an exclusive non-blocking file lock.
       Returns the file descriptor if successful, otherwise None."""
    fd = os.open(lock_file, os.O_CREAT | os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BlockingIOError:
        os.close(fd)
        return None


@asynccontextmanager
async def lifespan(app:FastAPI):
    
    # lock_fd = try_acquire_lock(lock_file="/mictlanx/local/x.lock")
    # print("LOCK", lock_fd)
    # if lock_fd:

    access_map_worker_task = replica_manager.run_acces_map_queue()
    spm_main_worker_task   =  storage_peer_manager.run_main_queue()
    gc_task                = asyncio.create_task(gc.run())
    storage_peer_manager_task = asyncio.create_task(
        run_async_storage_peer_manager(
            ph=storage_peer_manager,
        )
    )
    rm_task = asyncio.create_task(run_rm(
        rm = replica_manager,
    ))
    await storage_peer_manager.q.put(1)
    t2 = asyncio.create_task(run_data_replicator(
        data_replicator = data_replicator,
        heartbeat= MICTLANX_DATA_REPLICATOR_QUEUE_HEARTBEAT,
        max_idle_timeout= MICTLANX_DATA_REPLICATOR_MAX_IDLE_TIMEOUT
    ) )

    handshake_daemon = asyncio.create_task(storage_peer_manager.handshake_worker())
    
    recover_daemon = asyncio.create_task(storage_peer_manager.recover_daemon())
    
    yield
    access_map_worker_task.cancel()
    spm_main_worker_task.cancel()
    gc_task.cancel()
    recover_daemon.cancel()
    handshake_daemon.cancel()
    rm_task.cancel()
    storage_peer_manager_task.cancel()
    t2.cancel()
    # else:
        # yield

peers_controller           = Cx.PeersController(log = log, storage_peer_manager=storage_peer_manager,tracer=tracer, network_id=MICTLANX_ROUTER_NETWORK_ID, max_peers_rf=MICTLANX_ROUTER_MAX_PEERS_RF)
data_replicator_controller = Cx.DataReplicatorController(log =log, data_replicator=data_replicator)
tasks_controller           = Cx.TasksController(log = log, tm =tm)
rm_controller              = Cx.ReplicaManagerController(log = log, replica_manager=replica_manager)
buckets_controller         = Cx.BucketsController(log = log, tracer= tracer, replica_manager=replica_manager, storage_peer_manager=storage_peer_manager, tm = tm, cache = cache)
cache_controller = Cx.CacheControllers(log = log, cache =cache)

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
app.include_router(peers_controller.router)
app.include_router(data_replicator_controller.router)
app.include_router(tasks_controller.router)
app.include_router(rm_controller.router)
app.include_router(buckets_controller.router)
app.include_router(cache_controller.router)






# Support endpoints



Instrumentator().instrument(app).expose(app, endpoint="/metrics")
