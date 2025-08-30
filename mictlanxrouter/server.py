import os
import sys
import signal
from contextlib import asynccontextmanager
# 
import humanfriendly as HF
from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from fastapi.middleware.cors import CORSMiddleware
from option import Some
from ipaddress import IPv4Network
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter
)
from opentelemetry.exporter.zipkin.json import ZipkinExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
#
import mictlanxrouter.caching as ChX
import mictlanxrouter.controllers as Cx
from mictlanxrouter.opentelemetry import NoOpSpanExporter
# 
from mictlanx.logger.log import Log
from mictlanx.services import  Summoner
from mictlanx.utils.uri import MictlanXURI



MICTLANX_DEBUG                           = bool(int(os.environ.get("MICTLANX_DEBUG","0")))
MICTLANX_CACHE                           = bool(int(os.environ.get("MICTLANX_CACHE",1)))
MICTLANX_CACHE_EVICTION_POLICY           = os.environ.get("MICTLANX_CACHE_EVICTION_POLICY","LRU_SM")
MICTLANX_CACHE_CAPACITY                  = int(os.environ.get("MICTLANX_CACHE_CAPACITY","100"))
MICTLANX_CACHE_CAPACITY_STORAGE          = HF.parse_size(os.environ.get("MICTLANX_CACHE_CAPACITY_STORAGE","1GB"))
MICTLANX_ROUTER_SERVICE_NAME             = os.environ.get("MICTLANX_ROUTER_SERVICE_NAME","mictlanx-router")
MICTLANX_JAEGER_ENDPOINT                 = os.environ.get("MICTLANX_JAEGER_ENDPOINT","http://localhost:4318")
MICTLANX_ZIPKIN_ENDPOINT                 = os.environ.get("MICTLANX_ZIPKIN_ENDPOINT","http://localhost:9411")
MICTLANX_ROUTER_OPENTELEMETRY            = bool(int(os.environ.get("MICTLANX_ROUTER_OPENTELEMETRY",1)))
LOG_PATH                                 = os.environ.get("LOG_PATH","/log")
MICTLANX_ROUTER_HOST                     = os.environ.get("MICTLANX_ROUTER_HOST","localhost")
MICTLANX_ROUTER_PORT                     = int(os.environ.get("MICTLANX_ROUTER_PORT","60666"))
MICTLANX_ROUTER_MAX_WORKERS              = int(os.environ.get("MAX_WORKERS","4"))
MICTLANX_ROUTER_LOG_NAME                 = os.environ.get("MICTLANX_ROUTER_LOG_NAME","mictlanx-router-0")
MICTLANX_ROUTER_LOG_INTERVAL             = int(os.environ.get("MICTLANX_ROUTER_LOG_INTERVAL","24"))
MICTLANX_ROUTER_LOG_WHEN                 = os.environ.get("MICTLANX_ROUTER_LOG_WHEN","h")
MICTLANX_ROUTER_LOG_SHOW                 = bool(int(os.environ.get("MICTLANX_ROUTER_LOG_SHOW","1")))
MICTLANX_ROUTER_REPLICATOR_QUEUE_MAXSIZE = int(os.environ.get("MICTLANX_ROUTER_REPLICATOR_QUEUE_MAXSIZE","100"))
MICTLANX_ROUTER_SYNC_FAST                = bool(int(os.environ.get("MICTLANX_ROUTER_SYNC_FAST","1")))
MICTLANX_ROUTER_MAX_PEERS                = int(os.environ.get("MICTLANX_ROUTER_MAX_PEERS","10"))
MICTLANX_ROUTER_MAX_TTL                  = int(os.environ.get("MICTLANX_ROUTER_MAX_TTL","3"))
MICTLANX_ROUTER_AVAILABLE_NODES          = os.environ.get("MICTLANX_ROUTER_AVAILABLE_NODES","0;1;2;3;4;5;6;7;8;9;10").split(";")
MICTLANX_ROUTER_SHOW_REPLICATOR_LOGS     = bool(int(os.environ.get("MICTLANX_ROUTER_SHOW_REPLICATOR_LOGS","1")))
MICTLANX_ROUTER_MAX_TRIES                = int(os.environ.get("MICTLANX_ROUTER_MAX_TRIES","60"))
MICTLANX_ROUTER_MAX_TASKS                = int(os.environ.get("MICTLANX_ROUTER_MAX_TASKS","100"))
MICTLANX_ROUTER_MAX_CONCURRENCY          = int(os.environ.get("MICTLANX_ROUTER_MAX_CONCURRENCY","5"))
MICTLANX_ROUTER_MAX_PEERS_RF             = int(os.environ.get("MICTLANX_ROUTER_MAX_PEERS_RF","5"))
MICTLANX_ROUTER_NETWORK_ID               = os.environ.get("MICTLANX_ROUTER_NETWORK_ID","mictlanx")
MICTLANX_PROTOCOL                        = os.environ.get("MICTLANX_PROCOTOL","http")
MICTLANX_API_VERSION                     = os.environ.get("MICTLANX_API_VERSION","4")
MICTLANX_PEERS_URI                       = os.environ.get("MICTLANX_PEERS_URI",f"mictlanx://mictlanx-peer-0@localhost:24000,mictlanx-peer-1@localhost:24001/?protocol={MICTLANX_PROTOCOL}&api_version={MICTLANX_API_VERSION}")
MICTLANX_PEERS                           = MictlanXURI.parse_peers(uri = MICTLANX_PEERS_URI)
MICTLANX_TIMEOUT                         = int(os.environ.get("MICTLANX_TIMEOUT","3600"))
MICTLANX_SUMMONER_API_VERSION            = Some(int(os.environ.get("MICTLANX_SUMMONER_API_VERSION","3")))
MICTLANX_SUMMONER_IP_ADDR                = os.environ.get("MICTLANX_SUMMONER_IP_ADDR","localhost")
MICTLANX_SUMMONER_PORT                   = int(os.environ.get("MICTLANX_SUMMONER_PORT","15000"))
MICTLANX_SUMMONER_MODE                   = os.environ.get("MICTLANX_SUMMONER_MODE","docker")
MICTLANX_SUMMONER_SUBNET                 = os.environ.get("MICTLANX_SUMMONER_SUBNET","10.0.0.0/25")

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
# CONSOLE_SPAN_EXPORTER
# ===========================================================
if MICTLANX_DEBUG:
    processor = BatchSpanProcessor(ConsoleSpanExporter())
    trace_provider.add_span_processor(processor)


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
    "MICTLANX_ROUTER_MAX_PEERS":MICTLANX_ROUTER_MAX_PEERS,
    "MICTLANX_ROUTER_MAX_TTL": MICTLANX_ROUTER_MAX_TTL,
    "MICTLANX_ROUTER_AVAILABLE_NODES": ";".join(MICTLANX_ROUTER_AVAILABLE_NODES),
    "MICTLANX_ROUTER_SHOW_REPLICATOR_LOGS": MICTLANX_ROUTER_SHOW_REPLICATOR_LOGS,
    "MICTLANX_ROUTER_MAX_TRIES": MICTLANX_ROUTER_MAX_TRIES,
    "MICTLANX_PEERS": MICTLANX_PEERS_URI,
    "MICTLANX_PROTOCOL": MICTLANX_PROTOCOL,
    "MICTLANX_API_VERSION": MICTLANX_API_VERSION,
    "MICTLANX_TIMEOUT": MICTLANX_TIMEOUT,
})

# ______________________________


@asynccontextmanager
async def lifespan(app:FastAPI):
    yield

peers_controller   = Cx.PeersController(log = log, tracer=tracer, network_id=MICTLANX_ROUTER_NETWORK_ID, max_peers_rf=MICTLANX_ROUTER_MAX_PEERS_RF)
buckets_controller = Cx.BucketsController(log = log,cache = cache)
cache_controller   = Cx.CacheController(log = log, cache =cache)

app = FastAPI(
    root_path = os.environ.get("OPENAPI_PREFIX","/mictlanx-router-0"),
    lifespan  = lifespan
    
)
# Open telemetry
FastAPIInstrumentor.instrument_app(app)

MICTLANX_CORS_ALLOW_ORIGINS     = os.environ.get("MICTLANX_CORS_ALLOW_ORIGINS","*")
MICTLANX_CORS_ALLOW_CREDENTILAS = bool(int(os.environ.get("MICTLANX_CORS_ALLOW_CREDENTILAS","1")))
MICTLANX_CORS_ALLOW_METHODS     = os.environ.get("MICTLANX_CORS_ALLOW_METHODS","*")
MICTLANX_CORS_ALLOW_HEADERS     = os.environ.get("MICTLANX_CORS_ALLOW_HEADERS","*")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[MICTLANX_CORS_ALLOW_ORIGINS],
    allow_credentials=MICTLANX_CORS_ALLOW_CREDENTILAS,
    allow_methods=[MICTLANX_CORS_ALLOW_METHODS],
    allow_headers=[MICTLANX_CORS_ALLOW_HEADERS]
)



MICTLANX_OPEN_API_TITLE       = os.environ.get("MICTLANX_OPEN_API_TITLE","MictlanX Router")
MICTLANX_OPEN_API_VERSION     = os.environ.get("MICTLANX_OPEN_API_VERSION","0.1.0a0")
MICTLANX_OPEN_API_SUMMARY     = os.environ.get("MICTLANX_OPEN_API_SUMMARY","MictlanX Router: Virtual storage spaces management.")
MICTLANX_OPEN_API_DESCRIPTION = os.environ.get("MICTLANX_OPEN_API_DESCRIPTION","")
MICTLANX_OPEN_API_LOGO        = os.environ.get("MICTLANX_OPEN_API_LOGO","")

def generate_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title       = MICTLANX_OPEN_API_TITLE,
        version     = MICTLANX_OPEN_API_VERSION,
        summary     = MICTLANX_OPEN_API_SUMMARY,
        description = MICTLANX_OPEN_API_DESCRIPTION,
        routes      = app.routes,
    )
    openapi_schema["info"]["x-logo"] = {
        "url": MICTLANX_OPEN_API_LOGO
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema

# 
app.openapi = generate_openapi

app.include_router(peers_controller.router)
app.include_router(buckets_controller.router)
app.include_router(cache_controller.router)



