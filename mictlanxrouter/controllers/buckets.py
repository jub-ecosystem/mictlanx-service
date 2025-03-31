import os
import asyncio
import humanfriendly as HF
from mictlanx.logger.log import Log
from fastapi import APIRouter,Header,HTTPException,UploadFile,Response,Request,Depends,BackgroundTasks
from fastapi.responses import StreamingResponse
from opentelemetry.trace import Span,Status,StatusCode,Tracer
from typing import Annotated,Union,List,Dict,Tuple,Iterator
from mictlanxrouter.dto.metadata import Metadata
import httpx
from mictlanxrouter.dto import Operations,DeletedByBallIdResponse,DeletedByKeyResponse
from option import Result,Ok,Err
import time as T
import mictlanx.v4.interfaces as InterfaceX
# .responses import PeerPutMetadataResponse,PutMetadataResponse
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
import requests  as R
from mictlanx.utils import Utils as MictlanXUtils
from nanoid import generate as nanoid
from mictlanxrouter.caching import CacheX
from mictlanxrm.client import SPMClient
from mictlanxrm.models import TaskX

class BucketsController():
    def __init__(self,
                 log:Log,
                 tracer:Tracer,
                 cache:CacheX,
                 max_timeout:str= "10s"
    ):
        self.log                  = log
        self.router               = APIRouter()
        self.tracer               = tracer
        self.cache                = cache
        self.max_timeout = HF.parse_timespan(max_timeout)
        self.spm_client = SPMClient()
        self.add_routes()

    def get_spm_client(self):
        client = SPMClient(
            hostname=os.environ.get("MICTLANX_DAEMON_HOSTNAME","localhost"),
            port= int(os.environ.get("MICTLANX_DAEMON_PORT","5555")),
            protocol= os.environ.get("MICTLANX_DAEMON_PROTOCOL","tcp")
        )
        try:
            yield client
        except Exception as e: 
            self.log.error({
                "event":"GET.SPM.CLIENT.FAILED",
                "detail":str(e)
            })
            raise e
        finally:
            client.socket.close()

    def after_operation_task(self,operation:str, bucket_id:str, key:str, t1:float):
        def __inner():
            self.log.debug({
                "operation":operation,
                "bucket_id":bucket_id,
                "key":key,
                "response_time":T.time() - t1
            })
        return __inner
            # print("GET")
            # self.spm_client


    def add_routes(self):

        @self.router.get("/api/v4/buckets/{bucket_id}/{key}/size")
        async def get_size_by_key(bucket_id:str,key:str, spm_client:SPMClient = Depends(self.get_spm_client) ):
            try:
                peers_result = await spm_client.get_replicas(bucket_id=bucket_id,key=key)
                if peers_result.is_err:
                    raise HTTPException(status_code=500, detail=str(peers_result.unwrap_err()))
                replicas = peers_result.unwrap()
                responses = []

                ps = [ p.to_async_peer() for p in replicas.replicas]
                for p in ps:
                    result = await p.get_size(bucket_id=bucket_id,key=key, timeout = self.max_timeout)
                    if result.is_ok:
                        response = result.unwrap()
                        responses.append(response)

                return JSONResponse(content = jsonable_encoder(responses))
            except Exception as e:
                self.log.error({
                    "event":"GET.SIZE.FAILED",
                    "detail":str(e)
                })
                raise HTTPException(status_code = 500, detail="Uknown error: {}@{}".format(bucket_id,key))
        
        @self.router.get("/api/v4/buckets/{bucket_id}/metadata")
        async def get_bucket_metadata(bucket_id:str, spm_client:SPMClient = Depends(self.get_spm_client)):
            with self.tracer.start_as_current_span("get.bucket.metdata") as span:
                span:Span = span
                try:
                    start_time    = T.time()
                    gap_timestamp = T.time_ns()
                    peers_result         = await spm_client.get_peers()
                    if peers_result.is_err:
                        raise HTTPException(detail=str(peers_result.unwrap_err()), status=500)
                    peers = [ p.to_async_peer() for p in peers_result.unwrap().available]

                    span.add_event(name="get.available.peers",
                        attributes={
                            "peers":list(map(lambda p:p.peer_id,peers))
                        }, 
                        timestamp=gap_timestamp
                    )

                    response = {
                        "bucket_id":bucket_id,
                        "peers_ids":[],
                        "balls":[]
                    }
                    current_balls:Dict[str,Tuple[int, Metadata]] = {}

                    for peer in peers:
                        timestamp = T.time_ns()
                        result = await peer.get_bucket_metadata(bucket_id=bucket_id,headers={})
                        if result.is_err:
                            continue
                        metadata = result.unwrap()
                        response["peers_ids"].append(peer.peer_id)
                        
                        for ball in metadata.balls:
                            if ball.size ==0:
                                continue
                            updated_at = int(ball.tags.get("updated_at","-1"))
                            if not ball.key in current_balls :
                                current_balls.setdefault(ball.key, (updated_at,ball))
                                continue

                            (current_updated_at,current_ball) = current_balls.get(ball.key)
                            if  updated_at > current_updated_at:
                                current_balls[ball.key] = (updated_at,ball)

                        span.add_event(name="{}.get.bucket.metadata".format(peer.peer_id), attributes={
                            "n_balls":len(metadata.balls)
                        }, timestamp=timestamp)

                        filtered_balls = list(map(lambda x: x[1][1], current_balls.items()))
                        response["balls"]+= filtered_balls

                    self.log.info({
                        "event":"GET.BUCKET.METADATA",
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
                    self.log.error({
                        "detail":detail,
                        "status_code":status_code,
                        "reason":e.response.reason,
                    })
                    raise HTTPException(status_code=status_code, detail=detail  )
                except R.exceptions.ConnectionError as e:
                    detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
                    self.log.error({
                        "detail":detail,
                        "status_code":500
                    })
                    raise HTTPException(status_code=500, detail=detail  )
                
        @self.router.get("/api/v4/buckets/{bucket_id}/metadata/{key}")
        async def get_metadata(
            bucket_id:str,
            key:str,
            consistency_model:Annotated[Union[str,None], Header()]="LB",
            peer_id:Annotated[Union[str,None], Header()]=None,
            spm_client:SPMClient = Depends(self.get_spm_client)
            
        ):
            with self.tracer.start_as_current_span("get.metadata") as span:
                try:
                    span:Span = span
                    span.set_attributes({
                        "bucket_id":bucket_id,
                        "key":key
                    })
                    start_at   = T.time_ns()
                    start_time = T.time()

                    access_start_time = T.time_ns()
                    maybe_peer = await spm_client.get(bucket_id=bucket_id,key=key)


                    if maybe_peer.is_err:
                        # self.replica_manager.q.put_nowait("")
                        detail = "No available peers"
                        self.log.error({
                            "event":"GET.METADATA.FAILED",
                            "bucket_id":bucket_id,
                            "key":key,
                            "detail":detail
                        })
                        raise HTTPException(status_code=404, detail=detail)
                    peer = maybe_peer.unwrap()
                    span.add_event(name= "get.peer",attributes={},timestamp=access_start_time)
                    metadata_result = await peer.get_metadata(bucket_id=bucket_id, key=key, headers={})
                    if metadata_result.is_err:
                        detail = "No metadata found for {}@{}".format(bucket_id,key)
                        self.log.error({
                            "event":"GET.METADATA.FAILED",
                            "bucket_id":bucket_id,
                            "key":key,
                            "detail":detail
                        })
                        raise HTTPException(status_code=404, detail=detail)
                    most_recent_metadata = metadata_result.unwrap()
                    self.log.info({
                        "event":"GET.METADATA",
                        "start_at":start_at,
                        "end_at":T.time_ns(),
                        "bucket_id":bucket_id,
                        "key":key,
                        "peer_id":most_recent_metadata.peer_id,
                        "local_peer_id":most_recent_metadata.local_peer_id,
                        "size":most_recent_metadata.metadata.size,
                        "response_time":T.time()- start_time
                    })
                    # METADATA_ACCESS_COUNTER.labels(bucket_id=bucket_id, key = key).inc()

                    return JSONResponse(content=jsonable_encoder(most_recent_metadata) )
                except R.exceptions.HTTPError as e:
                    detail = str(e.response.content.decode("utf-8") )
                    status_code = e.response.status_code
                    self.log.error({
                        "event":"HTTP.ERROR",
                        "detail":detail,
                        "status_code":status_code,
                        "reason":e.response.reason,
                    })
                    raise HTTPException(status_code=status_code, detail=detail  )
                except R.exceptions.ConnectionError as e:
                    detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
                    self.log.error({
                        "event":"CONNECTION.ERROR",
                        "detail":detail,
                        "status_code":500
                    })
                    raise HTTPException(status_code=500, detail=detail  )
                except Exception as e:
                    detail = str(e)
                    self.log.error({
                        "event":"EXCEPTION",
                        "detail":detail,
                        "status_code":500
                    })
                    raise HTTPException(status_code=500, detail=detail  )
   
        @self.router.post("/api/v4/u/buckets/{bucket_id}/{key}")
        async def update_metadata(
            bucket_id:str,
            key:str,
            metadata:Metadata, 
        ):
            with self.tracer.start_as_current_span("update.metadata") as span:
                span:Span      = span  
                current_replicas = await self.replica_manager.get_current_replicas_ids(bucket_id=bucket_id,key=key)
                xs       =  await self.storage_peer_manager.from_peer_ids_to_peers(current_replicas)
                success_replicas = len(xs)
                for peer in xs:
                    result = await peer.put_metadata(
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
    
        @self.router.post("/api/v4/buckets/{bucket_id}/metadata")
        async def put_metadata(
            bucket_id:str,
            metadata:Metadata, 
            update:Annotated[Union[str,None], Header()] = "0",
            spm_client:SPMClient = Depends(self.get_spm_client)

        ):
            with self.tracer.start_as_current_span("put.metadata") as span:
                span:Span      = span  
                arrival_time   = T.time()
                start_at       = T.time_ns()
                key             = MictlanXUtils.sanitize_str(x = metadata.key)
                bucket_id       = MictlanXUtils.sanitize_str(x = bucket_id)
                group_id        = nanoid()
                get_replicas_result = await spm_client.get_replicas(bucket_id=bucket_id, key=key)
                if get_replicas_result.is_err:
                    detail= "No available peers."
                    self.log.error({
                        "event":"NO.AVAILABLE.PEERS",
                        "bucket_id":bucket_id,
                        "key":key,
                        "detail":detail,
                        "raw_error":str(get_replicas_result.unwrap_err()),
                    })
                    span.set_status(Status(status_code=StatusCode.ERROR))
                    raise HTTPException(status_code=404, detail=detail ,headers={} )
                
                get_replicas = get_replicas_result.unwrap()

                available_peers = get_replicas.available_replicas
                if len(available_peers) ==0:
                    detail= "No available peers."
                    self.log.error({
                        "event":"NO.AVAILABLE.PEERS",
                        "bucket_id":bucket_id,
                        "key":key,
                        "detail":detail,
                        "available_peers":list(map(lambda x:x.peer_id,get_replicas.available_replicas)),
                        "repicas":list(map(lambda x:x.peer_id,get_replicas.replicas))
                    })
                    span.set_status(Status(status_code=StatusCode.ERROR))
                    raise HTTPException(status_code=404, detail=detail ,headers={} )


                headers        = {"Update":update}
                if not bucket_id == metadata.bucket_id:
                    raise HTTPException(status_code=400, detail="Bucket ID mismatch: {} != {}".format(bucket_id, metadata.bucket_id))

                try: 
                    replicas_result               = await spm_client.put(
                        bucket_id=bucket_id, key=key, size=metadata.size
                    )
                    # print("*"*20)
                    # print("REPLICAS_+ReSLT", replicas_result)
                    # print("*"*20)
                    if replicas_result.is_err:
                        detail= "Put remote metadata failed"
                        self.log.error({
                            "event":"PUT.RM.FAILED",
                            "bucket_id":bucket_id,
                            "key":key,
                            "detail":detail,
                            "available_peers":[]
                        })
                        raise HTTPException(status_code=404, detail=detail ,headers={} )

                    replica = replicas_result.unwrap()

                    inner_start_time = T.time()
                    put_metadata_result = await replica.put_metadata(
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
                    if put_metadata_result.is_err:
                        raise put_metadata_result.unwrap_err()
                    

                    put_metadata_response = put_metadata_result.unwrap()

                    task = TaskX(
                        group_id= group_id,
                        task_id=put_metadata_response.task_id,
                        operation="PUT",
                        bucket_id=bucket_id,
                        key=metadata.key,
                        peers=[replica.peer_id],
                        size=metadata.size,
                        content_type=metadata.content_type
                    )
                    add_task_result = await spm_client.put_task(task=task)
                    if add_task_result.is_err:
                        raise add_task_result.unwrap_err()
                    # tasks_ids.append(task.task_id)
                    self.log.info({
                        "event":"PUT.METADATA",
                        "bucket_id":bucket_id,
                        "key":metadata.key,
                        "size":metadata.size,
                        "peer_id":replica.peer_id,
                        "task_id":task.task_id,
                        "content_type":task.content_type,
                        "replica_factor":metadata.replication_factor,
                        "response_time":T.time()- inner_start_time
                    })

                    put_metadata_response = InterfaceX.PutMetadataResponse(
                        bucket_id=bucket_id,
                        key=key,
                        replicas=[replica.peer_id],
                        tasks_ids=[task.task_id],
                        service_time=T.time()- arrival_time,
                    )
                    return JSONResponse(content=jsonable_encoder(put_metadata_response))

                except Exception as e:
                    detail = str(e)
                    span.set_status(Status(status_code=StatusCode.ERROR))
                    span.add_event(name="Exception", attributes={"detail":detail,"status_code":500})
                    raise HTTPException(status_code=500, detail=detail)

        @self.router.post("/api/v4/buckets/data/{task_id}")
        async def put_data(
            task_id:str,
            data:UploadFile, 
            background_task:BackgroundTasks,
            peer_id:Annotated[Union[str,None], Header()]=None,
            spm_client:SPMClient = Depends(self.get_spm_client),

        ):
            with self.tracer.start_as_current_span("put.data") as span:
                span:Span = span
                start_at   = T.time_ns()
                start_time = T.time()
                
                # =========================GET.TASK=======================================
                get_task_timestamp = T.time_ns()
                maybe_task         = await spm_client.get_task(task_id=task_id)

                if maybe_task.is_err:
                    detail = "Task({}) not found".format(task_id)
                    raise HTTPException(status_code=404, detail=detail)
                task = maybe_task.unwrap()
                background_task.add_task(self.after_operation_task(operation="PUT", bucket_id=task.bucket_id, key=task.key,t1=start_time))
                if not task.operation.value == Operations.PUT.value:
                    raise HTTPException(status_code=409, detail=f"Expected task operation [PUT], but was received [{task.operation}]")

                span.add_event(name="get.task",attributes={"task_id":task_id},timestamp=get_task_timestamp)
                # =========================GET.PEER=======================================
                
                get_task_timestamp = T.time_ns()
                maybe_peer         = await spm_client.get_peer_by_id(peer_id=peer_id)
                if maybe_peer.is_err:
                    raise HTTPException(status_code=404, detail="Peer({}) is not available.".format(peer_id))
                # else:
                peer = maybe_peer.unwrap()
                span.add_event(name="get.peer",attributes={"peer_id":peer.peer_id},timestamp=get_task_timestamp)

                try:

                    # =========================READ.DATA=======================================
                    get_task_timestamp = T.time_ns()
                    value              = await data.read()
                    size               = len(value)
                    if size != task.size:
                        raise HTTPException(status_code=500, detail="Data size mistmatch {} != {}".format(size, task.size))
                    span.set_attributes({"size":size})
                    span.add_event(name="read.data",attributes={"size":size},timestamp=get_task_timestamp)
                    # =========================PUT.DATA=======================================
                    get_task_timestamp = T.time_ns()
                    result = await peer.put_data(task_id=task_id,key=task.key,value=value,content_type=task.content_type)
                    if result.is_err:
                        self.log.error({
                            "event":"PUT.DATA.FAILED",
                            "detail":str(result.unwrap_err())
                        })
                        raise HTTPException(detail = str(result.unwrap_err()), status_code= 500 )
                    span.add_event(name="peer.put.data",attributes={},timestamp=get_task_timestamp)
                    self.log.info({
                        "event":"PUT.DATA",
                        "start_at":start_at,
                        "end_at":T.time_ns(),
                        "bucket_id":task.bucket_id,
                        "key":task.key,
                        "size":task.size,
                        "task_id":task_id,
                        "peer_id":peer.peer_id,
                        "response_time":T.time()- start_time
                    })
                    get_task_timestamp = T.time_ns()
                    await spm_client.delete_task(task_id=task_id)
                    span.add_event(name="delete.task",attributes={"task_id":task_id},timestamp=get_task_timestamp)
                    return Response(content=None, status_code=201)

                except R.exceptions.HTTPError as e:
                    detail = str(e.response.content.decode("utf-8") )
                    # e.response.reason
                    status_code = e.response.status_code
                    self.log.error({
                        "detail":detail,
                        "status_code":status_code,
                        "reason":e.response.reason,
                    })
                    raise HTTPException(status_code=status_code, detail=detail  )
                except R.exceptions.ConnectionError as e:
                    detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
                    self.log.error({
                        "detail":detail,
                        "status_code":500
                    })
                    raise HTTPException(status_code=500, detail=detail  )


        @self.router.post("/api/v4/buckets/data/{task_id}/chunked")
        async def put_data_chunked(
            task_id:str,
            request: Request,
            background_task:BackgroundTasks,
            spm_client:SPMClient = Depends(self.get_spm_client)

        ):
            with self.tracer.start_as_current_span("put.chunked") as span:
                span:Span = span
                start_time = T.time()
                try:
                    # ======================= GET TASK ============================
                    gt_start_time = T.time_ns()
                    task_maybe = await spm_client.get_task(task_id=task_id)
                    if task_maybe.is_err:
                        raise task_maybe.unwrap_err()
                    
                    span.add_event(name="get.task",attributes={"task_id":task_id}, timestamp=gt_start_time)
                    task = task_maybe.unwrap()
                    background_task.add_task(self.after_operation_task(
                        operation="PUT",
                        bucket_id=task.bucket_id,
                        key= task.key,
                        t1 = start_time
                    ))
                    if not task.operation.value == Operations.PUT.value:
                        raise HTTPException(status_code=409, detail=f"Expected task operation [PUT], but was received [{task.operation}]")

                    # ======================= GET PEER ============================
                    gt_start_time = T.time_ns()
                    for peer_id in task.peers:
                        maybe_peer = await spm_client.get_peer_by_id(peer_id=peer_id)
                        if maybe_peer.is_err:
                            raise HTTPException(status_code=404, detail="No peers available")
                        else:
                            peer = maybe_peer.unwrap()
                        span.add_event(name="get.peer",attributes={"task_id":task_id, "peer_id":peer_id}, timestamp=gt_start_time)

                        # ======================= PUT_CHUNKS ============================
                        gt_start_time = T.time_ns()
                        headers       = {}
                        chunks        = request.stream()
                        result        = await peer.put_chunked(task_id=task_id, chunks = chunks,headers=headers)
                        if result.is_err:
                            raise result.unwrap_err()

                        response = result.unwrap()
                        span.add_event(name="put.chunked",attributes={"task_id":task_id, "peer_id":peer_id}, timestamp=gt_start_time)
                    
                    # ======================= DELETE TASK ============================
                    gt_start_time = T.time_ns()
                    res = await spm_client.delete_task(task_id=task_id)
                    span.add_event(name="delete.task",attributes={"task_id":task_id}, timestamp=gt_start_time)
                    # 
                    self.log.info({
                        "event":"PUT.DATA",
                        "x_timestamp":T.time_ns(),
                        "bucket_id":task.bucket_id,
                        "key":task.key,
                        "size":task.size,
                        "task_id":task_id,
                        "peers":task.peers,
                        "content_type":task.content_type,
                        "deleted_tasks":res.is_ok,
                        "response_time":T.time() - start_time
                    })

                    return JSONResponse(content=jsonable_encoder(response))

                except HTTPException as e:
                    self.log.error({
                        "event":"HTTP.EXCEPTION",
                        "detail":e.detail,
                        "status_code":e.status_code
                    })
                    raise HTTPException(status_code=500, detail = str(e))
                except R.exceptions.HTTPError as e:
                    detail = str(e.response.content.decode("utf-8") )
                    # e.response.reason
                    status_code = e.response.status_code
                    self.log.error({
                        "event":"HTTPError",
                        "detail":detail,
                        "status_code":status_code,
                        "reason":e.response.reason,
                    })
                    raise HTTPException(status_code=status_code, detail=detail  )
                except R.exceptions.ConnectionError as e:
                    detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
                    self.log.error({
                        "event":"CONNECTION.ERROR",
                        "detail":detail,
                        "status_code":500
                    })
                    raise HTTPException(status_code=500, detail=detail  )
                except Exception as e:
                    self.log.error({
                        "event":"EXCEPTION",
                        "detail":str(e),
                        "status_code":500
                    })
                    raise HTTPException(status_code=500, detail = str(e))

        @self.router.post("/api/v4/buckets/{bucket_id}/{key}/disable")
        async def disable(bucket_id:str, key:str,):
            try:
                headers = {}
                start_time = T.time()
                # async with peer_healer_rwlock.reader_lock:
                    # peers = storage_peer_manager.peers
                peers = await self.replica_manager.get_current_replicas(bucket_id=bucket_id,key=key)
                
                for peer in peers:
                    res = await peer.disable(bucket_id=bucket_id,key=key,headers=headers)
                self.log.info({
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
                self.log.error({
                    "detail":detail,
                    "status_code":status_code,
                    "reason":e.response.reason,
                })
                raise HTTPException(status_code=status_code, detail=detail  )
            except R.exceptions.ConnectionError as e:
                detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
                self.log.error({
                    "detail":detail,
                    "status_code":500
                })
                raise HTTPException(status_code=500, detail=detail  )
                

        async def content_generator(response:httpx.Response,chunk_size:str="5mb"):
            _chunk_size = HF.parse_size(chunk_size)
            async for chunk in response.aiter_bytes(chunk_size=_chunk_size):
                yield chunk  
        
        async def memoryview_stream(data: memoryview, chunk_size: int = 65536):
            """Efficient async generator to stream memoryview in large chunks."""
            for i in range(0, len(data), chunk_size):
                chunk = bytes(data[i:i + chunk_size])
                yield chunk  # ✅ Yield memoryview slices directly

        @self.router.get("/api/v4/buckets/{bucket_id}/{key}")
        async def get_data(
            bucket_id:str,
            key:str,
            background_task:BackgroundTasks,
            content_type:str = "",
            chunk_size:Annotated[Union[str,None], Header()]="10mb",
            peer_id:Annotated[Union[str,None], Header()]=None,
            local_peer_id:Annotated[Union[str,None], Header()]=None,
            filename:str = "",
            attachment:bool = False,
            force_get:bool = False,
            spm_client:SPMClient = Depends(self.get_spm_client),

        ):

            with self.tracer.start_as_current_span("get.data") as span:
            
                span:Span = span 
                span.set_attributes({
                    "bucket_id":bucket_id,
                    "key":key
                })
                try:
                    start_at               = T.time_ns()
                    start_time             = T.time()
                    get_peer_start_time    = T.time_ns()
                    peer_id_param_is_empty = peer_id == "" or peer_id == None

                    background_task.add_task(self.after_operation_task(
                        operation="GET",bucket_id=bucket_id, key=key, t1 = start_time
                    ))
                    if not (peer_id_param_is_empty):
                        maybe_peer = await spm_client.get_peer_by_id(peer_id=peer_id)
                    else:
                        maybe_peer = await spm_client.get(bucket_id=bucket_id,key=key)
                    

                    if maybe_peer.is_err:
                        # self.replica_manager.q.put_nowait("")
                        detail = "No available peer{}".format( "" if peer_id_param_is_empty else " {}".format(peer_id))
                        self.log.error({
                            "event":"NO.PEER.AVAILABLE",
                            "bucket_id":bucket_id,
                            "key":key,
                            "peer_id":peer_id,
                            "detail":detail,
                        })
                        raise HTTPException(status_code=404, detail="No found available replica peer or data, try again later.")
                    
                    peer = maybe_peer.unwrap()
                    span.add_event(name="get.peer", attributes={"peer_id":peer.peer_id}, timestamp=get_peer_start_time)
                    headers = {}
                    # > ======================= GET.METADATA ============================
                    get_metadata_start_time = T.time_ns()
                    metadata_result         = await peer.get_metadata(bucket_id=bucket_id, key=key, headers=headers)
                    if metadata_result.is_err:
                        detail = "Fail to fetch metadata from peer {}".format(peer.peer_id)
                        n_deleted = await spm_client.delete(bucket_id=bucket_id,key=key)
                        # self.replica_manager.q.put_nowait("")
                        self.log.error({
                            "event":"GET.METADATA.FAILED",
                            "n_deletes":n_deleted.unwrap_or(0),
                            "bucket_id":bucket_id,
                            "key":key,
                            "peer_id":peer.peer_id,
                            "detail":detail,
                            "error":str(metadata_result.unwrap_err())
                        })
                        raise HTTPException(status_code=404, detail=detail)
                
                    metadata = metadata_result.unwrap()
                    span.set_attributes({"size":metadata.metadata.size})
                    span.add_event(name="get.metadata", attributes={},timestamp=get_metadata_start_time)
                    # > ======================= GET.STREAMING ============================
                    get_streaming_start_time = T.time_ns()
                    cache_result = self.cache.get(key=f"{bucket_id}@{key}")
                    _peer_id       = metadata.peer_id if not peer_id else peer_id
                    _local_peer_id = metadata.local_peer_id if not local_peer_id else local_peer_id
                    if cache_result.is_some and not force_get:
                        cached_datax =cache_result.unwrap()
                        self.log.info({
                            "event":"GET.DATA.CACHING",
                            "start_at":start_at,
                            "end_at":T.time_ns(),
                            "bucket_id":bucket_id,
                            "key":key,
                            "size":len(cached_datax),
                            "peer_id":_peer_id,
                            "local_peer_id":_local_peer_id,
                            "hit":1,
                            "chunk_size":chunk_size,
                            "response_time":T.time() - start_time 
                        })
                        chunk_size_bytes= HF.parse_size(chunk_size)
                        # print("CHUNKL_SIOZE_BYUTES", chunk_size_bytes)
                        # with cached_datax as cache_data:
                        response = StreamingResponse(memoryview_stream(data = cached_datax.get_data(), chunk_size=chunk_size_bytes ), media_type=content_type)
                        background_task.add_task(cached_datax.close)
                    
                        _filename = metadata.metadata.tags.get("fullname", metadata.metadata.tags.get("filename", "{}_{}".format(bucket_id,key) ) ) if filename == "" else filename
                        
                        if attachment:
                            response.headers["Content-Disposition"] = f"attachment; filename={_filename}" 
                        return response

                    result                   = await peer.get_streaming(bucket_id=bucket_id,key=key,headers=headers)
                    print("*"*20)
                    print("RESULT", result)
                    print("*"*20)
                    if result.is_err:
                        self.log.error({
                            "bucket_id":bucket_id,
                            "key":key,
                            "detail": str(result.unwrap_err())
                        })
                        raise HTTPException(status_code=404, detail="Ball({}@{}) not found".format(bucket_id,key))
                    else:
                        response   = result.unwrap()
                        put_result = self.cache.put(key=f"{bucket_id}@{key}", value=response.content)
                        print("CACHE_PUT_RESULT", put_result)
                        span.add_event("get.streaming", attributes={}, timestamp=get_streaming_start_time)
                        
                        cg             = content_generator(response=response,chunk_size=chunk_size)

                        media_type     = response.headers.get('Content-Type',metadata.metadata.content_type) if content_type == "" else content_type
        # 

                        self.log.info({
                            "event":"GET.DATA",
                            "start_at":start_at,
                            "end_at":T.time_ns(),
                            "bucket_id":bucket_id,
                            "key":key,
                            "size":metadata.metadata.size,
                            "peer_id":_peer_id,
                            "local_peer_id":_local_peer_id,
                            "hit":int(_local_peer_id==_peer_id),
                            "response_time":T.time() - start_time 
                        })
                        response  = StreamingResponse(cg, media_type=media_type)

                        _filename = metadata.metadata.tags.get("fullname", metadata.metadata.tags.get("filename", "{}_{}".format(bucket_id,key) ) ) if filename == "" else filename
                        if attachment:
                            response.headers["Content-Disposition"] = f"attachment; filename={_filename}" 

                        # ACCESS_COUNTER.labels(bucket_id=bucket_id, key = key).inc()
                        return response

                        # return JSONResponse(content=jsonable_encoder(most_recent_metadata) )
                        

                except HTTPException as e:
                    self.log.error({
                        "event":"HTTP.EXCEPTION",
                        "detail":e.detail,
                        "status_code":e.status_code
                    })
                    raise HTTPException(status_code=e.status_code, detail = e.detail, headers=e.headers)
                except R.exceptions.HTTPError as e:
                    detail = str(e.response.content.decode("utf-8") )
                    # e.response.reason
                    status_code = e.response.status_code
                    self.log.error({
                        "event":"HTTP.ERROR",
                        "detail":detail,
                        "status_code":status_code,
                        "reason":e.response.reason,
                    })
                    raise HTTPException(status_code=status_code, detail=detail  )
                except R.exceptions.ConnectionError as e:
                    detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
                    self.log.error({
                        "event":"CONNECTION.ERROR",
                        "detail":detail,
                        "status_code":500
                    })
                    raise HTTPException(status_code=500, detail=detail  )
        @self.router.get("/api/v4/buckets/{bucket_id}/metadata/{ball_id}/chunks")
        async def get_metadata_chunks(
            bucket_id:str,
            ball_id:str,
            spm_client:SPMClient = Depends(self.get_spm_client)
        ):
            try:
                start_time = T.time()
                # async with peer_healer_rwlock.reader_lock:
                    # peers = storage_peer_manager.peers
                peers_result = await spm_client.get_peers()
                
                if peers_result.is_err:
                    
                    detail = "No available peers"
                    
                    self.log.error({
                        "event":"NO.PEERS.AVAILABLE",
                        "bucket_id":bucket_id,
                        "key":ball_id,
                        "detail":detail,
                    })
                    raise HTTPException(status_code=404, detail="No found available peers, try again later.")
                
                peers_response = peers_result.unwrap()
                peers = list(map(lambda x:x.to_async_peer(),peers_response.available))

                responses:List[Metadata] = []
                tmp_keys = []
                size= 0
                for peer in peers:
                    result = await peer.get_chunks_metadata(bucket_id = bucket_id, key = ball_id)
                    if result.is_err:
                        continue

                    response = list(result.unwrap())
                    for r in response:
                        if not r.key in tmp_keys:
                            size+= r.size 
                            tmp_keys.append(r.key)
                        else:
                            continue
                    responses += response
                
                xs = sorted(responses,key=lambda x : int(x.tags.get("updated_at","-1")))
                if len(xs) ==0:
                    self.log.error({
                        "event":"Ball not found",
                        "bucket_id":bucket_id,
                        "ball_id":ball_id
                    })
                    raise HTTPException(
                        status_code= 404, 
                        detail="{bucket_id}@{ball_id} not found"
                    )

                self.log.info({
                    "event":"GET.METADATA.CHUNKS",
                    "bucket_id":bucket_id,
                    "ball_id":ball_id,
                    "response_time":T.time() - start_time
                })

                return {
                    "bucket_id":bucket_id,
                    "ball_id":ball_id,
                    "size":HF.format_size(size),
                    "size_bytes":size,
                    "checksum":xs[0].tags.get("full_checksum",""),
                    "chunks":xs
                }


            except R.exceptions.HTTPError as e:
                detail = str(e.response.content.decode("utf-8") )
                # e.response.reason
                status_code = e.response.status_code
                self.log.error({
                    "detail":detail,
                    "status_code":status_code,
                    "reason":e.response.reason,
                })
                raise HTTPException(status_code=status_code, detail=detail  )
            except R.exceptions.ConnectionError as e:
                detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
                self.log.error({
                    "detail":detail,
                    "status_code":500
                })
                raise HTTPException(status_code=500, detail=detail  )


        @self.router.delete("/api/v4/buckets/{bucket_id}/{key}")
        async def delete_data_by_key(
            bucket_id:str,
            key:str,
            spm_client:SPMClient = Depends(self.get_spm_client),
            force:Annotated[int, Header()] = 1
        ):
            with self.tracer.start_as_current_span("delete") as span:
                span:Span = span
                span.set_attributes({"bucket_id":bucket_id, "key":key})
                _bucket_id = MictlanXUtils.sanitize_str(x=bucket_id)
                _key       = MictlanXUtils.sanitize_str(x=key)
                try:

                    start_time= T.time()
                    gcr_timestamp = T.time_ns()
                    get_replicas_result = await spm_client.get_replicas(bucket_id=bucket_id,key=key)
                    if get_replicas_result.is_err:
                        error = str(get_replicas_result.unwrap_err())
                        self.log.error({
                            "detal":"GET.REPLICAS.FAILED",
                            "error":error
                        })
                        raise HTTPException(status_code=500, detail=f"Get replicas failed: {error}")
                    replicas = get_replicas_result.unwrap()
                    peers = list(map(lambda x:x.to_async_peer(),replicas.replicas))
                    if len(peers)==0:
                        self.replica_manager.q.put_nowait("")
                        detail = f"{bucket_id}@{key} not found"
                        status_code = 404
                        self.log.error({
                            "detail":detail,
                            "status_code":status_code,
                        })
                        raise HTTPException(status_code=status_code, detail=detail  )

                    span.add_event(name="get.replicas",attributes={},timestamp=gcr_timestamp)
                    default_delete_by_key_response = DeletedByKeyResponse(n_deletes=0,key=_key)
                    # print("PEERS",peers)
                    for peer in  peers:
                        timestamp = T.time_ns()
                        result = await peer.delete(
                            bucket_id=_bucket_id,
                            key= _key,
                            timeout=30,
                            headers={"Force":str(force)}
                        )
                        if result.is_ok:
                            res = result.unwrap()
                            # print("PEER.DELETE.RESULT", res)
                            self.log.debug({
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
                    res           = await spm_client.delete(bucket_id=bucket_id,key=key)
                    span.add_event(name="remove.replicas", attributes={}, timestamp=timestamp)
                    self.log.info({
                        "event":"DELETED.BY.KEY",
                        "bucket_id":_bucket_id,
                        "key":_key,
                        "n_deletes":default_delete_by_key_response.n_deletes,
                        "force":bool(force),
                        "response_time":response_time,
                    })
                    
                    return JSONResponse(content=jsonable_encoder(default_delete_by_key_response.model_dump()))
                except R.exceptions.HTTPError as e:
                        detail = str(e.response.content.decode("utf-8") )
                        status_code = e.response.status_code
                        self.log.error({
                            "detail":detail,
                            "status_code":status_code,
                            "reason":e.response.reason,
                        })
                        raise HTTPException(status_code=status_code, detail=detail  )
                except R.exceptions.ConnectionError as e:
                    detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
                    self.log.error({
                        "detail":detail,
                        "status_code":500
                    })
                    raise HTTPException(status_code=500, detail=detail  )


        @self.router.delete("/api/v4/buckets/{bucket_id}/bid/{ball_id}")
        async def delete_data_by_ball_id(
            bucket_id:str,
            ball_id:str,
            spm_client:SPMClient = Depends(self.get_spm_client), 
            force:Annotated[int, Header()] = 1,
        ):

            with self.tracer.start_as_current_span("delete") as span:
                span:Span = span 
                _bucket_id = MictlanXUtils.sanitize_str(x= bucket_id)
                _ball_id = MictlanXUtils.sanitize_str(x= ball_id)

                start_time= T.time()

                gcr_timestamp = T.time_ns()
                get_peers_result = await spm_client.get_peers()
                if get_peers_result.is_err:
                    error = str(get_peers_result.unwrap_err())
                    self.log.error({
                        "detal":"GET.PEERS.FAILED",
                        "error":error
                    })
                    raise HTTPException(status_code=500, detail=f"Get peers failed: {error}")
                replicas = get_peers_result.unwrap()
                peers = list(map(lambda x:x.to_async_peer(),replicas.available))
                span.add_event(name="get.replicas",attributes={},timestamp=gcr_timestamp)
                headers = {}
                combined_key = ""
                _start_time = T.time()
                # print("PEERS", peers)
                try:
                    default_del_by_ball_id_response = DeletedByBallIdResponse(n_deletes=0, ball_id=_ball_id)
                    for peer in  peers:
                        start_time = T.time()
                        chunks_metadata_result:Result[Iterator[Metadata],Exception] = await peer.get_chunks_metadata(
                            key=_ball_id,
                            bucket_id=_bucket_id,
                            headers=headers
                        )
                        # print("CUNKS_METDATA_RESULT", chunks_metadata_result)
                        # print("CHUNK_M_RESULT",chunks_metadata_result)
                        if chunks_metadata_result.is_ok:
                            response = chunks_metadata_result.unwrap()
                            for i,metadata in enumerate(response):
                                timestamp = T.time_ns()
                                if i ==0:
                                    combined_key = "{}@{}".format(metadata.bucket_id,metadata.key)

                                del_result = await peer.delete(
                                    bucket_id=_bucket_id,
                                    key=metadata.key,
                                    headers={"Force":str(force)}
                                )
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
                                res           = await self.replica_manager.remove_replicas(bucket_id=bucket_id,key=metadata.key)
                                self.log.debug({
                                    "event":"DELETE.BY.BALL_ID",
                                    "bucket_id":_bucket_id,
                                    "ball_id":_ball_id,
                                    "key":metadata.key,
                                    "peer_id":peer.peer_id,
                                    "status":int(del_result.is_ok)-1,
                                    "service_time":service_time,
                                })


                    if combined_key == "" or len(combined_key) ==0:
                        self.log.error({
                            "event":"NOT.FOUND",
                            "bucket_id":_bucket_id,
                            "ball_id":_ball_id,
                        })
                        res           = await self.replica_manager.remove_replicas(bucket_id=bucket_id,key=_ball_id)
                        return JSONResponse(content=jsonable_encoder(default_del_by_ball_id_response.model_dump()))
                    
                    self.log.info({
                        "event":"DELETED.BY.BALL_ID",
                        "bucket_id":_bucket_id,
                        "ball_id":_ball_id,
                        "n_deletes": default_del_by_ball_id_response.n_deletes,
                        "force":bool(force),
                        "response_time":T.time() - _start_time,
                    })
                    res           = await self.replica_manager.remove_replicas(bucket_id=bucket_id,key=_ball_id)
                    return JSONResponse(content=jsonable_encoder(default_del_by_ball_id_response))
                # Response(content=deleted_replicas, status_code=200)
                except R.exceptions.HTTPError as e:
                    detail = str(e.response.content.decode("utf-8") )
                    status_code = e.response.status_code
                    self.log.error({
                        "detail":detail,
                        "status_code":status_code,
                        "reason":e.response.reason,
                    })
                    raise HTTPException(status_code=status_code, detail=detail  )
                except R.exceptions.ConnectionError as e:
                    detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
                    self.log.error({
                        "detail":detail,
                        "status_code":500
                    })
                    raise HTTPException(status_code=500, detail=detail  )
