import asyncio
import humanfriendly as HF
from mictlanx.logger.log import Log
from fastapi import APIRouter,Header,HTTPException,UploadFile,Response
from fastapi.responses import StreamingResponse
from opentelemetry.trace import Span,Status,StatusCode,Tracer
from mictlanxrouter.helpers.utils import Utils
from typing import Annotated,Union,List,Dict,Tuple
from mictlanxrouter.dto.metadata import Metadata
from mictlanxrouter.replication_manager import ReplicaManager
from mictlanxrouter.peer_manager import StoragePeerManager
from mictlanxrouter.dto import TaskX,Operations
from mictlanxrouter.tasksx import TaskManagerX
from option import Result,Ok,Err
import time as T
from mictlanx.v4.interfaces.responses import PeerPutMetadataResponse,PutMetadataResponse
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
import requests  as R
from mictlanx.utils import Utils as MictlanXUtils
from nanoid import generate as nanoid
from mictlanxrouter.caching import CacheX
# from mictlanxrouter.dto.index i


class BucketsController():
    def __init__(self,
                 log:Log,
                 tracer:Tracer,
                 replica_manager:ReplicaManager,
                 storage_peer_manager:StoragePeerManager,
                 tm: TaskManagerX,
                 cache:CacheX
    ):
        self.log                  = log
        self.router               = APIRouter()
        self.tracer               = tracer
        self.replica_manager      = replica_manager
        self.storage_peer_manager = storage_peer_manager
        self.tm                   = tm
        self.cache                = cache
        self.add_routes()
    def add_routes(self):
        @self.router.post("/api/v4/buckets/{bucket_id}/metadata")
        async def put_metadata(
            bucket_id:str,
            metadata:Metadata, 
            update:Annotated[Union[str,None], Header()] = "0"
        ):
            with self.tracer.start_as_current_span("put.metadata") as span:
                span:Span      = span  
                arrival_time   = T.time()
                start_at       = T.time_ns()
                available_peers = await self.storage_peer_manager.get_available_peers()
                key       = MictlanXUtils.sanitize_str(x = metadata.key)
                bucket_id = MictlanXUtils.sanitize_str(x = bucket_id)
                group_id = nanoid()
                if len(available_peers) ==0:
                    detail= "Put metadata failed, no available peers."
                    self.log.error({
                        "event":"NO.AVAILABLE.PEERS",
                        "bucket_id":bucket_id,
                        "key":key,
                        "detail":detail,
                        "available_peers":[]
                    })
                    span.set_status(Status(status_code=StatusCode.ERROR))
                    raise HTTPException(status_code=404, detail=detail ,headers={} )


                headers        = {"Update":update}
                if not bucket_id == metadata.bucket_id:
                    raise HTTPException(status_code=400, detail="Bucket ID mismatch: {} != {}".format(bucket_id, metadata.bucket_id))

                
                span.set_attributes({
                    "bucket_id":bucket_id,
                    "key":key,
                    "size":metadata.size
                })
                # ======================= GET.CURRENT.REPLICAS ============================
               
                current_replicas = await self.replica_manager.get_current_replicas_ids(bucket_id=bucket_id,key=key)
            
                if len(current_replicas) > 0:
                    detail = "{}/{} already exists.".format(metadata.bucket_id,metadata.key)
                    self.log.error({
                        "event":"ALREADY.EXISTS",
                        "bucket_id":bucket_id,
                        "key":key,
                        "detail":detail,
                        "replicas":current_replicas
                    })
                    span.set_status(Status(status_code=StatusCode.ERROR))
                    raise HTTPException(status_code=409, detail=detail ,headers={} )
        
                
                # ======================= CREATE.REPLICAS ============================
                # cr_start_time = T.time_ns()
                create_replica_result = await self.replica_manager.create_replicas(
                    bucket_id=bucket_id,
                    key= key,
                    size= metadata.size,
                    peer_ids=[],
                    rf=metadata.replication_factor
                )

                if len(create_replica_result.success_replicas) ==0:
                    detail= "Put metadata failed, no available peers."
                    self.log.error({
                        "event":"NO.AVAILABLE.PEERS",
                        "bucket_id":bucket_id,
                        "key":key,
                        "detail":detail,
                        "available_peers":create_replica_result.available_replicas
                    })
                    span.set_status(Status(status_code=StatusCode.ERROR))
                    raise HTTPException(status_code=404, detail=detail ,headers={} )

                # combined_key = "{}@{}".format(bucket_id,key)

                self.log.debug("PUT.METADATA {} {}".format(bucket_id,key))

                try: 
                    replicas               = create_replica_result.success_replicas
                    selected_replica_peers = await self.storage_peer_manager.from_peer_ids_to_peers(replicas)
                    ordered_peer_ids       = list(map(lambda x: x.peer_id, selected_replica_peers))
                    tasks_ids              = []
                    for peer in selected_replica_peers:
                        inner_start_time = T.time()
                        put_metadata_result:Result[PeerPutMetadataResponse,Exception] = peer.put_metadata(
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
                            peers=[peer.peer_id],
                            size=metadata.size,
                            content_type=metadata.content_type
                        )
                        add_task_result = await self.tm.add_task(task= task)
                        if add_task_result.is_err:
                            raise add_task_result.unwrap_err()
                        tasks_ids.append(task.task_id)
                        self.log.info(
                            Utils.dict_to_string({
                            "event":"PUT.METADATA",
                            "bucket_id":bucket_id,
                            "key":metadata.key,
                            "size":metadata.size,
                            "peer_id":peer.peer_id,
                            "task_id":task.task_id,
                            "content_type":task.content_type,
                            "replica_factor":metadata.replication_factor,
                            "response_time":T.time()- inner_start_time
                        }))

                    put_metadata_response = PutMetadataResponse(
                        bucket_id=bucket_id,
                        key=key,
                        replicas=ordered_peer_ids,
                        tasks_ids=tasks_ids,
                        service_time=T.time()- arrival_time,
                    )
                    return JSONResponse(content=jsonable_encoder(put_metadata_response))

                except HTTPException as e:
                    span.add_event(name="HTTPException", attributes={"detail":e.detail,"status_code":e.status_code})
                    span.set_status(Status(status_code=StatusCode.ERROR))
                    raise HTTPException(status_code=e.status_code, detail = e.detail, headers=e.headers)
                except R.exceptions.HTTPError as e:
                    detail = str(e.response.content.decode("utf-8") )
                    status_code = e.response.status_code
                    span.add_event(name="HTTPError", attributes={"detail":detail,"status_code":status_code})
                    span.set_status(Status(status_code=StatusCode.ERROR))
                    raise HTTPException(status_code=status_code, detail=detail  )
                except R.exceptions.ConnectionError as e:
                    detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
                    status_code = e.response.status_code

                    span.set_status(Status(status_code=StatusCode.ERROR))
                    span.add_event(name="ConnectionError", attributes={"detail":detail,"status_code":status_code})
                    raise HTTPException(status_code=status_code, detail=detail  )
                except Exception as e:
                    detail = str(e)
                    span.set_status(Status(status_code=StatusCode.ERROR))
                    span.add_event(name="Exception", attributes={"detail":detail,"status_code":500})
                    raise HTTPException(status_code=500, detail=detail)

        @self.router.post("/api/v4/buckets/data/{task_id}")
        async def put_data(
            task_id:str,
            data:UploadFile, 
            peer_id:Annotated[Union[str,None], Header()]=None
        ):
            with self.tracer.start_as_current_span("put.data") as span:
                span:Span = span
                start_at   = T.time_ns()
                start_time = T.time()
                
                # =========================GET.TASK=======================================
                get_task_timestamp = T.time_ns()
                maybe_task         = await self.tm.get_task(task_id=task_id)

                if maybe_task.is_err:
                    detail = "Task({}) not found".format(task_id)
                    raise HTTPException(status_code=404, detail=detail)
                task = maybe_task.unwrap()
                if not task.operation == Operations.PUT.value:
                    raise HTTPException(status_code=409, detail=f"Expected task operation [PUT], but was received [{task.operation}]")

                # span.set_attributes({
                #     **task.__dict__
                # })
                span.add_event(name="get.task",attributes={"task_id":task_id},timestamp=get_task_timestamp)
                # =========================GET.PEER=======================================
                
                get_task_timestamp = T.time_ns()
                maybe_peer         = await self.storage_peer_manager.get_peer_by_id(peer_id=peer_id)
                if maybe_peer.is_none:
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
                    result = peer.put_data(task_id=task_id,key=task.key,value=value,content_type=task.content_type)
                    if result.is_err:
                        raise result.unwrap_err()
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
                    await self.tm.delete_task(task_id=task_id)
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


        async def content_generator(response:R.Response,chunk_size:str="5mb"):
            _chunk_size = HF.parse_size(chunk_size)
            for chunk in response.iter_content(chunk_size=_chunk_size):
                yield chunk  
        
        async def memoryview_stream(data: memoryview, chunk_size: int = 10):
            """Async generator to stream memoryview in chunks."""
            for i in range(0, len(data), chunk_size):
                yield data[i:i + chunk_size].tobytes()  # Convert memoryview slice to bytes
                await asyncio.sleep(0)  # Yield control for async streaming

        @self.router.get("/api/v4/buckets/{bucket_id}/{key}")
        async def get_data(
            bucket_id:str,
            key:str,
            content_type:str = "",
            chunk_size:Annotated[Union[str,None], Header()]="10mb",
            peer_id:Annotated[Union[str,None], Header()]=None,
            local_peer_id:Annotated[Union[str,None], Header()]=None,
            filename:str = "",
            attachment:bool = False
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

                    if not (peer_id_param_is_empty):
                        maybe_peer = await self.storage_peer_manager.get_peer_by_id(peer_id=peer_id)
                    else:
                        maybe_peer = await self.replica_manager.access(bucket_id=bucket_id,key=key)
                        
                    if maybe_peer.is_none:
                        self.replica_manager.q.put_nowait("")
                        detail = "No available peer{}".format( "" if peer_id_param_is_empty else " {}".format(peer_id))
                        self.log.error({
                            "event":"GET.DATA.FAILED",
                            "bucket_id":bucket_id,
                            "key":key,
                            "peer_id":peer_id,
                            "detail":detail,
                        })
                        raise HTTPException(status_code=404, detail=detail)
                    
                    peer = maybe_peer.unwrap()
                    span.add_event(name="get.peer", attributes={"peer_id":peer.peer_id}, timestamp=get_peer_start_time)
                    headers = {}
                    # > ======================= GET.METADATA ============================
                    get_metadata_start_time = T.time_ns()
                    metadata_result         = peer.get_metadata(bucket_id=bucket_id, key=key, headers=headers)
                    if metadata_result.is_err:
                        detail = "Fail to fetch metadata from peer {}".format(peer.peer_id)
                        self.log.error({
                            "event":"GET.METADATA.FAILED",
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
                    print("CACHIONG_RESULT", cache_result)
                    if cache_result.is_some:
                        cached_data =cache_result.unwrap()
                        self.log.info({
                            "event":"GET.DATA.CACHING",
                            "start_at":start_at,
                            "end_at":T.time_ns(),
                            "bucket_id":bucket_id,
                            "key":key,
                            "size":metadata.metadata.size,
                            "peer_id":_peer_id,
                            "local_peer_id":_local_peer_id,
                            "hit":1,
                            # "eviction_policy":
                            "response_time":T.time() - start_time 
                        })
                        response = StreamingResponse(memoryview_stream(data = cached_data, chunk_size=HF.parse_size(chunk_size)))
                        _filename = metadata.metadata.tags.get("fullname", metadata.metadata.tags.get("filename", "{}_{}".format(bucket_id,key) ) ) if filename == "" else filename
                        if attachment:
                            response.headers["Content-Disposition"] = f"attachment; filename={_filename}" 
                        return response

                    result                   = peer.get_streaming(bucket_id=bucket_id,key=key,headers=headers)

                    if result.is_err:
                        raise HTTPException(status_code=404, detail="Ball({}@{}) not found".format(bucket_id,key))
                    else:
                        response       = result.unwrap()
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



        @self.router.get("/api/v4/buckets/{bucket_id}/metadata")
        async def get_bucket_metadata(bucket_id):
            with self.tracer.start_as_current_span("get.bucket.metdata") as span:
                span:Span = span
                try:
                    start_time    = T.time()
                    gap_timestamp = T.time_ns()
                    peers         = await self.storage_peer_manager.get_available_peers()
                    span.add_event(name="get.available.peers",
                        attributes={
                            "peers":map(lambda p:p.peer_id,peers)
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
                    maybe_peer = await self.replica_manager.access(bucket_id=bucket_id,key=key)
                    if maybe_peer.is_none:
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
                    metadata_result = peer.get_metadata(bucket_id=bucket_id, key=key, headers={})
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






        @self.router.get("/api/v4/buckets/{bucket_id}/metadata/{ball_id}/chunks")
        async def get_metadata_chunks(
            bucket_id:str,
            ball_id:str,
        ):
            try:
                start_time = T.time()
                # async with peer_healer_rwlock.reader_lock:
                    # peers = storage_peer_manager.peers
                peers = await self.storage_peer_manager.get_available_peers()
                responses:List[Metadata] = []
                for peer in peers:
                    result = peer.get_chunks_metadata(bucket_id = bucket_id, key = ball_id)

                    if result.is_err:
                        continue
                    response = list(result.unwrap())
                    responses += response
                xs = sorted(responses,key=lambda x : int(x.tags.get("updated_at","-1")))
                self.log.info({
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
