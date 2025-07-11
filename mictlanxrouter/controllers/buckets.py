import os
import asyncio
import humanfriendly as HF
from typing import Annotated,Union,List,Dict,Tuple,Iterator
import time as T
import requests  as R
import aiofiles

# 
from option import Result,Ok,Err
# 
import httpx
# 
import tempfile
# from opentelemetry.trace import Span,Status,StatusCode,Tracer
# 
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi import APIRouter,Header,HTTPException,UploadFile,Response,Request,Depends,BackgroundTasks
from fastapi.responses import StreamingResponse
# 
from nanoid import generate as nanoid
# 
from mictlanx.logger.log import Log
from mictlanx.utils import Utils as MictlanXUtils
import mictlanx.v4.interfaces as InterfaceX
# 
from mictlanxrm.client import SPMClient
from mictlanxrm.models import TaskX
# 
from mictlanxrouter.dto import Operations,DeletedByBallIdResponse,DeletedByKeyResponse
from mictlanxrouter.caching import CacheX
from mictlanxrouter.dto.metadata import Metadata
from mictlanxrouter.helpers.utils import Utils
from mictlanxrouter.decorators import disconnect_protected
from tenacity import retry, stop_after_attempt, wait_fixed, RetryError
from contextlib import contextmanager

class DependencyContainer:
    def __init__(self, log:Log):
        self.log = log

    @contextmanager
    def get_spm_client(self):
        try:
            client = SPMClient(
                hostname=os.environ.get("MICTLANX_DAEMON_HOSTNAME", "localhost"),
                port=int(os.environ.get("MICTLANX_DAEMON_PORT", "5555")),
                protocol=os.environ.get("MICTLANX_DAEMON_PROTOCOL", "tcp")
            )
            yield client
        except Exception as e:
            self.log.error({
                "event": "GET.SPM.CLIENT.FAILED",
                "detail": str(e)
            })
            raise e

# Retry wrapper for the dependency
def retry_spm_client(dep: DependencyContainer):
    @retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
    def _try_get_client():
        with dep.get_spm_client() as client:
            return client
    try:
        return _try_get_client()
    except RetryError as e:
        dep.log.error({"event": "RETRY.GET.SPM.CLIENT.FAILED", "detail": str(e)})
        raise RuntimeError("SPM Client unavailable after retries")










class BucketsController():
    def __init__(self,
                 log:Log,
                 cache:CacheX,
                 max_timeout:str= "10s"
    ):
        self.log                  = log
        self.router               = APIRouter()
        self.cache                = cache
        self.max_timeout = HF.parse_timespan(max_timeout)
        self.dep = DependencyContainer(log=self.log)
        # self.spm_client = SPMClient()
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
            client.close()
            raise e
        # finally:
        #     client.socket.close()

    def after_operation_task(self,operation:str, bucket_id:str, key:str, t1:float,size:int):
        def __inner():
            self.log.debug({
                "operation":operation,
                "bucket_id":bucket_id,
                "key":key,
                "size":size,
                "response_time":T.time() - t1
            })
        return __inner
            # print("GET")
            # self.spm_client

    def spm_client_close(self,x:SPMClient):
        async def __inner():
            y = x.close()
            return None
            
        return __inner


    def add_routes(self):

        # @disconnect_protected()
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
        
        
        # @disconnect_protected()
        @self.router.get("/api/v4/buckets/{bucket_id}/metadata")
        async def get_bucket_metadata(bucket_id:str, spm_client:SPMClient = Depends(self.get_spm_client)):
                try:
                    start_time    = T.time()
                    gap_timestamp = T.time_ns()
                    peers_result         = await spm_client.get_peers()
                    if peers_result.is_err:
                        raise HTTPException(detail=str(peers_result.unwrap_err()), status=500)
                    peers = [ p.to_async_peer() for p in peers_result.unwrap().available]


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
                
        # @disconnect_protected()
        @self.router.get("/api/v4/buckets/{bucket_id}/metadata/{key}")
        async def get_metadata(
            bucket_id:str,
            key:str,
            consistency_model:Annotated[Union[str,None], Header()]="LB",
            peer_id:Annotated[Union[str,None], Header()]=None,
            spm_client:SPMClient = Depends(self.get_spm_client)
            
        ):
            try:
                start_time = T.time()

                access_start_time = T.time_ns()
                maybe_peer = await spm_client.get(bucket_id=bucket_id,key=key)


                if maybe_peer.is_err:
                    detail = "No available peers"
                    self.log.error({
                        "event":"GET.METADATA.FAILED",
                        "bucket_id":bucket_id,
                        "key":key,
                        "detail":detail
                    })
                    raise HTTPException(status_code=404, detail=detail)
                peer = maybe_peer.unwrap()
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
                end_at = T.time()
                self.log.info({
                    "event":"GET.METADATA",
                    "arrival_time":start_time,
                    "end_at":end_at,
                    "bucket_id":bucket_id,
                    "key":key,
                    "peer_id":most_recent_metadata.peer_id,
                    "local_peer_id":most_recent_metadata.local_peer_id,
                    "size":most_recent_metadata.metadata.size,
                    "response_time":end_at- start_time
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
                    "event":"EXCEPTION.GET.METADATA",
                    "detail":detail,
                    "status_code":500
                })
                raise HTTPException(status_code=500, detail=detail  )

        # @disconnect_protected()
        @self.router.post("/api/v4/u/buckets/{bucket_id}/{key}")
        async def update_metadata(
            bucket_id:str,
            key:str,
            metadata:Metadata, 
            spm_client:SPMClient = Depends(self.get_spm_client)
        ):
            current_replicas_result = await spm_client.get_replicas(bucket_id=bucket_id, key= key)
            if current_replicas_result.is_err:
                self.log.error(str(current_replicas_result.unwrap_err()))
                raise HTTPException("Failed to get current replicas")
            current_replicas = current_replicas_result.unwrap().available_replicas
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
    
        @disconnect_protected()
        @self.router.post("/api/v4/buckets/{bucket_id}/metadata")
        async def put_metadata(
            bucket_id:str,
            metadata:Metadata, 
            update:Annotated[Union[str,None], Header()] = "0",
            spm_client:SPMClient = Depends(self.get_spm_client)

        ):
                arrival_time   = T.time()
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
                    end_at = T.time()
                    self.log.info({
                        "event":"PUT.METADATA",
                        "arrival_time": arrival_time,
                        "end_at":end_at,
                        "bucket_id":bucket_id,
                        "key":metadata.key,
                        "size":metadata.size,
                        "peer_id":replica.peer_id,
                        "task_id":task.task_id,
                        "content_type":task.content_type,
                        "replica_factor":metadata.replication_factor,
                        "response_time":end_at- arrival_time
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
                    raise HTTPException(status_code=500, detail=detail)

        @disconnect_protected()
        @self.router.post("/api/v4/buckets/data/{task_id}")
        async def put_data(
            task_id:str,
            data:UploadFile, 
            background_task:BackgroundTasks,
            peer_id:Annotated[Union[str,None], Header()]=None,
            spm_client:SPMClient = Depends(lambda: retry_spm_client(dep=self.dep)),
            # spm_client:SPMClient = Depends(self.get_spm_client),

        ):
                start_time = T.time()
                
                # =========================GET.TASK=======================================
                get_task_timestamp = T.time_ns()
                maybe_task         = await spm_client.get_task(task_id=task_id)

                if maybe_task.is_err:
                    detail = "Task({}) not found".format(task_id)
                    raise HTTPException(status_code=404, detail=detail)
                task = maybe_task.unwrap()
                background_task.add_task(self.after_operation_task(operation="PUT", bucket_id=task.bucket_id, key=task.key,t1=start_time,size=task.size))
                if not task.operation.value == Operations.PUT.value:
                    raise HTTPException(status_code=409, detail=f"Expected task operation [PUT], but was received [{task.operation}]")

                # =========================GET.PEER=======================================
                
                get_task_timestamp = T.time_ns()
                maybe_peer         = await spm_client.get_peer_by_id(peer_id=peer_id)
                if maybe_peer.is_err:
                    raise HTTPException(status_code=404, detail="Peer({}) is not available.".format(peer_id))
                # else:
                peer = maybe_peer.unwrap()

                try:

                    # =========================READ.DATA=======================================
                    get_task_timestamp = T.time_ns()
                    value              = await data.read()
                    size               = len(value)
                    if size != task.size:
                        raise HTTPException(status_code=500, detail="Data size mistmatch {} != {}".format(size, task.size))
                    # =========================PUT.DATA=======================================
                    get_task_timestamp = T.time_ns()
                    result = await peer.put_data(task_id=task_id,key=task.key,value=value,content_type=task.content_type)
                    if result.is_err:
                        self.log.error({
                            "event":"PUT.DATA.FAILED",
                            "detail":str(result.unwrap_err())
                        })
                        raise HTTPException(detail = str(result.unwrap_err()), status_code= 500 )
                    end_at = T.time()
                    self.log.info({
                        "event":"PUT.DATA",
                        "arrival_time":start_time,
                        "end_at":end_at,
                        "bucket_id":task.bucket_id,
                        "key":task.key,
                        "size":task.size,
                        "task_id":task_id,
                        "peer_id":peer.peer_id,
                        "response_time":end_at- start_time
                    })
                    get_task_timestamp = T.time_ns()
                    await spm_client.delete_task(task_id=task_id)
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




        @disconnect_protected()
        @self.router.post("/api/v4/buckets/data/{task_id}/chunked")
        async def put_data_chunked(
            task_id: str,
            request: Request,
            background_task: BackgroundTasks,
            spm_client: SPMClient = Depends(self.get_spm_client),
            chunk_size: Annotated[Union[str, None], Header()] = "10mb",
        ):
            _chunk_size = HF.parse_size(chunk_size)
            start_time = T.time()
            temp_file = tempfile.NamedTemporaryFile(delete=False)
            tmp_path = temp_file.name
            temp_file.close()
            try:
                # ============ GET TASK ============
                task_maybe = await spm_client.get_task(task_id=task_id)
                if task_maybe.is_err:
                    raise task_maybe.unwrap_err()
                task = task_maybe.unwrap()

                if task.operation.value != Operations.PUT.value:
                    raise HTTPException(status_code=409, detail=f"Expected task operation [PUT], received [{task.operation}]")

                    # ============ STREAM TO TEMP FILE ============
                # print("HERE->", task.bucket_id,task.key)
                # expected_size = int(request.headers.get("content-length", 0))
                # received_size = 0
                total =0
                t_transfer = T.time()
                async with aiofiles.open(tmp_path, "wb") as f:
                    while True:
                        message = await request.receive()
                        # print("MSG", message)
                        msg_type = message["type"]

                        if msg_type == "http.request":
                            body = message.get("body", b"")
                            more = message.get("more_body", False)

                            if body:
                                await f.write(body)
                                total += len(body)
                                self.log.debug({
                                    "event":"RECEIVED.CHUNK",
                                    "bucket_id":task.bucket_id,
                                    "key":task.key,
                                    "size":len(body),
                                    "expected":task.size,
                                    "received": total,
                                    "percentage":f"{(total/task.size)*100:0.3f}"
                                })
                            if not more:
                                # real end-of-body
                                await f.flush()
                                break

                        elif msg_type == "http.disconnect":
                            raise HTTPException(499, "Client disconnected")
                self.log.info({
                    "event":"UPLOAD.COMPLETED",
                    "bucket_id":task.bucket_id,
                    "key":task.key,
                    "size":HF.format_size(total),
                    "response_time":T.time() - t_transfer
                })
                # print("Upload completed:", HF.format_size(total))

                # try:
                    
                #     print(request.headers)
                #     total_size = 0
                #     stream = request.stream()
                #     async with aiofiles.open(tmp_path, mode="wb") as f:
                #         async for chunk in stream:
                #             if await request.is_disconnected():
                #                 self.log.error("Client disconnected")
                #                 raise HTTPException(status_code=499, detail="Client disconnected during upload")
                #             # if not chunk:
                #                 # self.log.debug("EMPTY_CHUNK")
                #                 # break
                #             total_size += len(chunk)
                #             print(f"Received[{task.bucket_id,task.key}]: {HF.format_size(total_size)}")
                #             await f.write(chunk)
                #         await f.flush()
                # except Exception as e:
                #     print("ERROR!!!!!!!!",e)
                # ============ STREAM TO PEERS ============
                for peer_id in task.peers:
                    tp = T.time()
                    maybe_peer = await spm_client.get_peer_by_id(peer_id=peer_id)
                    if maybe_peer.is_err:
                        raise HTTPException(status_code=404, detail="No peers available")
                    peer = maybe_peer.unwrap()

                    headers = {}

                    async def file_chunk_generator(file_path: str, chunk_size: int):
                        async with aiofiles.open(file_path, "rb") as af:
                            while True:
                                chunk = await af.read(chunk_size)
                                # print("CUINK", len(chunk))
                                if not chunk:
                                    break
                                yield chunk
             

                    result = await peer.put_chunked(
                        task_id=task_id,
                        chunks=file_chunk_generator(file_path=temp_file.name,chunk_size=_chunk_size),
                        headers=headers
                    )
                    self.log.info({
                        "event":"PUT.DATA.PEER",
                        "peer_id":peer_id,
                        "bucket_id":task.bucket_id,
                        "key":task.key,
                        "ok":result.is_ok,
                        "response_time": T.time() -tp
                    })
                    if result.is_err:
                        
                        raise result.unwrap_err()

                    response = result.unwrap()

                # ============ DELETE TASK ============
                await spm_client.delete_task(task_id=task_id)

                # ============ BACKGROUND CACHE ============
                def cache_from_file(cache:CacheX, key:str, file_path:str):
                    try:
                        t1 = T.time()
                        with open(file_path, "rb") as f:
                            data = f.read()
                            cache.put(key=key, value=data)
                        self.log.info({
                            "event":"PUT.CACHE",
                            "key":key,
                            "path":file_path,
                            "response_time":T.time() - t1
                        })
                    except Exception as e:
                        self.log.error({
                            "event": "CACHE.ERROR",
                            "detail": str(e),
                            "file_path": file_path
                        })

                background_task.add_task(
                    cache_from_file,
                    self.cache,
                    key=f"{task.bucket_id}@{task.key}",
                    file_path=temp_file.name
                )

                end_at = T.time()
                self.log.info({
                    "event": "PUT.DATA",
                    "arrival_time": start_time,
                    "end_at": end_at,
                    "bucket_id": task.bucket_id,
                    "key": task.key,
                    "size": task.size,
                    "task_id": task_id,
                    "peers": task.peers,
                    "content_type": task.content_type,
                    "put_cache_background": True,
                    "response_time": end_at - start_time,
                })

                background_task.add_task(self.after_operation_task(
                    operation="PUT",
                    bucket_id=task.bucket_id,
                    key=task.key,
                    t1=start_time,
                    size=task.size
                ))

                return JSONResponse(content=jsonable_encoder(response))

            except HTTPException as e:
                self.log.error({
                    "event": "HTTP.EXCEPTION",
                    "detail": e.detail,
                    "status_code": e.status_code
                })
                raise HTTPException(status_code=500, detail=str(e))

            except R.exceptions.HTTPError as e:
                detail = e.response.content.decode("utf-8")
                self.log.error({
                    "event": "HTTP.ERROR",
                    "detail": detail,
                    "status_code": e.response.status_code,
                    "reason": e.response.reason,
                })
                raise HTTPException(status_code=e.response.status_code, detail=detail)

            except R.exceptions.ConnectionError as e:
                detail = f"Connection error - peers unavailable - {peer.peer_id}"
                self.log.error({
                    "event": "CONNECTION.ERROR",
                    "detail": detail,
                    "status_code": 500
                })
                raise HTTPException(status_code=500, detail=detail)

            except Exception as e:
                self.log.error({
                    "event": "EXCEPTION.PUT.DATA.CHUNKED",
                    "detail": str(e),
                    "status_code": 500
                })
                raise HTTPException(status_code=500, detail=str(e))

            finally:
                try:
                    temp_file.close()
                except:
                    pass







        # @disconnect_protected()
        @self.router.post("/api/v4/buckets/data/{task_id}/chunkedv1")
        async def put_data_chunked_v1(
            task_id:str,
            request: Request,
            background_task:BackgroundTasks,
            spm_client:SPMClient = Depends(self.get_spm_client), 
            chunk_size:Annotated[Union[str,None], Header()]="10mb",

        ):
            _chunk_size = HF.parse_size(chunk_size)
            start_time = T.time()
            try:
                # ======================= GET TASK ============================
                gt_start_time = T.time_ns()
                task_maybe = await spm_client.get_task(task_id=task_id)
                if task_maybe.is_err:
                    raise task_maybe.unwrap_err()
                
                task = task_maybe.unwrap()

                if not task.operation.value == Operations.PUT.value:
                    raise HTTPException(status_code=409, detail=f"Expected task operation [PUT], but was received [{task.operation}]")

                # ======================= GET PEER ============================
                gt_start_time = T.time_ns()
                # raw = await Utils.get_raw(request=request)

                for peer_id in task.peers:
                    maybe_peer = await spm_client.get_peer_by_id(peer_id=peer_id)
                    if maybe_peer.is_err:
                        raise HTTPException(status_code=404, detail="No peers available")
                    else:
                        peer = maybe_peer.unwrap()

                    # ======================= PUT_CHUNKS ============================
                    gt_start_time = T.time_ns()
                    headers       = {}
                    # chunks        = request.stream()
                    # del raw
                    # chunks        = Utils.bytes_to_stream(data = raw, chunk_size=_chunk_size)
                    chunks = request.stream()
                    # request.stream()
                    result        = await peer.put_chunked(task_id=task_id, chunks = chunks,headers=headers)
                    if result.is_err:
                        raise result.unwrap_err()

                    response = result.unwrap()
                
                # ======================= DELETE TASK ============================
                gt_start_time = T.time_ns()
                res = await spm_client.delete_task(task_id=task_id)
                # 

                # value = await request.stream()
                # put_result = self.cache.put(key=f"{task.bucket_id}@{task.key}", value=raw )
                end_at = T.time()
                self.log.info({
                    "event":"PUT.DATA",
                    "arrival_time":start_time,
                    "end_at":end_at,
                    "bucket_id":task.bucket_id,
                    "key":task.key,
                    "size":task.size,
                    "task_id":task_id,
                    "peers":task.peers,
                    "content_type":task.content_type,
                    "deleted_tasks":res.is_ok,
                    # "put.cache.result":put_result,
                    "response_time":end_at - start_time
                })
                background_task.add_task(self.after_operation_task(
                    operation="PUT",
                    bucket_id=task.bucket_id,
                    key= task.key,
                    t1 = start_time,
                    size=task.size
                ))
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
                    "event":"EXCEPTION.PUT.DATA.CHUNKED",
                    "detail":str(e),
                    "status_code":500
                })
                raise HTTPException(status_code=500, detail = str(e))

        # @disconnect_protected()
        @self.router.post("/api/v4/buckets/{bucket_id}/{key}/disable")
        async def disable(bucket_id:str, key:str,spm_client:SPMClient = Depends(self.get_spm_client)):
            try:
                headers = {}
                start_time = T.time()
                # async with peer_healer_rwlock.reader_lock:
                    # peers = storage_peer_manager.peers
                peers_res = await spm_client.get_replicas(bucket_id=bucket_id,key=key)
                if peers_res.is_err:
                    self.log(str(peers_res.unwrap_err()))
                    raise HTTPException("Failed to get replicas")
                peers = peers_res.unwrap().replicas
                
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
        
        async def memoryview_stream(request:Request,data: memoryview, chunk_size: int = 65536):
            """Efficient async generator to stream memoryview in large chunks."""
            for i in range(0, len(data), chunk_size):
                if await request.is_disconnected():
                    raise HTTPException(status_code=499, detail="Client disconnected")
                chunk = bytes(data[i:i + chunk_size])
                yield chunk  # ✅ Yield memoryview slices directly


        @disconnect_protected()
        @self.router.get("/api/v4/buckets/{bucket_id}/{ball_id}/merge")
        async def get_data_merged(
            bucket_id:str,
            ball_id:str,
            background_tasks:BackgroundTasks,
            content_type:str = "",
            chunk_size:Annotated[Union[str,None], Header()]="10mb",
            peer_id:Annotated[Union[str,None], Header()]=None,
            # local_peer_id:Annotated[Union[str,None], Header()]=None,
            filename:str = "",
            attachment:bool = False,
            force_get:Annotated[Union[int,None], Header()] = 1,
            spm_client:SPMClient = Depends(self.get_spm_client),

        ):
            force = bool(force_get)
            chunk_size_bytes = HF.parse_size(chunk_size or "1mb")
            start_time = T.time()

            # Schedule your after-operation logging/cleanup.
            background_tasks.add_task(self.spm_client_close(spm_client))


            # 1) Pick a peer for metadata (either specified or the first chunk)
            if peer_id:
                peer_res = await spm_client.get_peer_by_id(peer_id=peer_id)
            else:
                # This must be change in the future is a hardcoded way to find the first chunk.
                peer_res = await spm_client.get(bucket_id=bucket_id, key=f"{ball_id}_0")

            if peer_res.is_err:
                raise HTTPException(404, "No available peer; try again later.")

            peer = peer_res.unwrap()
            # peer.get_bucket_metadata()

            # 2) Fetch metadata to learn num_chunks
            md_res = await peer.get_metadata(bucket_id=bucket_id, key=f"{ball_id}_0")
            if md_res.is_err:
                raise HTTPException(404, "Could not fetch metadata.")

            metadata = md_res.unwrap()
            num_chunks = int(metadata.metadata.tags.get("num_chunks", 0))
            if num_chunks <= 0:
                raise HTTPException(404, "Invalid number of chunks.")
            # 3) Define our async generator that yields each chunk in turn
            async def __inner_get(ckey:str ):
                max_tries = 10
                tries = 0
                while tries < max_tries:
                    peer_res = await spm_client.get(bucket_id=bucket_id, key=ckey)
                    if peer_res.is_err:
                        tries+=1
                        # spm_client=self.get_spm_client()
                        await asyncio.sleep(2)
                        print(f"Trying[{tries}].... {peer_res.unwrap_err()}")
                        continue
                    else: 
                        return peer_res
            async def merged_stream():
                size = 0 
                for idx in range(num_chunks):
                    chunk_key = f"{ball_id}_{idx}"
                    cache_key = f"{bucket_id}@{chunk_key}"

                    # Try cache if not forcing a fresh fetch
                    if not force:
                        cached_opt = self.cache.get(cache_key)
                        if cached_opt.is_some:
                            shm_data = cached_opt.unwrap()  # SharedMemoryData
                            try:
                                data_bytes = shm_data.get_data().tobytes()
                                size +=len(data_bytes)
                            finally:
                                shm_data.close()
                            yield data_bytes
                            continue

                    # Otherwise fetch from peer
                    peer_res = await __inner_get(chunk_key)
                            
                    if peer_res.is_err:
                        raise HTTPException(404, f"Chunk {chunk_key} not found on any peer.")

                    chunk_peer = peer_res.unwrap()
                    stream_res = await chunk_peer.get_streaming(bucket_id, chunk_key)
                    if stream_res.is_err:
                        raise HTTPException(500, f"Failed streaming chunk {chunk_key}.")

                    resp = stream_res.unwrap()
                    chunk_data = resp.content
                    size+=len(chunk_data)
                    self.cache.put(cache_key, chunk_data)
                    yield chunk_data
                background_tasks.add_task(
                    self.after_operation_task,
                    "GET",
                    bucket_id,
                    ball_id,
                    start_time,
                    size
                )

            # 4) Build the StreamingResponse

            # fstream = await merged_stream()
            end_time = T.time()
            media = content_type or metadata.metadata.content_type or "application/octet-stream"
            response = StreamingResponse(merged_stream(), media_type=media)
            self.log.info({
                "event":"GET.DATA",
                "arrival_time":start_time,
                "end_at":end_time,
                "bucket_id":bucket_id,
                "key":ball_id,
                "force":force,
                "response_time": end_time- start_time 
            })
            if attachment:
                fname = filename or metadata.metadata.tags.get("fullname", f"{ball_id}")
                response.headers["Content-Disposition"] = f'attachment; filename="{fname}"'

            return response

        @disconnect_protected()
        @self.router.get("/api/v4/buckets/{bucket_id}/{key}")
        async def get_data(
            request:Request,
            bucket_id:str,
            key:str,
            background_task:BackgroundTasks,
            content_type:str = "",
            chunk_size:Annotated[Union[str,None], Header()]="10mb",
            peer_id:Annotated[Union[str,None], Header()]=None,
            local_peer_id:Annotated[Union[str,None], Header()]=None,
            filename:str = "",
            attachment:bool = False,
            force_get:Annotated[Union[int,None], Header()] = 0,
            spm_client:SPMClient = Depends(self.get_spm_client),

        ):
            force = bool(force_get)
            chunk_size_bytes= HF.parse_size(chunk_size)
            try:
                start_time             = T.time()
                get_peer_start_time    = T.time_ns()
                peer_id_param_is_empty = peer_id == "" or peer_id == None

                if not (peer_id_param_is_empty):
                    maybe_peer = await spm_client.get_peer_by_id(peer_id=peer_id)
                else:
                    maybe_peer = await spm_client.get(bucket_id=bucket_id,key=key)
                

                if maybe_peer.is_err:
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
                headers = {}
                # > ======================= GET.METADATA ============================
                get_metadata_start_time = T.time_ns()
                metadata_result         = await peer.get_metadata(bucket_id=bucket_id, key=key, headers=headers)
                if metadata_result.is_err:
                    detail = "Fail to fetch metadata from peer {}".format(peer.peer_id)
                    # n_deleted = await spm_client.delete(bucket_id=bucket_id,key=key)
                    self.log.error({
                        "event":"GET.METADATA.FAILED",
                        # "n_deletes":n_deleted.unwrap_or(0),
                        "bucket_id":bucket_id,
                        "key":key,
                        "peer_id":peer.peer_id,
                        "detail":detail,
                        "error":str(metadata_result.unwrap_err())
                    })
                    raise HTTPException(status_code=404, detail=detail)
            
                metadata = metadata_result.unwrap()
                background_task.add_task(self.after_operation_task(
                    operation="GET",bucket_id=bucket_id, key=key, t1 = start_time,size =metadata.metadata.size
                ))
                # > ======================= GET.STREAMING ============================
                get_streaming_start_time = T.time_ns()

                _peer_id       = metadata.peer_id if not peer_id else peer_id
                _local_peer_id = metadata.local_peer_id if not local_peer_id else local_peer_id

                if not force:
                    cache_result = self.cache.get(key=f"{bucket_id}@{key}")

                    if cache_result.is_some:
                        cached_datax =cache_result.unwrap()
                        end_at = T.time()
                        self.log.info({
                            "event":"GET.DATA.CACHING",
                            "arrival_time":start_time,
                            "end_at":end_at,
                            "bucket_id":bucket_id,
                            "key":key,
                            "size":len(cached_datax),
                            "peer_id":_peer_id,
                            "local_peer_id":_local_peer_id,
                            "hit":1,
                            "chunk_size":chunk_size,
                            "force":force,
                            "response_time":end_at - start_time 
                        })
                        media = content_type or metadata.metadata.content_type
                        response = StreamingResponse(memoryview_stream(request=request ,data = cached_datax.get_data(), chunk_size=chunk_size_bytes ), media_type=media)
                        background_task.add_task(cached_datax.close)
                    
                        _filename = metadata.metadata.tags.get("fullname", metadata.metadata.tags.get("filename", "{}_{}".format(bucket_id,key) ) ) if filename == "" else filename
                        
                        if attachment:
                            response.headers["Content-Disposition"] = f"attachment; filename={_filename}" 
                        return response

                result                   = await peer.get_streaming(bucket_id=bucket_id,key=key,headers=headers)
                if result.is_err:
                    self.log.error({
                        "bucket_id":bucket_id,
                        "key":key,
                        "detail": str(result.unwrap_err())
                    })
                    raise HTTPException(status_code=404, detail="{}@{} not found".format(bucket_id,key))
                else:
                    response   = result.unwrap()
                    put_result = self.cache.put(key=f"{bucket_id}@{key}", value=response.content)
                    
                    cg             = Utils.safe_content_generator(peer_resp=response, chunk_size=chunk_size_bytes, logger=self.log,ctx=f"{bucket_id}@{key}")
                    # content_generator(response=response,chunk_size=chunk_size)

                    media_type     = response.headers.get('Content-Type',metadata.metadata.content_type) if content_type == "" else content_type
    # 

                    end_at = T.time()
                    self.log.info({
                        "event":"GET.DATA",
                        "arrival_time":start_time,
                        "end_at":end_at,
                        "bucket_id":bucket_id,
                        "key":key,
                        "size":metadata.metadata.size,
                        "peer_id":_peer_id,
                        "local_peer_id":_local_peer_id,
                        "hit":int(_local_peer_id==_peer_id),
                        "force":force,
                        "response_time":end_at- start_time 
                    })
                    response  = StreamingResponse(content=cg, media_type=media_type,background=background_task)

                    _filename = metadata.metadata.tags.get("fullname", metadata.metadata.tags.get("filename", "{}_{}".format(bucket_id,key) ) ) if filename == "" else filename
                    if attachment:
                        response.headers["Content-Disposition"] = f"attachment; filename={_filename}" 


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
        
        
      

        # @disconnect_protected()
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


        # @disconnect_protected()
        @self.router.delete("/api/v4/buckets/{bucket_id}/{key}")
        async def delete_data_by_key(
            bucket_id:str,
            key:str,
            spm_client:SPMClient = Depends(self.get_spm_client),
            force:Annotated[int, Header()] = 1
        ):
                arrival_time = T.time()
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
                        detail = f"{bucket_id}@{key} not found"
                        status_code = 404
                        self.log.error({
                            "detail":detail,
                            "status_code":status_code,
                        })
                        raise HTTPException(status_code=status_code, detail=detail  )

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
                    c_result = self.cache.remove(key=f"{bucket_id}@{key}")
                    timestamp     = T.time_ns()
                    res           = await spm_client.delete(bucket_id=bucket_id,key=key)
                    end_at = T.time()
                    response_time = end_at - start_time
                    self.log.info({
                        "event":"DELETED.BY.KEY",
                        "arrival_time":arrival_time,
                        "end_at":end_at,
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

        # @disconnect_protected()
        @self.router.delete("/api/v4/buckets/{bucket_id}/bid/{ball_id}")
        async def delete_data_by_ball_id(
            bucket_id:str,
            ball_id:str,
            spm_client:SPMClient = Depends(self.get_spm_client), 
            force:Annotated[int, Header()] = 1,
        ):

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
                                    self.cache.remove(key=f"{bucket_id}@{metadata.key}")
                                    del_response = del_result.unwrap()
                                    if del_response.n_deletes>=0:
                                        default_del_by_ball_id_response.n_deletes+= del_response.n_deletes
                            
                                res_del = await spm_client.delete(bucket_id=bucket_id, key=ball_id, peer_ids=[peer.peer_id])
                                self.log.debug({
                                    "event":"DELETE.BY.BALL_ID",
                                    "bucket_id":_bucket_id,
                                    "ball_id":_ball_id,
                                    "key":metadata.key,
                                    "peer_id":peer.peer_id,
                                    "status":int(del_result.is_ok)-1,
                                    "ok":res_del.is_ok,
                                    "service_time":service_time,
                                })


                    if combined_key == "" or len(combined_key) ==0:
                        self.log.error({
                            "event":"NOT.FOUND",
                            "bucket_id":_bucket_id,
                            "ball_id":_ball_id,
                        })
                        return JSONResponse(content=jsonable_encoder(default_del_by_ball_id_response.model_dump()))
                    
                    end_at = T.time()
                    self.log.info({
                        "event":"DELETED.BY.BALL_ID",
                        "arrival_time":start_time,
                        "end_at":end_at,
                        "bucket_id":_bucket_id,
                        "ball_id":_ball_id,
                        "n_deletes": default_del_by_ball_id_response.n_deletes,
                        "force":bool(force),
                        "response_time":T.time() - _start_time,
                    })
                    return JSONResponse(content=jsonable_encoder(default_del_by_ball_id_response))
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
