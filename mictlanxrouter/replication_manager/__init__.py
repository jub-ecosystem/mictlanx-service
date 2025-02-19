from mictlanx.logger.log import Log
from option import Option,Some,NONE,Result,Ok,Err
from mictlanxrouter.dto.data_replication import ReplicationEvent,ReplicationTask,ReplicationTaskStatus,ReplicationProcessResponse
import time as T
import aiorwlock 
import humanfriendly as HF
import mictlanx.v4.interfaces as InterfaceX
from mictlanxrouter.peer_manager.healer import StoragePeerManager
from typing import List,Dict,Tuple,Callable,Any,Awaitable
import aiorwlock
import asyncio
import string
# from mictlanx.utils.index import Utils as MXUtils
import json as J
import os 
from opentelemetry.trace import Span,StatusCode,Status
from opentelemetry.sdk.trace import Tracer


MICTLANX_TIMEOUT = HF.parse_timespan(os.environ.get("MICTLANX_TIMEOUT","10s"))

ALPHABET = string.digits + string.ascii_lowercase

class Pagination(object):
    def __init__(self,n:int,batch_size:int = 10):
        self.n                = n
        self.batch_size       = batch_size
        self.__completed      = False
        self.current_position = 0
        self.counter          = 0
        self.percentage       = 0.0
        self.end              = batch_size

    def reset(self,n:int= -1):
        self.n = self.n if n <= 0 else n
        self.current_position = 0
        self.__completed = False
        self.counter  = 0
        self.end  = self.batch_size
    def soft_next(self):
        return self.current_position, self.end, self.__completed
    def next(self):
        # if self.completed:
            # return self.current_position,,self.__completed
            # self.current_position = 0
            # self.completed = False

        # if self.counter ==0:
        # start = self.current_position
        # start = self.end % self.n
        if self.n ==0:
            return self.current_position, self.end , self.__completed
        
        self.current_position = self.end % self.n
        self.end = min(self.current_position + self.batch_size, self.n)
        
        # self.end = end
        
        if self.end == self.n:
            self.__completed = False if self.counter ==0 else True
        # else:
            # self.current_position = self.end % self.n
        
        self.counter += 1

        self.percentage = 0.0 if self.n ==0  else 100.0 if self.__completed else float(100*self.current_position)/float(self.n)
        # self.percentage =  
        return self.current_position, self.end, self.__completed
    
    def prev(self):
        if self.batch_size==0:
            return
        
        if self.current_position == 0:
            start = (self.n // self.batch_size) * self.batch_size
            end = self.n
        else:
            end = self.current_position
            start = max(self.current_position - self.batch_size, 0)
        
        self.current_position = start
        self.__completed = False
        
        return start, end, self.__completed
    def is_completed(self):
        return self.__completed
    def to_dict(self):
        return self.__dict__
    def __str__(self):
        # end = min(self.current_position + self.batch_size, self.n)
        x = self.__dict__
        # x["end"]=end 
        return J.dumps(x,indent=4)
class ReplicaManagerParams(object):
    def __init__(self, 
        queue_max_idle_timeout:str="30s",
        heartbeat_t:str = "5s",
        batch_size:int = 1000,
        batch_index:int = 0,
        current_start_index: int = 0,
        current_end_index: int = -1,
        total_n_balls:int = -1,
        local_n_balls_map:Dict[str,int ] = {},
        elastic:bool = False,
        paginations:Dict[str, Pagination] = {}
    ):
        self.queue_max_idle_timeout = queue_max_idle_timeout
        self.heartbeat_timeout      = heartbeat_t
        self.batch_size             = batch_size
        self.batch_index            = batch_index
        self.current_start_index    = current_start_index
        self.current_end_index      = current_end_index
        self.total_n_balls          = total_n_balls
        self.local_n_balls_map      = local_n_balls_map
        self.elastic                = elastic
        self.paginations            = paginations
        self.__last_update_at       = T.time()
    def elapsed_time_last_update(self):
        return HF.format_timespan(T.time() - self.__last_update_at)
    def update(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
    def check(self, rmp: 'ReplicaManagerParams') -> Option['ReplicaManagerParams']:
        if (self.queue_max_idle_timeout != rmp.queue_max_idle_timeout or
            self.heartbeat_timeout != rmp.heartbeat_timeout or
            self.batch_size != rmp.batch_size or
            self.batch_index != rmp.batch_index or
            self.current_start_index != rmp.current_start_index or
            self.current_end_index != rmp.current_end_index or
            self.total_n_balls != rmp.total_n_balls or
            self.local_n_balls_map != rmp.local_n_balls_map or
            self.elastic != rmp.elastic 
            ):
            self.__last_update_at = T.time()
            return Some(rmp)
        return NONE
    def __str__(self):
        return J.dumps(self.__dict__,indent=4)

class CreateReplicasResult(object):
    def __init__(self, bucket_id:str,key:str, success_replicas:List[str]=[], failed_replicas:List[str]=[],available_replicas:List[str]=[], replicas:List[str]=[], no_found_peers:List[str]=[]):
        self.bucket_id          = bucket_id
        self.key                = key
        self.current_replicas   = replicas
        self.success_replicas   = success_replicas
        self.failed_replicas    = failed_replicas
        self.available_replicas = available_replicas
        self.no_found_peers     = no_found_peers
        self.current_rf         = len(replicas)
        self.max_rf             = self.current_rf + len(available_replicas)
    @staticmethod
    def empty(bucket_id:str, key:str,current_replicas:List[str]=[])->'CreateReplicasResult':
        return CreateReplicasResult(bucket_id=bucket_id,key=key,replicas=current_replicas)
    
    def __str__(self):
        return "CreateReplicasResult(bucket_id = {}, key = {}, replicas ={}, success_replicas= {}, failed_replicas= {}, available_replicas={}, no_found_peers={})".format(
            self.bucket_id,
            self.key,
            self.current_replicas,
            self.success_replicas,
            self.failed_replicas,
            self.available_replicas,
            self.no_found_peers
        )



class ReplicaManager(object):
    def __init__(self,
                 tracer:Tracer,
                 q:asyncio.Queue,
                 spm:StoragePeerManager,
                 elastic:bool= True,
                 show_logs:bool = True,
                 params:Option[ReplicaManagerParams] = NONE, 
    ):
        # <BucketID>@<Key> -> List[PeerId]
        self.tracer:Tracer =tracer
        self.params = params.unwrap() if params.is_some else ReplicaManagerParams()
        self.q = q 
        self.elastic = elastic
        self.replica_map:Dict[str, List[str]] = {}
        self.spm = spm
        self.params_lock = aiorwlock.RWLock(fast=True)
        self.lock        = aiorwlock.RWLock(fast=True)
        self.get_lock    = aiorwlock.RWLock(fast=True)
        self.__log = Log(
            name = "mictlanx-router-rm-0",
            console_handler_filter=lambda x: show_logs,
            interval=24,
            when="h"
        )
        # peer_id.bucket_id.key -> # access
        self.access_replica_map:Dict[str,int]= {}
        self.access_map_queue =  asyncio.Queue()
        self.access_map_task = asyncio.create_task(self.access_map_worker())
        self.EVENT_HANDLERS:Dict[str, Callable[[Dict[str, Any]], Awaitable[None]]] = {
            "EXTENDED_ACCESS_MAP":self.extended_access_map_handler,
            "ACCESSED":self.access_handler
        }
    async def access_handler(self,params:Dict[str,Any]): 
        start_time       = T.time()
        bucket_id        = params.get("bucket_id","")
        key              = params.get("key","")
        peer_id          = params.get("peer_id","")
        ok = False
        if not (bucket_id == "" or key =="" or peer_id ==""):
            ok = True
            await self.__access_increment_async(bucket_id=bucket_id,key=key, peer_id=peer_id)
        self.__log.info({
            "event":"ACCESSED",
            "ok":ok,
            "bucket_id":bucket_id,
            "key":key,
            "peer_id":peer_id,
            "service_time":T.time() - start_time
        })
    async def extended_access_map_handler(self,params:Dict[str,Any]):
        start_time = T.time()
        access_map:Dict[str,int] = params.get("access_map",{})
        if len(access_map.keys()) ==0:
            return -1
        await self.__extend_access_map_async(access_map=access_map )
        self.__log.info({
            "event":"EXTENDED_ACCESS_MAP",
            "n_balls": len(access_map),
            "service_time": T.time() - start_time
        })

    async def access_map_worker(self):
        while True:
            event = await self.access_map_queue.get()
            # print("EVENT", event)
            event_type = event.get("type")
            event_params = event.get("params",{})
            handler = self.EVENT_HANDLERS.get(event_type)
            # print("HANDLER", handler)
            if handler:
                await handler(event_params)
            # if event
            await asyncio.sleep(1)

    # async def extend
    async def get_params(self,)->ReplicaManagerParams:
        async with self.params_lock.reader_lock:
            return self.params
    async def update_params(self,**kwargs):
        async with self.params_lock.writer_lock:
            self.params.update(**kwargs)
        
    async def get_replica_map(self):
        async with  self.lock.reader_lock:
            return self.replica_map
    async def get_access_replica_map(self):
        async with self.get_lock.reader_lock:
            return self.access_replica_map
    
    async def extend_access_map(self, access_map:Dict[str,int]={}):
        async with self.get_lock.writer_lock:
            for key in access_map.keys() | self.access_replica_map.keys():
                value = access_map.get(key,0) + self.access_replica_map.get(key,0)
                self.access_replica_map[key] = value
    async def __extend_access_map_async(self, access_map:Dict[str,int]={}):
        for key in access_map.keys() | self.access_replica_map.keys():
            value = access_map.get(key,0) + self.access_replica_map.get(key,0)
            self.access_replica_map[key] = value
        print(self.access_replica_map)


    async def __access_increment_async(self,bucket_id:str, key:str,peer_id:str)->None:
        k       = "{}.{}.{}".format(peer_id,bucket_id,key)
        x       = self.access_replica_map.setdefault(k,0)
        self.access_replica_map[k] = x + 1


    async def access(self,bucket_id:str, key:str)->Option[InterfaceX.Peer]:
        min_access_replicas = await self.get_min_accessed_replica(bucket_id=bucket_id,key=key)
        self.__log.debug({
            "event":"ACCESS.INNER",
            "bucket_id":bucket_id,
            "key":key,
            "min_access_replicas":min_access_replicas.map(lambda x : x.peer_id).unwrap_or("")
        })
        if min_access_replicas.is_none:
            return NONE
        
        peer = min_access_replicas.unwrap()
        await self.access_map_queue.put({"type":"ACCESSED", "params":{"bucket_id":bucket_id, "key":key, "peer_id": peer.peer_id}})
        # async with self.get_lock.writer_lock:
        #     peer_id = peer.peer_id
        #     k = "{}.{}.{}".format(peer_id,bucket_id,key)
        #     x = self.access_replica_map.setdefault(k,0)
        #     self.access_replica_map[k] = x + 1
        return min_access_replicas
    async def get_min_accessed_replica(self,bucket_id:str,key:str)->Option[InterfaceX.Peer]:
        replicas    = await self.get_current_replicas(bucket_id=bucket_id, key=key)
        min_replica = -1
        min_val = -0
        if len(replicas)==1:
            return Some(replicas[0])
        
        async with self.get_lock.reader_lock:
            for r in replicas:
                k = "{}.{}.{}".format(r.peer_id,bucket_id,key)
                if min_replica == -1:
                    min_replica = r
                    min_val = self.access_replica_map.setdefault(k,0)
                else:
                    current_min_val = self.access_replica_map.setdefault(k,0)
                    if current_min_val< min_val:
                        min_replica = r
            return Some(min_replica) if min_replica != -1 else NONE
                    

                

    
    async def get_available_peers_ids(self,bucket_id:str,key:str,size:int=0):
        with self.tracer.start_as_current_span("rm.get.available.peers.ids") as span:
            span:Span = span
            _all_peers_ids = await self.spm.sorted_by_uf(size=size)
            span.add_event(name = "rm.spm.sorted_by_uf", attributes={"available_peer_ids": _all_peers_ids})
            async with self.lock.reader_lock:
                combined_key         = "{}@{}".format(bucket_id,key)
                span.set_attributes({"combined_key": combined_key,"bucket_id":bucket_id, "key":key})
                current_replicas = set(self.replica_map.setdefault(combined_key,[]))
                result_list = [item for item in _all_peers_ids if item not in current_replicas]
                span.add_event(name="rm.current_replicas", attributes={"current_replicas":list(current_replicas)})
                span.add_event(name="rm.filtered.available_replicas", attributes={"available_replicas":result_list })
                return result_list
            # return list(_all_peers_ids.difference(current_replicas))
    async def get_available_peers(self,bucket_id:str,key:str)->List[InterfaceX.Peer]:
        try:
            xs = await self.get_available_peers_ids(bucket_id=bucket_id,key=key)
            return await self.spm.from_peer_ids_to_peers(peer_ids=xs)
            # available_peers = [await self.spm.get_peer_by_id(peer_id=peer_id) for peer_id in await xs]
            # return list(map(lambda op:op.unwrap(),filter(lambda p: p.is_some,available_peers)))
        except Exception as e:
            return []

    # 
    def get_replicated_peers(self,bucket_id:str,key:str):
        combined_key     = "{}@{}".format(bucket_id,key)
        current_replicas = self.replica_map.setdefault(combined_key,[])
        return list(current_replicas)

    async def remove_replicas(self,bucket_id:str,key:str)->List[str]:
        combined_key = "{}@{}".format(bucket_id,key)
        async with self.lock.writer_lock:
            if combined_key in self.replica_map:
                res = self.replica_map.pop(combined_key)
            else:
                res = []
        
        async with self.get_lock.writer_lock:
            for p in res:
                k = "{}.{}.{}".format(p,bucket_id,key)
                if k in self.access_replica_map:
                    del self.access_replica_map[k]
            return res
            # return []

    async def get_current_replicas(self,bucket_id:str, key:str)->List[InterfaceX.Peer]:
        combined_key    = "{}@{}".format(bucket_id,key)
        # available_peers = await self.spm.get_available_peers()
        async with self.lock.reader_lock:
            replicas = self.replica_map.get(combined_key,[])
            maybe_replicas = [await self.spm.get_peer_by_id(peer_id=i) for i in replicas]
            return list(map(lambda x:x.unwrap(),filter(lambda mr:mr.is_some, maybe_replicas)))

    async def get_current_replicas_ids(self,bucket_id:str, key:str)->List[str]:
        with self.tracer.start_as_current_span("rm.get.current.replicas.ids") as span:
            span:Span = span
            combined_key    = "{}@{}".format(bucket_id,key)
            async with self.lock.reader_lock:
                replicas = self.replica_map.get(combined_key,[])
                span.set_attributes({"bucket_id":bucket_id, "key":key, "replicas":replicas})
                return replicas
          
    async def __create_replicas(self,bucket_id:str,key:str,replica_peer_ids:List[str]=[]):
        combined_key       = "{}@{}".format(bucket_id,key)
        async with self.lock.writer_lock:
            if len(replica_peer_ids) >0:
                self.replica_map[combined_key]= list(replica_peer_ids)
                return True
            else:
                return False
    async def current_rf(self,bucket_id:str,key:str)->int:
        # async with self.lock.reader_lock:
            current = len(await self.get_current_replicas_ids(bucket_id=bucket_id,key=key))
                # self.key_replicas.get("{}@{}".format(bucket_id,key),[]))
            return current
    async def create_replicas(self, bucket_id:str,key:str,size:int=0,rf:int = 1, peer_ids:List[str]=[])->CreateReplicasResult :
        start_time         = T.time()
        available_peer_ids = await self.get_available_peers_ids(
            bucket_id=bucket_id,
            key=key,
            size=size
        )
        
        if len(available_peer_ids) ==0:
            return CreateReplicasResult.empty(
                bucket_id=bucket_id,
                key=key
            )
        n_selected_peers              = len(peer_ids)
        n_available_peers             = len(available_peer_ids)
        current_replicas_ids          = await self.get_current_replicas_ids(bucket_id=bucket_id,key=key)
        target_selected_peers_ids = list(filter(lambda x:  not x in current_replicas_ids, peer_ids))
        
        current_rf                          = len(current_replicas_ids)
        _rf                                  = rf if n_selected_peers == 0 else len(target_selected_peers_ids)
        diff_rf                             = _rf - current_rf if _rf > current_rf else 0
        if _rf <=  current_rf:
            return CreateReplicasResult.empty(
                bucket_id=bucket_id,
                key=key,
                current_replicas=current_replicas_ids
            )

  
        
        if  diff_rf == 0:
            replicas  = set(peer_ids).union(set(current_replicas_ids))
  
            return CreateReplicasResult(
                bucket_id=bucket_id,
                key=key,
                success_replicas=[],
                failed_replicas=[],
                available_replicas=available_peer_ids,
                no_found_peers=[],
                replicas=current_replicas_ids
            )
        

        # No selected peers and RF is lower than the number of available peers
        if n_selected_peers == 0 and diff_rf <= n_available_peers:
            replica_peer_ids = available_peer_ids[:diff_rf]
            # ____________________________________
            x = await self.__create_replicas(
                bucket_id=bucket_id,
                key=key,
                replica_peer_ids=replica_peer_ids
            )
            # UPDATE UFS
            await self.spm.puts(peer_ids=replica_peer_ids, size = size)
            replicas = list(set(current_replicas_ids+ replica_peer_ids))
            __available_replicas = await self.get_available_peers_ids(bucket_id=bucket_id,key=key)
           
            return CreateReplicasResult(
                bucket_id          = bucket_id,
                key                = key,
                replicas           = replicas,
                success_replicas   = replica_peer_ids,
                available_replicas = __available_replicas,
                failed_replicas    = [],
                no_found_peers     = []
            )
        # No selected peers and RF is greater than number of availabler peers
        elif n_selected_peers ==0  and diff_rf > n_available_peers:
            if self.elastic:
                res = await self.spm.active_deploy_peers(rf=diff_rf - n_available_peers)
                replicas = list(set(current_replicas_ids+res.success_peers))
                # res.success_peers
                x = await self.__create_replicas(bucket_id=bucket_id,key=key, replica_peer_ids=replicas)
           
                return CreateReplicasResult(
                    bucket_id=bucket_id,
                    key= key,
                    replicas=replicas,
                    failed_replicas=res.failed_peers,
                    available_replicas = await self.get_available_peers_ids(bucket_id=bucket_id,key=key),
                    success_replicas=res.success_peers,

                )
            else:

                return CreateReplicasResult(
                    bucket_id=bucket_id,
                    key= key,
                    replicas=current_replicas_ids,
                )
            
        elif n_selected_peers >=1 and diff_rf <= n_available_peers:
            # if diff_rf <= n_selected_peers:
            selected_peers     = await self.spm.from_peer_ids_to_peers(peer_ids=target_selected_peers_ids)
            _selected_peer_ids = set(list(map(lambda p: p.peer_id, selected_peers)))

            new_replicas_peer_ids = _selected_peer_ids.difference(set(current_replicas_ids)) 
            new_current_replicas = new_replicas_peer_ids.union(current_replicas_ids)

            replica_peer_ids = list(new_current_replicas)
            create_replicas_result = await self.__create_replicas(bucket_id=bucket_id,key=key,replica_peer_ids=replica_peer_ids)
            replicas = await self.get_current_replicas_ids(bucket_id=bucket_id,key=key)

            
            return CreateReplicasResult(
                bucket_id=bucket_id,
                key=key,
                available_replicas=await self.get_available_peers_ids(bucket_id=bucket_id,key=key),
                failed_replicas=[],
                no_found_peers=[],
                replicas=replicas,
                success_replicas=replica_peer_ids,
            )
        
          
        elif n_selected_peers >=1  and diff_rf > n_available_peers:
            if not self.elastic:
              
                return CreateReplicasResult(
                    bucket_id=bucket_id,
                    key=key,
                    available_replicas=[],
                    failed_replicas=[],
                    no_found_peers=[],
                    replicas=current_replicas_ids
                )
            else:
                return CreateReplicasResult.empty(bucket_id=bucket_id,key=key, current_replicas=current_replicas_ids)

        





class DataReplicator:
    def __init__(self,
        tracer:Tracer,
        queue:asyncio.Queue,
        rm:ReplicaManager,
        strategy:str = "FLOOD",
        strategy_mode:str="STATIC",
        show_logs:bool=True,
        chunk_size:int = 100,
        elastic:bool = False,
        max_fails:int = 5
    ) -> None:
        
        self.is_running = True
        self.tracer = tracer
        self.q = queue
        # self.ph = ph
        self.api_version = 4
        self.__log = Log(
            name = "mictlanx-router-replicator-0",
            console_handler_filter=lambda x: show_logs,
            interval=24,
            when="h"
        )
        self.to_replicate = []
        self.replicated_objects = {}
        self.strategy = strategy
        self.strategy_mode = strategy_mode
        self.chunk_size = chunk_size
        self.rm = rm
        self.elastic = elastic
        self.tasks = []
        # task_id 
        self.tasks_map:Dict[str, List[ReplicationTask]] ={}
        self.completed_tasks:Dict[str, List[ReplicationTask]] = {}
        self.lock = aiorwlock.RWLock(fast=True)
        self.max_fails = max_fails
    
    async def drain_tasks(self):
        to_remove:List[Tuple[str,str]] = []
        try:
            # print("DRAINING_TASKS")
            # async with self.lock.reader_lock:
            tasks_map = await self.get_tasks()
            for k, tasks in tasks_map.items():
                bucket_id, key = k.split("@")
                is_not_completed = next(filter(lambda t: t.status == ReplicationTaskStatus.COMPLETED, tasks),None)
                fails = list(filter(lambda t:t.status == ReplicationTaskStatus.FAILED, tasks))
                n_fails = len(fails)
                maybe_latest_task = await self.get_current_task(bucket_id=bucket_id,key=key)
                if maybe_latest_task.is_none:
                    to_remove.append((bucket_id, key))
                    # await self.remove_task(bucket_id=bucket_id,key=key)
                    continue
                latest_task = maybe_latest_task.unwrap()


                if not is_not_completed:
                    self.completed_tasks.setdefault(k, tasks)
                    to_remove.append((bucket_id, key))
                    # await self.remove_task(bucket_id=bucket_id,key=key)
                    continue
                elif n_fails >= self.max_fails:
                    x = latest_task.link_task(status=ReplicationTaskStatus.CANCELLED, detail="Max number of fails reached.")
                    _tasks = tasks.copy()
                    _tasks.append(x)
                    self.completed_tasks.setdefault(k, _tasks)
                    to_remove.append((bucket_id,key))
                    # await self.remove_task(bucket_id=bucket_id,key=key)
                    self.__log.error({
                        "event":"MAX.FAILS.REACHED",
                        "bucket_id":bucket_id,
                        "key":key,
                    })
                    continue
                    

                maybe_task = await self.move_task(bucket_id=bucket_id, key=key,status=ReplicationTaskStatus.PENDING, detail="Try to recover.")
                if maybe_task.is_none:
                    continue
                task = maybe_task.unwrap()
                
                revent = ReplicationEvent.from_replication_task(x = task.link_task(status=ReplicationTaskStatus.PENDING))
                revent.force = True
                self.q.put_nowait(revent)
            return Ok(True)
        except Exception as e:
            self.__log.error({
                "event":"DRAIN.TASKS.FAILED",
                "detail":str(e)
            })
            return Err(e)
        finally:
            for (bucket_id, key) in to_remove:
                res = await self.remove_task(bucket_id=bucket_id,key=key)
                self.__log.debug({
                    "event":"REMOVED.TASKS",
                    "bucket_id":bucket_id,
                    "key":key,
                    "tasks":len(res)
                })
                # print("ENQUEUE AGAIN {}".format(k))
        
    async def get_tasks(self,)->Dict[str, List[ReplicationTask]]:
        async with self.lock.reader_lock:
            return self.tasks_map
    async def remove_task(self,bucket_id:str,key:str)->List[ReplicationTask]:
        with self.tracer.start_as_current_span("remove.task") as span:
            span:Span=  span
            async with self.lock.writer_lock:
                ckey = "{}@{}".format(bucket_id, key)
                if ckey in self.tasks_map:
                    x = self.tasks_map.pop(ckey)
                    span.add_event(name="task.found", attributes={"bucket_id":bucket_id,"key":key, "n_tasks":len(x)})
                    return x
                span.add_event(name="task.not.found", attributes={"bucket_id":bucket_id,"key":key})
                return []
    async def put_task(self, event:ReplicationEvent):
        # async with self.lock.writer_lock:
            # task = ReplicationTask.from_replication_event(x= event)
            # self.tasks.append(task)
        self.q.put_nowait(event)
    async def __replicate(self,bucket_id:str,key:str, peer:InterfaceX.Peer):
        with self.tracer.start_as_current_span("dr.replicate") as span:
            span:Span = span
            # start_time = T.time()
            x          = peer.replicate(bucket_id=bucket_id,key=key,timeout=60,headers={})
            if x.is_err:
                span.add_event(name="replicate.failed", attributes={"bucket_id":bucket_id,"key":key,"peer_id":peer.peer_id})
                # self.__log.error({
                #     "event":"REPLICATE.FAILED",
                #     "msg":str(x.unwrap_err())
                # })
                return False
            else:
                # self.__log.info({
                #     "event":"REPLICATED",
                #     "bucket_id":bucket_id,
                #     "key":key,
                #     "peer_id":peer.peer_id,
                #     "strategy":self.strategy,
                #     "mode":self.strategy_mode,
                #     "ok":x.is_ok,
                #     "response_time": T.time() - start_time,
                # })
                span.add_event(name="replicate.completed", attributes={"bucket_id":bucket_id,"key":key,"peer_id":peer.peer_id})

                return True


    async def add_task(self,task:ReplicationTask)->Option[str]:
        with self.tracer.start_as_current_span("dr.add.task") as span:
            span:Span = span
            async with self.lock.writer_lock:
                if not task.replication_event_id in self.tasks_map:
                    self.tasks_map.setdefault(task.replication_event_id,[])
                    self.tasks_map[task.replication_event_id].append(task)
                    span.add_event(name="dr.added.task",attributes={
                        "n_tasks":len(self.tasks_map),
                        **task.to_dict()
                    })
                    return Some(task.replication_event_id)
                span.add_event(name="dr.task.exists",attributes={
                    **task.to_dict()
                })
                return NONE
    async def move_task(self,bucket_id:str, key:str,status:ReplicationTaskStatus,detail:str="")->Option[ReplicationTask]:
        with self.tracer.start_as_current_span("dr.move.task") as span:
            span:Span = span
            async with self.lock.writer_lock:
                maybe_task = await self.get_current_task(bucket_id=bucket_id, key=key)
                if maybe_task.is_some:
                    task = maybe_task.unwrap()
                    linked_task = task.link_task(status=status,detail=detail)
                    self.tasks_map[task.replication_event_id].append(linked_task)
                    span.add_event(name="task.found", attributes={"bucket_id":bucket_id,"key":key,"detail":detail,"status":str(status), **task.to_dict() })
                    return Some(linked_task)
                span.add_event(name="task.not.found", attributes={"bucket_id":bucket_id,"key":key,"detail":detail,"status":str(status)})
                return maybe_task
    async def get_current_task(self,bucket_id:str,key:str)->Option[ReplicationTask]:
        with self.tracer.start_as_current_span("dr.get.current.task") as span:
            span:Span = span
            async with self.lock.reader_lock:
                ckey = "{}@{}".format(bucket_id,key)
                if ckey in self.tasks_map:
                    tasks = self.tasks_map[ckey]
                    if len(tasks)==0:
                        span.add_event(name="task.not.found.empty", attributes={"ckey":ckey})
                        return NONE
                    last_task = max(tasks, key= lambda x: x.index)
                    span.add_event(name="task.found", attributes={"ckey":ckey, **last_task.to_dict()})
                    return Some(last_task)
                span.add_event(name="task.not.found", attributes={"ckey":ckey})
                return NONE
    async def replication_process(
        self,
        bucket_id:str,
        key:str,
        rf:int=1,
        from_peer_id:str="",
        available_peers_ids:List[str]=[],
        replicated_peers_ids:List[str]=[]
    )->Result[ReplicationProcessResponse,Exception]:
        # replicas_subetset_from_available_peer_ids = set(replicated_peers_ids).difference(available_peers_ids)
        with self.tracer.start_as_current_span("dr.replication.process") as span:
            span:Span                                    = span 
            span.set_attributes({"bucket_id":bucket_id,"key":key,"rf":rf})
            available_peers_ids                          = list(set(available_peers_ids).difference(set(replicated_peers_ids)))
            current_rf                                   = len(replicated_peers_ids)
            ball_size                                    = 0
            local_current_replicated_peers_ids:List[str] = replicated_peers_ids.copy()
            diff_rf = rf - current_rf if rf > current_rf else 0
            # self.__log.debug({
            #     "event":"REPLICATION.PROCESS",
            #     "available_peers":available_peers_ids,
            #     "replicas":local_current_replicated_peers_ids,
            #     "current_rf":current_rf,
            # })
            span.add_event(name="replication.process.init",attributes={
                "available": available_peers_ids,"current_rf":current_rf,
                "bucket_id":bucket_id,
                "key":key,
                "rf":rf,
                "current_rf":current_rf,
                "diff_rf":diff_rf,
                "replicas":replicated_peers_ids
            })
            # T.sleep(100)
            # print("AVAIALBLE_PEER",available_peers_ids)
            # print("CURRENT_RF",cur)
            # available_peers_ids = self.rm.get_available_peers(bucket_id=bucket_id,key=key)
            available_peers:List[InterfaceX.Peer] = []
            for pid in available_peers_ids:
                maybe_peer = await self.rm.spm.get_peer_by_id(peer_id= pid)
                if maybe_peer.is_some:
                    available_peers.append(maybe_peer.unwrap())
            span.add_event(name="replication.process.available",attributes={
                "availables":list(map(lambda x:x.peer_id, available_peers))
            })




            if diff_rf > len(available_peers):
                detail = "Replication factor is greater than available peers."
                span.set_status(status=Status(StatusCode.ERROR))
                span.add_event(name="replica.process.failed",attributes={"detail":detail, "n_availables":len(available_peers), "rf":rf, "current_rf":current_rf,"diff_rf":diff_rf})
                return Err(Exception(detail))
            
            # self.__log.debug({
            #     "event":"REPLICATION.PROCESS",
            #     "bucket_id":bucket_id,
            #     "key":key,
            #     "current_rf":current_rf,
            #     "target_rf":rf,
            #     "diff_rf":diff_rf,
            #     "available_peers":available_peers_ids,
            #     "replicas":replicated_peers_ids,
            # })
            # _rf = len(replicated_peers_ids)


            for peer in available_peers[:diff_rf]:
                get_size_result = peer.get_size(bucket_id=bucket_id, key=key, timeout=MICTLANX_TIMEOUT)
                if get_size_result.is_ok:
                    ball_size_response = get_size_result.unwrap()
                    ball_size          = ball_size_response.size
                    
                    # No replica in this peer
                    if ball_size == 0:
                        res = await self.__replicate(bucket_id=bucket_id, key=key, peer= peer)
                        if res:
                            current_rf+=1
                            local_current_replicated_peers_ids.append(peer.peer_id)
                            span.add_event(
                                name="replication.successfully", 
                                attributes={"current_rf":current_rf, "replicas": local_current_replicated_peers_ids,"peer_id":peer.peer_id}
                            )
                        continue
                    else:
                        # self.__log.debug({
                        #     "event":"REPLICA.EXISTS",
                        #     "peer_id":peer.peer_id,
                        #     "bucket_id":bucket_id,
                        #     "key":key
                        # })
                        local_current_replicated_peers_ids.append(peer.peer_id)
                        current_rf+=1
                        span.add_event(name="replica.exists",attributes={"peer_id":peer.peer_id,"current_rf":current_rf,"replicas": local_current_replicated_peers_ids})
                        continue
                else:
                    detail = str(get_size_result.unwrap_err())
                    span.add_event(name="get.size.failed", attributes={"detail":detail,"peer_id": peer.peer_id})
                    # self.__log.error({
                    #     "event":"GET.SIZE.FAILED",
                    #     "peer_id":peer.peer_id,
                    #     "msg":str(get_size_result.unwrap_err())
                    # })
                    continue

            rm_create_replicas = await self.rm.create_replicas(
                bucket_id=bucket_id,
                key=key,
                peer_ids=local_current_replicated_peers_ids
            )
            
            if len(rm_create_replicas.failed_replicas)>0:
                pass
                # self.__log.error({
                #     "event":"REPLICATION.FAILED",
                #     "bucket_id":bucket_id,
                #     "key":key,
                #     "failed": rm_create_replicas.failed_replicas
                # })
                            
            # _____________________________________________
            left_replicas = rf-current_rf
            # event = "REPLICATION.UNCOMPLETED" if left_replicas >0 else "REPLICATION.COMPLETED"
            span.add_event(name="dr.replication.process.completed",attributes={
                "bucket_id":bucket_id,
                "key":key,
                "left_replicas":left_replicas, 
                "current_rf":current_rf,
                "rf":rf,
                "replicas":local_current_replicated_peers_ids
            })
            # self.__log.debug({
            #     "event":event,
            #     "current_rf":current_rf,
            #     "rf":rf,
            #     "left_replicas": left_replicas,
            #     "peers": []
            #     # "peers": len(self.ph.peers)
            # })
            return Ok(ReplicationProcessResponse(
                bucket_id=bucket_id,
                key=key,
                left_replicas=left_replicas,
                combined_key_str="{}@{}".format(bucket_id,key),
                replicas=local_current_replicated_peers_ids
            ))
    
    
    async def run(self,event:ReplicationEvent)->Result[ReplicationProcessResponse,Exception]:
        with self.tracer.start_as_current_span("dr.run") as span:
            span:Span = span
            bucket_id            = event.bucket_id
            key                  = event.key
            try:
                combined_key         = event.get_combined_key_str()
                replication_task     = ReplicationTask.from_replication_event(x =  event)

                # Starts status in Pending
                add_task_result = await  self.add_task(task=replication_task.link_task(status=ReplicationTaskStatus.PENDING))
                # Change status to In progress
                maybe_replication_task = await self.move_task(bucket_id=bucket_id, key=key,status=ReplicationTaskStatus.IN_PROGRESS)
         
                if add_task_result.is_none and not replication_task.force:
                    span.set_status(status=Status(StatusCode.ERROR))
                    span.add_event(name="dr.task.exists", attributes={**replication_task.to_dict()})
                    return Err(Exception("Replication task {} is already in queue.".format(replication_task.replication_event_id)))
                elif replication_task.force:
                    if maybe_replication_task.is_none:
                        await self.remove_task(bucket_id = replication_task.bucket_id, key = replication_task.key)
                        span.set_status(status=Status(StatusCode.ERROR))
                        span.add_event(name="dr.task.not.found", attributes={"bucket_id":bucket_id, "key":key})
                        return Exception("{}@{} no exists".format(replication_task.bucket_id, replication_task.key))
                    last_replication_task = maybe_replication_task.unwrap()
                else:
                    last_replication_task = replication_task

                available_peers_ids  = await self.rm.get_available_peers_ids(bucket_id=bucket_id,key=key)
                replicated_peers_ids = await self.rm.get_current_replicas_ids(bucket_id=bucket_id,key=key)
                n_replicas           = len(replicated_peers_ids)
                diff_rf              = event.rf - n_replicas if event.rf > n_replicas else 0
                

                span.add_event(name="dr.replication.init", attributes={"bucket_id":bucket_id,"key":key,"available": available_peers_ids,"current": replicated_peers_ids,"n_replicas":n_replicas,"diff_rf":diff_rf})




                if diff_rf == 0:
                    await self.remove_task(bucket_id = replication_task.bucket_id, key = replication_task.key)
                    span.add_event(name="dr.replication.completed",attributes={"bucket_id":bucket_id,"key":key,"available": available_peers_ids,"current": replicated_peers_ids,"n_replicas":n_replicas,"diff_rf":diff_rf} )
                    return Ok(
                        ReplicationProcessResponse(
                            bucket_id=bucket_id,
                            key=key,
                            combined_key_str="{}@{}".format(bucket_id,key),
                            left_replicas=0,
                            replicas= replicated_peers_ids
                        ) 
                    )

                if len(replicated_peers_ids) ==0:
                    detail =  "{} not found".format(event.get_combined_key_str()) 
                    await self.move_task(bucket_id=bucket_id,key=key, status=ReplicationTaskStatus.FAILED,detail=detail)
                    span.add_event(name="dr.task.not.found",attributes={"detail":detail,"bucket_id":bucket_id,"key":key,"available": available_peers_ids,"current": replicated_peers_ids,"n_replicas":n_replicas,"diff_rf":diff_rf })
                    span.set_status(status=Status(StatusCode.ERROR))
                    # self.__log.error({
                    #     "event":"REPLICA.NO.FOUND",
                    #     "bucket_id":bucket_id,
                    #     "key":key,
                    #     "detail":detail,
                    # })
                    return Err(Exception(detail))
                
                elif len(available_peers_ids) == 0 and not self.elastic:
                    detail = "No available peers.".format()
                    await self.move_task(bucket_id=bucket_id,key=key, status=ReplicationTaskStatus.FAILED,detail=detail)
                    span.add_event(name="dr.no.available.peers", attributes={
                        "detail":detail,"bucket_id":bucket_id,"key":key,"available": available_peers_ids,"current": replicated_peers_ids,"n_replicas":n_replicas,"diff_rf":diff_rf 
                    })
                    span.set_status(status=Status(StatusCode.ERROR))
                    # self.__log.error({
                    #     "event":"NO.AVAILABLE.PEERS",
                    #     "bucket_id":bucket_id,
                    #     "key":key,
                    #     "detail":detail,
                    # })
                    return Err(Exception(detail))
                
                elif  diff_rf >= len(available_peers_ids) and not self.elastic:
                    detail ='Elasticity is disabled. No available peers to complete the replication task.' 
                    n_peers = await self.rm.spm.get_len_available_peers()
                    
                    span.add_event(name="dr.elasticity.disabled", attributes={
                        "detail":detail,"bucket_id":bucket_id,"key":key,"available": available_peers_ids,"current": replicated_peers_ids,"n_replicas":n_replicas,"diff_rf":diff_rf 
                    })
                    span.set_status(status=Status(StatusCode.ERROR))
                    # self.__log.error({
                    #     "event":"ELASTICITY.DISABLED",
                    #     "bucket_id":bucket_id,
                    #     "key":key,
                    #     "rf":event.rf,
                    #     "n_available_peers":len(available_peers_ids),
                    #     "elastic":self.elastic,
                    #     "peers":n_peers,
                    #     "target_peers": n_peers + (event.rf-len(replicated_peers_ids)),
                    #     "detail":detail
                    # })
                    await self.move_task(bucket_id=bucket_id,key=key, status=ReplicationTaskStatus.FAILED,detail=detail)
                    return Err(Exception(detail))
                
                
                
                # self.__log.debug({
                #     "event":"DATA.REPLICATOR",
                #     "combined_key":combined_key,
                #     "bucket_id":bucket_id,
                #     "key":key,
                #     "available_peers":available_peers_ids,
                #     "replica_peers":replicated_peers_ids,
                # })
                # T.sleep(100)
                result = await self.replication_process(
                    bucket_id=bucket_id,
                    key=key,
                    rf= event.rf,
                    from_peer_id = event.pivot_peer_id, 
                    available_peers_ids = available_peers_ids,
                    replicated_peers_ids= replicated_peers_ids
                )
                span.add_event(name="dr.completed", attributes={
                    "bucket_id":bucket_id, 
                    "key":key,
                    "available_peers":available_peers_ids,
                    "replica_peers":replicated_peers_ids,
                    "ok":result.is_ok
                })
                # if 
                return result

            except Exception as e:
                span.add_event(name="dr.exception", attributes={"bucket_id":bucket_id,"key":key,"detail":str(e)})
                span.set_status(status=Status(StatusCode.ERROR))
                return Err(e)
   