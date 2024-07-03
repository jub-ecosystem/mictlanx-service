import random as RND
from mictlanx.logger.log import Log
from option import Result,Ok,Err,Option,Some,NONE
import time as T
import humanfriendly as HF
import mictlanx.v4.interfaces as InterfaceX
from mictlanxrouter.peer_manager.healer import StoragePeerManager
from typing import List,Dict,Any,Tuple,Optional
import aiorwlock
import asyncio
from pydantic import BaseModel,Field
from nanoid import generate as nanoid
import string
from dataclasses import dataclass
from mictlanx.utils.index import Utils as MXUtils

ALPHABET = string.digits + string.ascii_lowercase

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
    def empty(bucket_id:str, key:str)->'CreateReplicasResult':
        return CreateReplicasResult(bucket_id=bucket_id,key=key)
    
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


class LoadBalancer(object):
    def __init__(self, algorithm,peer_healer:StoragePeerManager):
        self.algorithm = algorithm
        self.peer_healer:StoragePeerManager = peer_healer
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
            stats = self.peer_healer.get_stats()
            min_uf = -1
            min_uf_peer = None
            min_stats = None
            n = len(list(stats.keys()))
            if n == 0 :
                return None
            
            for peer_id,stats in stats.items():
                uf = MXUtils.calculate_disk_uf(total=stats.total_disk,used=stats.used_disk , size=size)
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
        stats = self.ph.get_stats()
        keys = list(stats.keys())
        if len(keys) == 0 :
            return None
        
        if len(keys) == 1: 
            x:InterfaceX.PeerStats = stats[keys[0]]
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
        
    def lb(self,operation_type:str="get",algorithm:str ="",key:str ="",size:int=0):
        _algorithm = self.algorithm if algorithm == "" else algorithm
        if _algorithm == "SORTING_UF":
            return self.lb_sort_uf(size=size, operation_type=operation_type,key=key)
        elif _algorithm =="ROUND_ROBIN":
            return self.lb_round_robin(operation_type=operation_type)
        elif _algorithm == "2C_UF":
            return self.lb_2c_uf(operation_type=operation_type,size=size,key=key)
        else:
            return self.lb_sort_uf(size=size, operation_type=operation_type,key=key)


class ReplicaManager(object):
    def __init__(self,q:asyncio.Queue,spm:StoragePeerManager,elastic:bool= True,show_logs:bool = True, ):
        # <BucketID>@<Key> -> List[PeerId]
        self.q = q 
        self.elastic = elastic
        self.replica_map:Dict[str, List[str]] = {}
        self.spm = spm
        self.lock = aiorwlock.RWLock(fast=True)
        self.get_lock = aiorwlock.RWLock(fast=True)
        self.__log = Log(
            name = "mictlanx-router-rm-0",
            console_handler_filter=lambda x: show_logs,
            interval=24,
            when="h"
        )
        # peer_id.bucket_id.key -> # access
        self.access_replica_map:Dict[str,int]= {}

    # async def extend
    async def get_access_replica_map(self):
        async with self.get_lock.reader_lock:
            return self.access_replica_map
    
    async def extend_access_map(self, access_map:Dict[str,int]={}):
        async with self.get_lock.writer_lock:
            for key in access_map.keys() | self.access_replica_map.keys():
                value = access_map.get(key,0) + self.access_replica_map.get(key,0)
                self.access_replica_map[key] = value

    async def access(self,bucket_id:str, key:str)->Option[InterfaceX.Peer]:
        min_access_replicas = await self.get_min_accessed_replica(bucket_id=bucket_id,key=key)
        if min_access_replicas.is_none:
            return NONE
        peer = min_access_replicas.unwrap()
        async with self.get_lock.writer_lock:
            peer_id = peer.peer_id
            k = "{}.{}.{}".format(peer_id,bucket_id,key)
            x = self.access_replica_map.setdefault(k,0)
            self.access_replica_map[k] = x + 1
            return min_access_replicas
    async def get_min_accessed_replica(self,bucket_id:str,key:str)->Option[InterfaceX.Peer]:
        replicas    = await self.get_current_replicas(bucket_id=bucket_id, key=key)
        min_replica = -1
        min_val = -0
        print("bucket_id", bucket_id, replicas)
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
        _all_peers_ids = set(await self.spm.sorted_by_uf(size=size) )
        async with self.lock.reader_lock:
            combined_key         = "{}@{}".format(bucket_id,key)
            current_replicas = set(self.replica_map.setdefault(combined_key,[]))
            return list(_all_peers_ids.difference(current_replicas))
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
        combined_key    = "{}@{}".format(bucket_id,key)
        async with self.lock.reader_lock:
            replicas = self.replica_map.get(combined_key,[])
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

        # peer_ids = list(set(peer_ids))
        available_placement_peer_ids_replicas = await self.get_available_peers_ids(
            bucket_id=bucket_id,
            key=key,
            size=size
        )
        n_selected_peers                    = len(peer_ids)
        n_available_placement_peer_replicas = len(available_placement_peer_ids_replicas)
        current_replicas_ids                = await self.get_current_replicas_ids(bucket_id=bucket_id,key=key)
        current_rf                          = len(current_replicas_ids)
        rf                                  = rf if n_selected_peers == 0 else n_selected_peers
        diff_rf                             = rf - current_rf if rf > current_rf else 0

        self.__log.debug({
            "event":"CREATE.REPLICA",
            "bucket_id":bucket_id,
            "key":key,
            "size":size,
            "rf":rf,
            "current_rf":current_rf,
            "diff_rf":diff_rf,
            "current_replicas":current_replicas_ids,
            "available_peers": available_placement_peer_ids_replicas,
            "n_available_peers":n_available_placement_peer_replicas,
            "selected_peers":peer_ids
        })
        if  diff_rf == 0:
            return CreateReplicasResult(
                bucket_id=bucket_id,
                key=key,
                success_replicas=[],
                failed_replicas=[],
                available_replicas=available_placement_peer_ids_replicas,
                no_found_peers=[],
                replicas=current_replicas_ids
            )
        
        # print("CURRENT_RF",current_rf)

        # print(n_selected_peers, rf<= n_available_placement_peer_replicas)
        # No selected peers and RF is lower than the number of available peers
        if n_selected_peers == 0 and diff_rf <= n_available_placement_peer_replicas:
            replica_peer_ids = available_placement_peer_ids_replicas[:diff_rf]
            x = await self.__create_replicas(
                bucket_id=bucket_id,
                key=key,
                replica_peer_ids=replica_peer_ids
            )
            # print("RRES", x)
            return CreateReplicasResult(
                bucket_id          = bucket_id,
                key                = key,
                replicas           = list(set(current_replicas_ids+ replica_peer_ids)),
                success_replicas   = replica_peer_ids,
                available_replicas = await self.get_available_peers_ids(bucket_id=bucket_id,key=key),
                failed_replicas    = [],
                no_found_peers     = []
            )
        # CreateReplicasResult.empty(bucket_id=bucket_id,key=key)
        elif n_selected_peers ==0  and diff_rf > n_available_placement_peer_replicas:
            if self.elastic:
                await self.spm.active_deploy_peers(rf=diff_rf - n_available_placement_peer_replicas)
                return CreateReplicasResult(
                    bucket_id=bucket_id,
                    key= key,
                    replicas=current_replicas_ids,
                )
            else:
                return CreateReplicasResult(
                    bucket_id=bucket_id,
                    key= key,
                    replicas=current_replicas_ids,
                )
            
        elif n_selected_peers >=1 and diff_rf <= n_available_placement_peer_replicas:
            if rf <= n_selected_peers:
                selected_peers     = await self.spm.from_peer_ids_to_peers(peer_ids=peer_ids)
                _selected_peer_ids = set(list(map(lambda p: p.peer_id, selected_peers)))
                # print("CURRENT_REPLI(CAS_D__)", current_replicas_ids)
                if len(current_replicas_ids) ==0:
                    replica_peer_ids = list(_selected_peer_ids)
                    create_replicas_result = await self.__create_replicas(bucket_id=bucket_id,key=key,replica_peer_ids=replica_peer_ids)
                else:
                    new_replicas_peer_ids = _selected_peer_ids.difference(set(current_replicas_ids)) 
                    new_current_replicas = new_replicas_peer_ids.union(current_replicas_ids)
                    replica_peer_ids = list(new_current_replicas)
                    create_replicas_result = await self.__create_replicas(bucket_id=bucket_id,key=key,replica_peer_ids=replica_peer_ids)
                return CreateReplicasResult(
                    bucket_id=bucket_id,
                    key=key,
                    available_replicas=await self.get_available_peers_ids(bucket_id=bucket_id,key=key),
                    failed_replicas=[],
                    no_found_peers=[],
                    replicas=await self.get_current_replicas_ids(bucket_id=bucket_id,key=key),
                    success_replicas=replica_peer_ids,
                )
           
            else:
                available_replicas = self.get_available_peers_ids(bucket_id=bucket_id,key=key)
                replicas =  await self.get_current_replicas_ids(bucket_id=bucket_id,key=key)
                self.__log.warning({
                    "event":"ELASTICITY.DEACTIVATED",
                    "bucket_id":bucket_id,
                    "key":key,
                    "available_replicas":available_replicas,
                    "replicas":replicas
                })
                return CreateReplicasResult(
                    bucket_id=bucket_id,
                    key=key,
                    available_replicas=available_replicas,
                    failed_replicas=[],
                    no_found_peers= [],
                    replicas=replicas,
                    success_replicas=[]
                )
                # print("ELASTIC_SECOND")
        elif n_selected_peers >=1  and diff_rf > n_available_placement_peer_replicas:
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
                print("ELASTIC WITH SELECTEd")

        
        # current replicas of <combined key>
        
        # current_rm_replicas      = set(self.key_replicas.setdefault(combined_key,[]))
        # Select peers 
        # _selected_replicas_peers = set(peer_ids)
        # Current replicas  - selected  = no duplicate peers  = [p1,p2] - [p1] = [p2]
        # diff                     = current_rm_replicas.difference(_selected_replicas_peers)
        #  Current replicas - <no duplicate peers> =  [p1,p2]-[p2]  = [p1]
        # replica_set_diff         = current_rm_replicas - diff

        # self.__log.debug({
        #     "event":"CREATE.REPLICAS",
        #     "bucket_id":bucket_id,
        #     "key":key,
        #     "size":size,
        #     "combined_key":combined_key,
        #     "all_peers":list(_all_peers_ids),
        #     "current_rm_replicas":list(current_rm_replicas),
        #     "selected_replicas":peer_ids,
        #     "diff":list(diff),
        #     "replica_set_diff":list(replica_set_diff)
        # })
        # if len(replica_set_diff ) == 0:
        #     latest_current_replicas = current_rm_replicas.union(_selected_replicas_peers)
        #     no_found_peers          = latest_current_replicas.difference(_all_peers_ids)
        #     available_replicas      = _all_peers_ids.difference(latest_current_replicas)

        #     async with self.lock.writer_lock:
        #         self.key_replicas[combined_key] =  latest_current_replicas
        #     return ReplicationProcessResult(
        #         bucket_id=bucket_id,
        #         key= key,
        #         success_replicas= peer_ids,
        #         replicas= list(latest_current_replicas),
        #         failed_replicas = [],
        #         available_replicas= list(available_replicas),
        #         no_found_peers= list(no_found_peers)
        #     )
        # else:
        #     failed                  = replica_set_diff
        #     success                 = _selected_replicas_peers.difference(diff).difference(failed)
        #     available               = _all_peers_ids.difference(_selected_replicas_peers)
        #     latest_current_replicas = current_rm_replicas.union(success)
        #     no_found_peers          = latest_current_replicas.difference(_all_peers_ids)

        #     async with self.lock.writer_lock:
        #         self.key_replicas[combined_key] =  latest_current_replicas
        #     return ReplicationProcessResult(
        #         bucket_id          = bucket_id,
        #         key                = key,
        #         success_replicas   = list(success),
        #         failed_replicas    = list(failed),
        #         available_replicas = list(available),
        #         replicas           = list(latest_current_replicas),
        #         no_found_peers= list(no_found_peers)
        #     )




class ReplicationEvent(BaseModel):
    id:str = Field(default_factory=lambda: str(nanoid(alphabet= ALPHABET )))
    rtype:Optional[str] = "DATA" 
    rf:int 
    # strategy:Optional[str]=""
    elastic:Optional[str] = "false"
    from_peer_id:Optional[str] =""
    bucket_id:Optional[str]=""
    key:Optional[str]=""
    memory:Optional[int] = 4000000000
    disk:Optional[int]= 40000000000
    workers:Optional[int] = 2
    protocol:Optional[str] = "http"
    strategy:Optional[str] = "ACTIVE"
    ttl:Optional[int] = 1 
    def get_combined_key_str(self):
        return "{}@{}".format(self.bucket_id,self.key)
    # def get_combined_key(self):
        # return H.


@dataclass
class ReplicatedBall:
    bucket_id:str
    key:str
    combined_key:str
    size:int
    replicated_at:float
@dataclass 
class ReplicationProcessResponse:
    bucket_id:str
    key:str
    combined_key_str:str
    left_replicas:int
    replicas:List[str]
    @staticmethod
    def empty():
        return ReplicationProcessResponse(
            bucket_id="",
            key="",
            combined_key_str="",
            left_replicas=0,
            replicas=[]
        )


class Replicator:
    def __init__(self,
        queue:asyncio.Queue,
        ph:StoragePeerManager,
        strategy:str = "FLOOD",
        strategy_mode:str="STATIC",
        show_logs:bool=True,
        chunk_size:int = 100
    ) -> None:
        
        self.is_running = True
        self.queue = queue
        self.ph = ph
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
        self.rm = ReplicaManager(spm=self.ph)
    
    async def __replicate(self,bucket_id:str,key:str, peer:InterfaceX.Peer):
        start_time = T.time()
        x          = peer.replicate(bucket_id=bucket_id,key=key,timeout=60,headers={})
        if x.is_err:
            self.__log.error({
                "event":"REPLICATE.FAILED",
                "msg":str(x.unwrap_err())
            })
            return False
        else:
        # if x.is_ok:
            self.__log.info({
                "event":"REPLICATION.PROCESS",
                "bucket_id":bucket_id,
                "key":key,
                "peer_id":peer.peer_id,
                "strategy":self.strategy,
                "mode":self.strategy_mode,
                "ok":x.is_ok,
                "response_time": T.time() - start_time,
            })
            self.replicated_objects.setdefault("{}@{}".format(bucket_id,key) ,{
                "bucket_id":bucket_id,
                "key":key,
                "replicated_at":T.time()
            })
            return True
        # else:
            #  return False
    

    async def replication_process(self,bucket_id:str,key:str,rf:int=1,from_peer_id:str="",available_peers_ids:List[str]=[], replicated_peers_ids:List[str]=[])->ReplicationProcessResponse:
        current_replicas_counter   = len(replicated_peers_ids)
        ball_size                  = 0
        local_current_replicated_peers_ids:List[str] = replicated_peers_ids.copy()
        # available_peers_ids = self.rm.get_available_peers(bucket_id=bucket_id,key=key)
        available_peers = list(
            map(
                lambda p:p.unwrap(),
                filter(
                    lambda pop: pop.is_some,
                    map(
                        lambda pid: self.ph.get_peer_by_id(peer_id=pid),
                        available_peers_ids
                    )
                )
            )
        )
        
        self.__log.debug({
            "event":"REPLICATION.PROCESS",
            "bucket_id":bucket_id,
            "key":key,
            "rf":rf,
            "from_peer_id":from_peer_id,
            "available_peers":available_peers_ids,
            "replicated_peers":replicated_peers_ids,
        })
        for peer in available_peers:
            # if current_replicas_counter == rf:
            #     self.rm.create_replicas(
            #         bucket_id=bucket_id,
            #         key=key,
            #         selected_replicas=local_current_replicated_peers_ids
            #     )
            #     return ReplicationProcessResponse(
            #         bucket_id=bucket_id,
            #         key=key,
            #         left_replicas=0,
            #         combined_key_str="{}@{}".format(bucket_id,key),
            #         replicas=local_current_replicated_peers_ids
            #     )

            get_size_result = peer.get_size(bucket_id=bucket_id, key=key, timeout=60)
            
            if get_size_result.is_ok:
                ball_size_response =get_size_result.unwrap()
                ball_size =ball_size_response.size
                
                # No replica in this peer
                if ball_size == 0:
                    res = await self.__replicate(bucket_id=bucket_id, key=key, peer= peer)
                    if res:
                        current_replicas_counter+=1
                        local_current_replicated_peers_ids.append(peer.peer_id)
                else:
                    self.__log.debug({
                        "event":"REPLICA.EXISTS",
                        "peer_id":peer.peer_id,
                        "bucket_id":bucket_id,
                        "key":key
                    })
                    # self.cre
                    local_current_replicated_peers_ids.append(peer.peer_id)
                    current_replicas_counter+=1
                self.rm.create_replicas(
                    bucket_id=bucket_id,
                    key=key,
                    peer_ids=local_current_replicated_peers_ids
                )
                
                        
            else:
                self.__log.error({
                    "event":"GET.SIZE.FAILED",
                    "peer_id":peer.peer_id,
                    "msg":str(get_size_result.unwrap_err())
                })
                continue
        # _____________________________________________
        left_replicas = rf-current_replicas_counter
        event = "REPLICATION.UNCOMPLETED" if left_replicas >0 else "REPLICATION.COMPLETED"
        self.__log.debug({
            "event":event,
            "current_rf":current_replicas_counter,
            "rf":rf,
            "left_replicas": left_replicas,
            "peers": len(self.ph.peers)
        })
        return ReplicationProcessResponse(
            bucket_id=bucket_id,
            key=key,
            left_replicas=left_replicas,
            combined_key_str="{}@{}".format(bucket_id,key),
            replicas=local_current_replicated_peers_ids
        )
    async def run(self,event:ReplicationEvent)->ReplicationProcessResponse:
        try:
            bucket_id            = event.bucket_id
            key                  = event.key
            # size                 = self.rm.ge
            combined_key         = event.get_combined_key_str()
            available_peers_ids  = self.rm.get_available_peers_ids(bucket_id=bucket_id,key=key)
            replicated_peers_ids = self.rm.get_replicated_peers(bucket_id=bucket_id,key=key)


            self.__log.debug({
                "event":"REPLICATOR.MANAGER",
                "combined_key":combined_key,
                "bucket_id":bucket_id,
                "key":key,
                "available_peers":available_peers_ids,
                "replicated_peers":replicated_peers_ids,
            })
            if combined_key in self.replicated_objects:
                self.__log.debug({
                    "event":"REPLICATION.PROCESS.SKIPPED",
                    "bucket_id":bucket_id,
                    "key":key
                })
                return ReplicationProcessResponse(bucket_id=event.bucket_id, key = event.key, combined_key_str=combined_key, left_replicas=0, replicas=[])
            
            result = await self.replication_process(
                bucket_id=bucket_id,
                key=key,
                rf= event.rf,
                from_peer_id = event.from_peer_id, 
                available_peers_ids = available_peers_ids,
                replicated_peers_ids= replicated_peers_ids
            )
          
            return result
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return False
   