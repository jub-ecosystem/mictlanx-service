import os
import asyncio
from mictlanx.logger.log import Log
import time as T
import humanfriendly as HF
from mictlanx.v4.interfaces.index import Peer,PeerStats
from mictlanxrouter.interfaces.healer import PeerHealer
from concurrent.futures import ThreadPoolExecutor
from typing import List,Dict,Any,Tuple,Optional
import httpx as R
from asyncio import Queue
# from dataclasses import dataclass
from pydantic import BaseModel,Field
from nanoid import generate as nanoid
import string
from dataclasses import dataclass

ALPHABET = string.digits + string.ascii_lowercase

class ReplicationProcessResult(object):
    def __init__(self, bucket_id:str,key:str, success_replicas:List[str]=[], failed_replicas:List[str]=[],available_replicas:List[str]=[]):
        self.bucket_id        = bucket_id
        self.key              = key
        self.success_replicas = success_replicas 
        self.failed_replicas  = failed_replicas
        self.available_replicas = available_replicas
    def __str__(self):
        return "ReplicationProcessResult(bucket_id = {}, key = {}, success_replicas= {}, failed_replicas= {}, available_replicas={})".format(
            self.bucket_id,
            self.key,
            self.success_replicas,
            self.failed_replicas,
            self.available_replicas
        )
class ReplicaManager(object):
    def __init__(self,ph:PeerHealer):
        self.key_replicas:Dict[str, List[str]] = {}
        self.ph = ph
    def get_available_peers(self,bucket_id:str,key:str):
        _all_peers     = self.ph.peers
        _all_peers_ids = set(list(map(lambda p: p.peer_id, _all_peers)))
        combined_key         = "{}@{}".format(bucket_id,key)
        current_replicas = set(self.key_replicas.setdefault(combined_key,[]))
        return list(_all_peers_ids.difference(current_replicas))
    # 
    def get_replicated_peers(self,bucket_id:str,key:str):
        combined_key     = "{}@{}".format(bucket_id,key)
        current_replicas = self.key_replicas.setdefault(combined_key,[])
        return current_replicas

    def remove_replicas(self,bucket_id:str,key:str, to_remove_replicas:List[str]=[])->List[str]:
        combined_key = "{}@{}".format(bucket_id,key)
        current = set(self.key_replicas.setdefault(combined_key, []))
        trr = set(to_remove_replicas)
        x = current.difference(trr)
        self.key_replicas[combined_key] = x
        return x


        
    def create_replicas(self, bucket_id:str,key:str, selected_replicas:List[str]=[])->str :
        _all_peers     = self.ph.peers
        _all_peers_ids = set(list(map(lambda p: p.peer_id, _all_peers)))
        combined_key = "{}@{}".format(bucket_id,key)
        
        current_replicas = set(self.key_replicas.setdefault(combined_key,[]))
        _selected_replicas_peers  = set(selected_replicas)
        diff = current_replicas.difference(_selected_replicas_peers)
        replica_set_diff = current_replicas - (current_replicas - _selected_replicas_peers)
        # print("RSD",replica_set_diff)
        if len(replica_set_diff ) == 0:
            latest_current_replicas         = current_replicas.union(_selected_replicas_peers)
            self.key_replicas[combined_key] =  latest_current_replicas
            print("FIRST")
            return ReplicationProcessResult(
                bucket_id=bucket_id,
                key= key,
                success_replicas= selected_replicas,
                failed_replicas = [],
                available_replicas= _all_peers_ids.difference(latest_current_replicas)
            )
        else:
            print("SECOND",diff)
            return ReplicationProcessResult(
                bucket_id=bucket_id,
                key= key,
                success_replicas= list(_selected_replicas_peers.difference(diff)),
                failed_replicas = list(replica_set_diff),
                available_replicas= _all_peers_ids.difference(_selected_replicas_peers)
            )




class ReplicationEvent(BaseModel):
    id:str = Field(default_factory=lambda: str(nanoid(alphabet= ALPHABET )))
    rtype:Optional[str] = "DATA" 
    rf:int 
    # strategy:Optional[str]=""
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


class Replicator:
    def __init__(self,
                 queue:Queue,
                 ph:PeerHealer,strategy:str = "FLOOD",strategy_mode:str="STATIC",show_logs:bool=True,chunk_size:int = 100) -> None:
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
        self.rm = ReplicaManager(ph=self.ph)
    
    async def __replication_process(self,bucket_id:str,key:str, peer:Peer):
        start_time = T.time()
        x = peer.replicate(bucket_id=bucket_id,key=key,timeout=60,headers={})
        self.__log.info({
            "event":"REPLICATION.PROCESS",
            "bucket_id":bucket_id,
            "key":key,
            "peer_id":peer.peer_id,
            "strategy":self.strategy,
            "mode":self.strategy_mode,
            "ok":x.is_ok,
            "response_time": T.time() - start_time
        })
        if x.is_ok:
            self.replicated_objects.setdefault("{}@{}".format(bucket_id,key) ,{
                "bucket_id":bucket_id,
                "key":key,
                "replicated_at":T.time()
            })
            return True
        else:
             return False
    

    async def replication_process(self,bucket_id:str,key:str,rf:int=1,from_peer_id:str="")->ReplicationProcessResponse:
        current_replicas   = 0
        ball_size          = 0
        replicas:List[str] = []
        available_peers_ids = self.rm.get_available_peers(bucket_id=bucket_id,key=key)
        available_peers = list(map(lambda p:p.unwrap(),filter(lambda pop: pop.is_some,map(lambda pid: self.ph.get_peer(peer_id=pid), available_peers_ids))))
        

        for peer in available_peers:
            if current_replicas == rf:
                self.rm.create_replicas(
                    bucket_id=bucket_id,
                    key=key,
                    selected_replicas=replicas
                )
                return ReplicationProcessResponse(
                    bucket_id=bucket_id,
                    key=key,
                    left_replicas=0,
                    combined_key_str="{}@{}".format(bucket_id,key),
                    replicas=replicas
                )
            get_size_result = peer.get_size(bucket_id=bucket_id, key=key, timeout=60)
            if get_size_result.is_ok:
                ball_size_response =get_size_result.unwrap()
                ball_size =ball_size_response.size
                # No replica in this peer
                if ball_size == 0:
                    res = await self.__replication_process(bucket_id=bucket_id, key=key, peer= peer)
                    if res:
                        current_replicas+=1
                        replicas.append(peer.peer_id)
                else:
                    self.__log.debug({
                        "event":"REPLICA.EXISTS",
                        "peer_id":peer.peer_id,
                        "bucket_id":bucket_id,
                        "key":key
                    })
                    replicas.append(peer.peer_id)
                    current_replicas+=1
                        
            else:
                continue
        # _____________________________________________
        left_replicas = rf-current_replicas
        self.__log.debug({
            "event":"REPLICATION.UNCOMPLETED",
            "current_rf":current_replicas,
            "rf":rf,
            "left_replicas": left_replicas,
            "peers": len(self.ph.peers)
        })
        return ReplicationProcessResponse(
            bucket_id=bucket_id,
            key=key,
            left_replicas=left_replicas,
            combined_key_str="{}@{}".format(bucket_id,key),
            replicas=replicas
        )
    async def run(self,event:ReplicationEvent)->ReplicationProcessResponse:
        try:
            bucket_id = event.bucket_id
            key       =  event.key
            combined_key = event.get_combined_key_str()
            # "{}@{}".format(bucket_id,key)
            self.__log.debug({
                "event":"REPLICATOR.MANAGER",
                "bucket_id":bucket_id,
                "key":key,
                "available_peers":self.rm.get_available_peers(bucket_id=bucket_id,key=key),
                "replicated_peers":self.rm.get_replicated_peers(bucket_id=bucket_id,key=key),
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
                from_peer_id = event.from_peer_id
            )
          
            return result
        except Exception as e:
            self.__log.error({
                "msg":str(e)
            })
            return False
   