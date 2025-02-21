
import asyncio
from mictlanxrouter.replication_manager import ReplicaManager
from mictlanxrouter.peer_manager import StoragePeerManager
from mictlanx.logger import Log
import humanfriendly as HF
from typing import Dict,Set,List
class GarbageCollectorX:

    def __init__(self, rm : ReplicaManager,storage_peer_manager:StoragePeerManager,max_tries:int = 5,show_logs:bool= True,timeout:str = "10sec"):
        self.rm = rm
        self.__log = Log(
            name = "mictlanx-gc-0",
            console_handler_filter=lambda x: show_logs,
            interval=24,
            when="h"
        )
        self.spm = storage_peer_manager
        self.timeout = HF.parse_timespan(timeout)
        self.garbage_map:Dict[str, int] = {}
        self.zero_size_peer_replicas:Dict[str, Set[str]]={}
        self.max_tries = max_tries
    async def run(self):

        while True:
            ball_replicas = list(self.rm.replica_map.keys())
            self.__log.debug({
                "event":"CHECK.REPLICAS",
                "n":len(ball_replicas)
            })
            for combined_key in ball_replicas:
                current_tries = self.garbage_map.setdefault(combined_key,0)
                self.zero_size_peer_replicas.setdefault(combined_key,set([]))
                if current_tries >= self.max_tries:
                    del self.garbage_map[combined_key]
                    replica_ids      = list(self.zero_size_peer_replicas[combined_key])
                    deleted_replicas = await self.rm.remove_replicas_by_ids(bucket_id=bucket_id,key=key,replica_ids=replica_ids)
                    self.__log.info({
                        "event":"GARBAGE.COLLECTED",
                        "bucket_id":bucket_id,
                        "key":key,
                        "deleted_replicas":deleted_replicas
                    })
                    continue
                bucket_id, key = combined_key.split("@")
                replica_peers = self.rm.replica_map[combined_key]
                async_peers = await self.spm.from_peer_ids_to_peers(peer_ids=replica_peers)
                for peer in async_peers:
                    result = await peer.get_size(bucket_id=bucket_id,key=key)
                    if result.is_ok:
                        res = result.unwrap()
                        if res.size ==0:
                            self.zero_size_peer_replicas[combined_key].add(peer.peer_id)
                            self.garbage_map[combined_key]+=1
                        self.__log.debug({
                            "event":"CHECK.BALL.PEER.SIZE",
                            "size":res.size,
                            "peer_id":res.peer_id,
                            "bucket_id":bucket_id,
                            "key":key,
                            "counter":self.garbage_map[combined_key],
                            "to_delete_replicas":list(self.zero_size_peer_replicas[combined_key])
                        })
   
            await asyncio.sleep(self.timeout)

