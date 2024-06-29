

from mictlanx.logger.log import Log
from mictlanx.v4.interfaces.index import Peer,PeerStats
from mictlanx.v4.interfaces.responses import PeerStatsResponse
import humanfriendly as HF
from queue import Queue
from threading import Thread
from typing import List,Dict,Literal,Tuple
from option import Result,Ok,Err
import requests as R
import time as T
from option import NONE,Some,Option
import aiorwlock
# import a

class StoragePeerManager:
    def __init__(self,q:Queue,peers:List[Peer],name: str="mictlanx-peer-manager-0", show_logs:bool=True,max_tries:int =3 ,max_timeout_to_recover:str="30s") -> None:
        self.is_running:bool         = True
        self.operations_counter:int = 0
        self.lock               = aiorwlock.RWLock(fast=True)
        self.peers              = []
        self.available_peers  :List[Peer]           = peers
        # self.available_peers
        self.unavailable_peers:List[Peer]           = []
        self.__peer_ufs       :Dict[str, PeerStats] = {}
        self.global_stats     :PeerStatsResponse    = PeerStatsResponse.empty()
        self.__peers_stats_responses:Dict[str, PeerStatsResponse] = {}
        self.max_timeout_recover = HF.parse_timespan(max_timeout_to_recover)
        self.last_recover_tick = T.time()
        self.q= q
        self.completed_tasks:List[str] = []
        self.max_tries = max_tries
        self.__log             = Log(
            name = name,
            console_handler_filter=lambda x: show_logs,
            interval=24,
            when="h"
        )
        self.avg_rt_map:Dict[str, float] = {}
        self.counter_map:Dict[str, Dict[str, Dict[str, int] ]] = {}
        self.total_counter:Dict[str,int] = {
            "GET":0,
            "PUT":0
        }
        # self.total_gets_counter = 0
        # self.total_puts_counter = 0

    async def get_available_peers(self):
        async with self.lock.reader_lock:
            return self.available_peers
    async def get_unavailable_peers(self):
        async with self.lock.reader_lock:
            return self.unavailable_peers
        

    async def get_tupled_peers(self):
        for peer in await self.get_available_peers():
            yield (peer.peer_id,peer.port)

    def total_counter_by_key(self,key:str,operation:Literal["PUT","GET"]):
        x = self.counter_map.setdefault(key,{})
        total= 0
        for k,v in x.items():
            total+=v
        return total



    async def next_peer(self,key:str,operation:Literal["PUT","GET"])->Option[Peer]:
        try:
            peers = await self.get_available_peers()
            n_peers = len(peers)
            if n_peers ==0:
                return NONE
            total_operations = self.total_counter.setdefault(operation,0)
            next_peer_index = total_operations % n_peers
            selected_peer = peers[next_peer_index]
            current_operations = self.counter_map.setdefault(key, {}).setdefault(selected_peer.peer_id, {}).setdefault(operation,0)
            self.counter_map[key][selected_peer.peer_id][operation] = current_operations+1
            self.total_counter.setdefault(operation,0)
            self.total_counter[operation]+=1
            return Some(selected_peer)
        except Exception as e:
            return NONE
        # self.total_counter[operation]+=1


    def ufs(self)->Dict[str,float]:
        return dict([ (key,stats.calculate_disk_uf()) for (key,stats) in self.__peer_ufs.items() ])
    async def get_peer(self,peer_id:str)->Option[Peer]:
        unavailable_peers = await self.get_unavailable_peers_ids()
        available_peers = await self.get_available_peers()
        if not peer_id in unavailable_peers:
            maybe_peer = next( (  peer for peer in available_peers if peer.peer_id == peer_id ), None)
            if maybe_peer is None:
                return NONE
            else:
                return Some(maybe_peer)

    async def add_peer(self,peer:Peer)->int:
        # available_peers = await self.get_available_peers()
        all_peer_ids = (await self.get_available_peers_ids() ) + (await self.get_unavailable_peers_ids())
        # list(map(lambda x: x.peer_id, self.available_peers))
        async with self.lock.writer_lock:
            if not peer.peer_id in all_peer_ids:
                self.peers.append(peer)
                return 0
            return -1

    async def remove_peer(self,peer_id:str)->bool:
        
        async with self.lock.writer_lock:
            n = len(self.available_peers + self.unavailable_peers)
            self.available_peers  = list(filter(lambda peer: peer.peer_id != peer_id, self.available_peers))
            self.unavailable_peers  = list(filter(lambda peer: peer.peer_id != peer_id, self.unavailable_peers))
            return n > len(self.peers)

   

    def get_stats(self):
        return self.__peer_ufs
    # def get(task_id:str)->Result[]
    async def get_available_peers_ids(self):
        async with self.lock.reader_lock:
            return list(set(list(map(lambda x:x.peer_id, self.available_peers))))
    async def get_unavailable_peers_ids(self):
        async with self.lock.reader_lock:
            return list(set(list(map(lambda x:x.peer_id, self.unavailable_peers))))
    
    async def stats(self)->List[PeerStatsResponse]:
        """Traverse the peers and get the stats /api/v4/peers/stats"""
        available_peers = await self.get_available_peers()
        xs=  [p.get_stats().unwrap_or(PeerStatsResponse.empty()) for p in available_peers ]
        xs_dict = dict([(p.peer_id, p) for p in xs])
        async with self.lock.writer_lock:
            self.__peers_stats_responses = xs_dict
        return xs
    async def recover(self):
        current_peers = set(await self.get_available_peers_ids())
        unavailable_peers = set(await self.get_unavailable_peers_ids())
        to_recover_peers = unavailable_peers.difference(current_peers)

        async with self.lock.writer_lock:
            for unavailable_peer in self.unavailable_peers:
                if unavailable_peer.peer_id in to_recover_peers:
                    self.__log.debug({
                        "event":"PEER.RECOVERING",
                        "peer_id":unavailable_peer.peer_id,
                    })
                    self.peers.append(unavailable_peer)
    async def run(self):
        try:
            # print("PM","RUNNNN")
            current_time = T.time()
            (available_peers, unavailable_peers)= await self.check_peers_availability()
            current_stats = await self.stats()
            self.global_stats = sum(current_stats, PeerStatsResponse.empty())
            elapsed_time = current_time - self.last_recover_tick
            self.__log.debug({
                "event":"PEER.MANAGER.TICK",
                "elapsed_time":HF.format_timespan(elapsed_time),
                "available_peers":len(available_peers),
                "unavailable_peers":len(unavailable_peers),
            })
            if elapsed_time >= self.max_timeout_recover:
                await self.recover()
                self.last_recover_tick = T.time()
            return Ok(0)
        except Exception as e:
            return Err(e)
        # finally:
            # self.last_tick = T.time()
    
    def make_available(self,peer_id:str)->bool:
        unavailable_peer       = next(filter(lambda up: up.peer_id==peer_id ,self.unavailable_peers), -1)
        not_in_unavailable     = unavailable_peer == -1
        self.unavailable_peers = list(filter(lambda up: not up.peer_id==peer_id ,self.unavailable_peers))
        already_available      = len(list(filter(lambda ap: ap.peer_id == peer_id, self.available_peers))) == 1

        if not already_available and not not_in_unavailable:
            self.__log.info({
                "event":"AVAILABLE",
                "peer_id":peer_id
            })
            self.available_peers.append(unavailable_peer)
            return True
        return False
    def make_unavailable(self,peer_id:str)->bool:
        available_peer         = next(filter(lambda up: up.peer_id==peer_id ,self.available_peers), None)
        if not available_peer:
            return False
        # print("AVAILABLE_PEER", available_peer)
        self.available_peers = list(filter(lambda up: not up.peer_id==peer_id ,self.available_peers))
        already_unavailable      = len(list(filter(lambda ap: ap.peer_id == peer_id, self.unavailable_peers)))==1
        if not already_unavailable:
            self.unavailable_peers.append(available_peer)
            self.__log.info({
                "event":"UNAVAILABLE",
                "peer_id":peer_id
            })
            return True
        return False
        



    async def check_peers_availability(self) -> Tuple[List[str], List[str]]:
        """Returns a tuple of the list of the peers ids -> (available, unavailable) """
        # async with self.lock.reader_lock:
        peers = (await self.get_available_peers()) + self.peers
            # f len(self.peers) == 0 else self.peers

        counter                = 0
        # self.unavailable_peers = []
        unavailable            = []
        available              = []
        # print("PEERS",peers)
        for peer in peers:
            try:
                get_ufs_response  = peer.get_ufs_with_retry(tries=self.max_tries,timeout=15)
                if get_ufs_response.is_ok:
                    get_uf_result              = get_ufs_response.unwrap()
                    peer_stats            = self.__peer_ufs.get(peer.peer_id,PeerStats(peer_id=peer.peer_id))
                    async with self.lock.writer_lock:
                        peer_stats.total_disk = get_uf_result.total_disk
                        peer_stats.used_disk  = get_uf_result.used_disk
                        x                     = self.make_available(peer_id=peer.peer_id)
                        counter                +=1
                        available.append(peer.peer_id)
                    self.__log.debug("Peer {} is  available".format(peer.peer_id))
                else:
                    async with self.lock.writer_lock:
                        x = self.make_unavailable(peer_id=peer.peer_id)
                    self.__log.error("Peer {} is not available.".format(peer.peer_id))
                percentage_available_peers =  (counter / (len(await self.get_available_peers()) + len(await self.get_unavailable_peers())) )*100 
                if percentage_available_peers == 0:
                    self.__log.error("No available peers. Please contact me on jesus.castillo.b@cinvestav.mx")
                    raise Exception("No available peers. Please contact me on jesus.castillo.b@cinvestav.mx")
                
                self.__log.debug("{}% of the peers are available".format(percentage_available_peers ))
        
            except R.exceptions.ConnectTimeout as e:
                self.__log.error({
                    "msg":"ConnectionTimeout",
                    "peer_id":peer.peer_id
                })

            except R.exceptions.ConnectionError as e:
                self.__log.error({
                    "msg":"ConnectionError",
                    "peer_id":peer.peer_id
                })
            except R.exceptions.HTTPError as e:
                self.__log.error({
                    "msg":str(e.response.content.decode("utf8") ),
                    "peer_id":peer.peer_id
                })
                # continue
            except Exception as e:
                self.__log.error({
                    "msg":str(e),
                    "peer_id":peer.peer_id
                })
        # Clean peers
        async with self.lock.writer_lock:
            self.peers = []
        return (await self.get_available_peers_ids(),await self.get_unavailable_peers_ids())
            # continue
            # finally:
            #     T.sleep(self.heartbeat)
                # print(e)
