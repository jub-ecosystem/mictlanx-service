

from mictlanx.logger.log import Log
from mictlanx.v4.interfaces.index import Peer,PeerStats
from queue import Queue
from threading import Thread
from typing import List,Dict,Literal
import requests as R
import time as T
import humanfriendly as HF
from option import NONE,Some,Option

class PeerHealer:
    def __init__(self,q:Queue,peers:List[Peer],name: str="mictlanx-peer-healer-0", show_logs:bool=True) -> None:
        self.is_running = True
        self.operations_counter = 0
        self.peers=peers
        self.unavailable_peers = []
        self.__peer_stats:Dict[str, PeerStats] = {}
        self.q= q
        self.completed_tasks:List[str] = []
        self.__log             = Log(
            name = "mictlanx-peer-healer-0",
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

    def get_tupled_peers(self):
        for peer in self.peers:
            yield (peer.peer_id,peer.port)

    def total_counter_by_key(self,key:str,operation:Literal["PUT","GET"]):
        x = self.counter_map.setdefault(key,{})
        total= 0
        for k,v in x.items():
            total+=v
        return total

    # def update_counter_map(self,peer_id:str,key:str,operation:Literal["PUT","GET"],extra_counter:int =1):
        # _key = "{}/{}@{}".format(operation,peer_id,key), 0 
        # current_value = self.counter_map.setdefault(_key)
        # self.counter_map[_key]+=  current_value+extra_counter

    def next_peer(self,key:str,operation:Literal["PUT","GET"])->Option[Peer]:
        try:
            n_peers = len(self.peers)
            if n_peers ==0:
                return NONE
            total_operations = self.total_counter.setdefault(operation,0)
            next_peer_index = total_operations % n_peers
            selected_peer = self.peers[next_peer_index]
            current_operations = self.counter_map.setdefault(key, {}).setdefault(selected_peer.peer_id, {}).setdefault(operation,0)
            self.counter_map[key][selected_peer.peer_id][operation] = current_operations+1
            self.total_counter.setdefault(operation,0)
            self.total_counter[operation]+=1
            return Some(selected_peer)
        except Exception as e:
            return NONE
        # self.total_counter[operation]+=1


    def ufs(self)->Dict[str,float]:
        return dict([ (key,stats.calculate_disk_uf()) for (key,stats) in self.__peer_stats.items() ])
    def get_peer(self,peer_id:str)->Option[Peer]:
        if not peer_id in self.unavailable_peers:
            maybe_peer = next( (  peer for peer in self.peers if peer.peer_id == peer_id ), None)
            if maybe_peer is None:
                return NONE
            else:
                return Some(maybe_peer)

    def add_peer(self,peer:Peer)->int:
        peers_ids = list(map(lambda x: x.peer_id, self.peers))
        if not peer.peer_id in peers_ids:
            self.peers.append(peer)
            return 0
        return -1

    def remove_peer(self,peer_id:str)->bool:
        n = len(self.peers)
        self.peers  = list(filter(lambda peer: peer.peer_id != peer_id, self.peers))
        return n > len(self.peers)

   

    def get_stats(self):
        return self.__peer_stats
    # def get(task_id:str)->Result[]
    def peers_ids(self):
        return list(set(list(map(lambda x:x.peer_id, self.peers))))
    def run(self) -> None:
        # while True:
        peers= self.peers
        counter = 0
        # unavailable_peers =[]
        self.unavailable_peers = []
        for peer in peers:
            try:
                peer:Peer = peer

                get_ufs_response = peer.get_ufs()
                # print(peer.peer_id  ,get_ufs_response,peer.base_url())
                if get_ufs_response.is_ok:
                    get_uf_result = get_ufs_response.unwrap()
                    peer_stats = self.__peer_stats.get(peer.peer_id,PeerStats(peer_id=peer.peer_id))

                    peer_stats.total_disk = get_uf_result.total_disk
                    peer_stats.used_disk  = get_uf_result.used_disk
                    self.__peer_stats.setdefault(peer.peer_id,peer_stats)
                    counter +=1
                    self.__log.debug("Peer {} is  available".format(peer.peer_id))
                else:
                    self.peers = list(filter(lambda p: not p.peer_id == peer.peer_id ,self.peers))
                    self.__peer_stats.pop(peer.peer_id)
                    # self.peers
                    self.unavailable_peers.append(peer.peer_id)
                    self.__log.error("Peer {} is not available.".format(peer.peer_id))
                    
                        
                percentage_available_peers =  (counter / len(peers))*100 
                if percentage_available_peers == 0:
                    self.__log.error("No available peers. Please contact me on jesus.castillo.b@cinvestav.mx")
                    # for peer_id in self.unavailable_peers:
                        # self.q.put(UnavilablePeer(peer_id=peer_id))
                    raise Exception("No available peers. Please contact me on jesus.castillo.b@cinvestav.mx")
                # elif percentage_available_peers < 100:
                    # for peer_id in self.unavailable_peers:
                        # self.q.put(UnavilablePeer(peer_id=peer_id))
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
            # continue
            # finally:
            #     T.sleep(self.heartbeat)
                # print(e)
