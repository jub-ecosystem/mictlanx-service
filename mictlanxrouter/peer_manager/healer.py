

import os
import asyncio
from mictlanx.logger.log import Log

from mictlanxrouter.helpers.utils import Utils as RouterUtils
from mictlanx.v4.interfaces.index import Peer,PeerStats
from mictlanx.v4.interfaces.responses import PeerStatsResponse,GetUFSResponse
import humanfriendly as HF
from queue import Queue
# from threading import Thread
from typing import List,Dict,Literal,Tuple,Any
from option import Result,Ok,Err
import requests as R
import time as T
from option import NONE,Some,Option
import aiorwlock
from mictlanx.v4.summoner.summoner import Summoner
from mictlanx.interfaces.payloads import SummonContainerPayload,ExposedPort,MountX
from mictlanx.interfaces.responses import SummonContainerResponse
from dataclasses import dataclass
# import a
@dataclass
class DeployPeersResult:
    success_peers:List[str]
    failed_peers:List[str]
    service_time:float 
    technique:str = "ACTIVE"


class StoragePeerManagerParams(object):
    def __init__(self,
        max_retries:int = 2,
        max_idle_time:str ="10s",
        queue_tick_timeout:str="5s",
        max_timeout_to_recover:str="30s",
        base_port:int = 25000,
        base_protocol:str = "http",
        physical_nodes_indexes:List[int] = [0,2,3,4,5,6,7,8,9],
        debug:bool = True,
        summoner_mode:str ="docker",
        max_recover_time_until_restart:str= "1m",
        peers_config_path:str = "", 
        max_timeout_to_save_peer_config:str = "30min",
        peer_elastic:bool = True,
    ):
        self.peer_elastic = peer_elastic
        self.max_retries = max_retries
        self.max_idle_time = max_idle_time
        self.queue_tick_timeout = queue_tick_timeout
        # self.max_idle_time = max_idle_time
        self.queue_tick_timeout = queue_tick_timeout
        self.max_timeout_to_recover = max_timeout_to_recover
        self.base_port = base_port
        self.base_protocol = base_protocol
        self.physical_nodes_indexes = physical_nodes_indexes
        self.debug = debug
        self.summoner_mode = summoner_mode
        self.max_recover_time_until_restart = max_recover_time_until_restart
        self.peers_config_path = peers_config_path
        self.max_timeout_to_save_peer_config = max_timeout_to_save_peer_config
        self.__last_update_at       = T.time()

    def check(self, spmp: 'StoragePeerManagerParams') -> Option['StoragePeerManagerParams']:
        if (self.max_idle_time != spmp.max_idle_time or
            self.max_retries != spmp.max_retries or 
            self.queue_tick_timeout != spmp.queue_tick_timeout or
            self.max_timeout_to_recover != spmp.max_timeout_to_recover or
            self.base_port != spmp.base_port or
            self.base_protocol != spmp.base_protocol or
            self.physical_nodes_indexes != spmp.physical_nodes_indexes or
            self.debug != spmp.debug or
            self.summoner_mode != spmp.summoner_mode or
            self.max_recover_time_until_restart != spmp.max_recover_time_until_restart or
            self.peers_config_path != spmp.peers_config_path or
            self.max_timeout_to_save_peer_config != spmp.max_timeout_to_save_peer_config):
            return Some(spmp)
        return NONE 

    def elapsed_time_last_update(self):
        return HF.format_timespan(T.time() - self.__last_update_at)
    def update(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
class StoragePeerManager:
    def __init__(self,
        q:Queue,
        peers:List[Peer],
        summoner:Summoner,
        name: str="mictlanx-peer-manager-0",
        show_logs:bool=True,
        params:Option[StoragePeerManagerParams] = NONE
    ):
        """
        [args]
        q: Asyncio queue
        peers: The list of init storage peers
        summoner: Instance of xolo summoner
        name: logger name 
        params:StoragePeersManagerParams
        show_logs: Enable if true the logs otherwise it disable it
        max_tries: Number of max retries begore fail
        max_timeout_to_recover: the number of seconds before try to recover the unavailable peers
        base_port: Base port of the summoner (must be defined in summoner)
        physical_nodes_indeces
        debug: If true the ip_addtr turns into localhost addresses
        summoner_mode: Mode of summoner (must be in the summoner object)
        max_recover_time_until_restart: default 5min if the peers not respond after 5mon the summoner restart the peers
        """
        self.params = params.unwrap() if params.is_some else StoragePeerManagerParams()
        self.max_timeout_to_save_peer_config = HF.parse_timespan(self.params.max_timeout_to_save_peer_config)
        self.debug        = self.params.debug
        self.peer_config_path =self.params.peers_config_path 
        self.peers_config = RouterUtils.read_peers(path=self.peer_config_path).unwrap_or({})
        self.is_running        :bool        = True
        self.operations_counter:int         = 0
        self.lock                           = aiorwlock.RWLock(fast=True)
        self.peers             :List[Peer]  = []
        self.available_peers    :List[Peer] = peers
        self.summoner          :Summoner    = summoner
        self.local_ip_addr                  = "localhost"
        self.params_lock = aiorwlock.RWLock(fast=True)
        self.time_last_recover_by_peer:Dict[str,float]  = {}
        self.max_recover_time_until_restart =  HF.parse_timespan(self.params.max_recover_time_until_restart)
        self.unavailable_peers:List[Peer]           = []
        self.__peer_ufs       :Dict[str, PeerStats] = {}
        self.global_stats     :PeerStatsResponse    = PeerStatsResponse.empty()
        self.__peers_stats_responses:Dict[str, PeerStatsResponse] = {}
        self.max_timeout_to_recover = HF.parse_timespan(self.params.max_timeout_to_recover)
        self.last_recover_tick = T.time()
        self.last_to_save_peer_config = T.time()
        self.q= q
        self.completed_tasks:List[str] = []
        # self.max_tries = self.params.max_tries
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
    

    async def get_params(self,)->StoragePeerManagerParams:
        async with self.params_lock.reader_lock:
            return self.params
    async def update_params(self,**kwargs):
        async with self.params_lock.writer_lock:
            self.max_timeout_to_recover         = HF.parse_timespan(kwargs.get("max_timeout_to_recover",self.params.max_timeout_to_recover))
            self.max_recover_time_until_restart = HF.parse_timespan(kwargs.get("max_recover_time_until_restart",self.params.max_recover_time_until_restart))
            self.max_timeout_to_save_peer_config = HF.parse_timespan(kwargs.get("max_timeout_to_save_peer_config", self.params.max_timeout_to_save_peer_config))

            self.params.update(**kwargs)

    async def active_deploy_peers(
        self,
        disk:int= HF.parse_size("10GB"),
        memory:int = HF.parse_size("2GB"),
        workers:int = 2,
        rf:int = 1,
        cpu:int = 1,
        elastic:str="false",
        base_path:str= "/app/mictlanx",
        network_id:str = "mictlanx"
    )->DeployPeersResult:
        start_time = T.time()
        success    = []
        failed     = []
        # n = self.get_available_peers
        params = await self.get_params()
        n = await self.get_n_all_peers()
        deployed_peers:List[Peer] = []
        for i in range(rf):
            peer_index =  i + n
            peer_id = "mictlanx-peer-{}".format(peer_index)
            port = params.base_port + peer_index
            selected_node = (n+i)%len(self.physical_nodes_indexes)
            (container_id, port, result) = await self.deploy_peer(
                container_id=peer_id,
                port=port,
                selected_node=selected_node,
                base_path=base_path,
                cpu=cpu,
                disk=disk,
                elastic=elastic,
                memory=memory,
                network_id=network_id,
                workers=workers,

            )
            if result.is_err:
                self.__log.error({
                    "event":"DEPLOY.FAILED",
                    "peer_id":peer_id,
                    "port":port,
                    "error":str(result.unwrap_err())
                })
                continue
            self.__log.info({
                "event":"DEPLOY.PEER",
                "container_id":container_id,
                "peer_id":peer_id,
                "physical_node":selected_node,
                "port":port,
                "cpu":cpu,
                "memory":HF.format_size(memory),
                "disk":HF.format_size(disk),
                "elastic":elastic,
                "network_id":network_id,
                "workers":workers,
                "ok":int(result.is_ok)
            })
            if result.is_ok:
                success.append(peer_id)
                peer = Peer(
                        peer_id  = peer_id,
                        ip_addr  = peer_id if not params.debug else self.local_ip_addr,
                        port     = port,
                        protocol = params.base_protocol
                )
                deployed_peers.append(peer)
            else:
                failed.append(peer_id)
        

        # self.add_peer()
        available_peers = await self.get_available_peers()
        all_peers = available_peers + deployed_peers
        # print("AVAILABLE_PEERs",all_peers)
        for p1 in all_peers:
            if not p1 in available_peers:
                await self.add_peer(peer=p1)

            for p2 in all_peers:
                if p1.peer_id == p2.peer_id:
                    continue
                
                p1.add_peer_with_retry(
                    id=p2.peer_id,
                    disk=disk,
                    ip_addr= p2.ip_addr,
                    memory=memory,
                    port=p2.port,
                    used_disk=0,
                    used_memory=0,
                    weight=1,
                    logger=self.__log

                )

        service_time = T.time() - start_time
        return DeployPeersResult(
            success_peers= success,
            failed_peers= failed,
            service_time=service_time,
            technique= "ACTIVE"
        )
    async def deploy_peer(self,
                          container_id:str,
                          port:int,
                          workers:int = 2,
                          disk:int = HF.parse_size("20GB"),
                          memory:int = HF.parse_size("2GB"),
                          cpu:int = 1,
                          selected_node:int=0,
                          elastic:str="false",
                          base_path:str= "/app/mictlanx",
                          network_id:str = "mictlanx"
    )->Tuple[str, int,Result[SummonContainerResponse,Exception]]:
        """return the <container_id> <port> <result>"""
        _start_time     = T.time()
        params      = await self.get_params()
        local_path = "{}/local".format(base_path)
        log_path = "{}/log".format(base_path)
        data_path = "{}/data".format(base_path)
        payload         =self.peers_config.get(container_id,SummonContainerPayload(
            container_id=container_id,
            image=os.environ.get("MICTLANX_PEER_IMAGE","nachocode/mictlanx:peer"),
            hostname    = container_id,
            exposed_ports=[ExposedPort(NONE,port,port,NONE)],
            envs= {
                "USER_ID":"6666",
                "GROUP_ID":"6666",
                "BIN_NAME":"peer",
                "NODE_ID":container_id,
                "NODE_PORT":str(port),
                "IP_ADDRESS":container_id,
                "SERVER_IP_ADDR":"0.0.0.0",
                "NODE_DISK_CAPACITY":str(disk),
                "NODE_MEMORY_CAPACITY":str(memory),
                "BASE_PATH":base_path,
                "LOCAL_PATH":local_path,
                "DATA_PATH":data_path,
                "LOG_PATH":log_path,
                "MIN_INTERVAL_TIME":"5",
                "MAX_INTERVAL_TIME":"20",
                "WORKERS":str(workers),
                "ELASTIC":elastic
            },
            memory=memory,
            cpu_count=cpu,
            mounts=[
                MountX(source="{}-data".format(container_id), target=data_path, mount_type=1),
                MountX(source="{}-log".format(container_id), target=log_path, mount_type=1),
                MountX(source="{}-local".format(container_id), target=local_path, mount_type=1),
            ],
            network_id=network_id,
            selected_node=Some(str(selected_node)),
            force=Some(True)
        ))
        # print("PAYLOAD", payload)
        response        = self.summoner.summon(
            mode= params.summoner_mode,
            payload=payload, 
        )
 
        return (container_id,port ,response)

    async def __find_peer(self, peer_id:str, peers:List[Peer])->Option[Peer]:
        async with self.lock.reader_lock:
            x = next(filter(lambda p: p.peer_id == peer_id, peers), -1)
            return  NONE if x == -1 else Some(x)
    

    async def sorted_by_uf(self,size:int=0)->List[str]:
        async with self.lock.reader_lock:
            size_filtered = filter(lambda p: p.available_disk >= size,sorted(self.__peers_stats_responses.values(),key=lambda x: x.disk_uf))
            return list(map(lambda x: x.peer_id,   size_filtered ) )
    # async def sorted_by_uf_ids(self,size:int=0)->List[Peer]:
    #     async with self.lock.reader_lock:
    #         size_filtered = filter(lambda p: p.available_disk >= size,sorted(self.__peers_stats_responses.values(),key=lambda x: x.disk_uf))
    #         return list(map(lambda x: x.peer_id,   size_filtered ) )

    async def get_available_peers(self):
        async with self.lock.reader_lock:
            return self.available_peers
    async def get_unavailable_peers(self):
        async with self.lock.reader_lock:
            return self.unavailable_peers
    async def get_peers(self):
        async with self.lock.reader_lock:
            return self.peers
    async def get_peers_ids(self):
        async with self.lock.reader_lock:
            return list(set(list(map(lambda x:x.peer_id, self.peers))))

    async def get_n_all_peers(self):
        async with self.lock.reader_lock:
            return len(self.available_peers) + len(self.unavailable_peers)
    async def get_n_available_peers(self):
        async with self.lock.reader_lock:
            return len(self.available_peers) 
    async def get_n_unavailable_peers(self):
        async with self.lock.reader_lock:
            return len(self.unavailable_peers) 


    async def get_available_peers_ids(self):
        async with self.lock.reader_lock:
            return list(set(list(map(lambda x:x.peer_id, self.available_peers))))
    
    async def get_len_available_peers(self):
        async with self.lock.reader_lock:
            return len(self.available_peers)

    async def get_len_unavailable_peers(self):
        async with self.lock.reader_lock:
            return len(self.unavailable_peers)

    async def get_unavailable_peers_ids(self):
        async with self.lock.reader_lock:
            return list(set(list(map(lambda x:x.peer_id, self.unavailable_peers))))
        

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
    async def get_peer_by_id(self,peer_id:str)->Option[Peer]:
        unavailable_peers = await self.get_unavailable_peers_ids()
        available_peers = await self.get_available_peers()
        if not peer_id in unavailable_peers:
            maybe_peer = next( (  peer for peer in available_peers if peer.peer_id == peer_id ), None)
            if maybe_peer is None:
                return NONE
            else:
                return Some(maybe_peer)
        else: 
            return NONE

    async def from_peer_ids_to_peers(self,peer_ids:List[str]=[])->List[Peer]:
        _selected_replicas_to_place = [ await self.get_peer_by_id(peer_id=i) for i in peer_ids]
        return list(map(lambda p: p.unwrap(),filter(lambda p: p.is_some, _selected_replicas_to_place)))
    async def add_peer(self,peer:Peer)->int:
        all_peer_ids = (await self.get_available_peers_ids() ) + (await self.get_unavailable_peers_ids())
        async with self.lock.writer_lock:
            if not peer.peer_id in all_peer_ids:
                self.peers.append(peer)
                return 0
            return -1


    async def remove_peer(self,peer_id:str)->bool:
        await self.leave_peer(peer_id=peer_id)
        res = self.summoner.delete_container(container_id=peer_id)
        return res.is_ok
        # async with self.lock.writer_lock:
        #     n = len(self.available_peers + self.unavailable_peers)
        #     self.available_peers  = list(filter(lambda peer: peer.peer_id != peer_id, self.available_peers))
        #     self.unavailable_peers  = list(filter(lambda peer: peer.peer_id != peer_id, self.unavailable_peers))
        #     return n > len(self.available_peers) + len(self.unavailable_peers)
    async def leave_peer(self,peer_id:str)->bool:
        async with self.lock.writer_lock:
            n = len(self.available_peers + self.unavailable_peers)
            self.available_peers  = list(filter(lambda peer: peer.peer_id != peer_id, self.available_peers))
            self.unavailable_peers  = list(filter(lambda peer: peer.peer_id != peer_id, self.unavailable_peers))
            return n > len(self.available_peers) + len(self.unavailable_peers)

   

    def get_stats(self):
        return self.__peer_ufs
    # def get(task_id:str)->Result[]
    
    async def stats(self)->List[PeerStatsResponse]:
        """Traverse the peers and get the stats /api/v4/peers/stats"""
        available_peers = await self.get_available_peers()
        xs=  [p.get_stats().unwrap_or(PeerStatsResponse.empty()) for p in available_peers ]
        xs_dict = dict([(p.peer_id, p) for p in xs])
        async with self.lock.writer_lock:
            self.__peers_stats_responses = xs_dict
        return xs
    async def recover(self,params:StoragePeerManagerParams):
        try:
            current_peers     = set(await self.get_available_peers_ids())
            unavailable_peers = set(await self.get_unavailable_peers_ids())
            to_recover_peers  = unavailable_peers.difference(current_peers)
            async with self.lock.writer_lock:
                for unavailable_peer in self.unavailable_peers:
                    if unavailable_peer.peer_id in to_recover_peers:
                        last_time = self.time_last_recover_by_peer.setdefault(unavailable_peer.peer_id,T.time())
                        elapsed   = T.time() - last_time
                        elapsed_str = HF.format_timespan(elapsed)

                        if elapsed >= self.max_recover_time_until_restart and params.peer_elastic:
                            (_,_,res) = await self.deploy_peer(container_id=unavailable_peer.peer_id,port=unavailable_peer.port)
                            self.time_last_recover_by_peer[unavailable_peer.peer_id] = T.time()
                            if res.is_err:
                                self.__log.error({
                                    "event":"DEPLOY.PEER.FAILED",
                                    "detail":str(res.unwrap_err())
                                })
                            else:
                                self.__log.info({
                                    "event":"PEER.RESTART",
                                    "elapsed":elapsed_str,
                                    "max_timeout":self.max_recover_time_until_restart,
                                    "ok":res.is_ok
                                })
                        elif elapsed >= self.max_recover_time_until_restart and not params.peer_elastic:
                            self.__log.warning({
                                "event":"PEER.ELASTICITY.DISABLED",
                                "elapsed":elapsed_str,
                                "peer_elastic":params.peer_elastic 
                            })
            
                        self.__log.debug({
                            "event":"PEER.RECOVERING",
                            "peer_id":unavailable_peer.peer_id,
                            "elapsed":elapsed_str,
                            "max_recover_time_until_restart":params.max_recover_time_until_restart
                        })
                        self.peers.append(unavailable_peer)
        except Exception as e:
            self.__log.error({
                "event":"RECOVER.EXCEPTION",
                "detail":str(e)
            })
    async def run(self):
        try:
            current_time                         = T.time()
            params                               = await self.get_params()
            (available_peers, unavailable_peers) = await self.check_peers_availability()
            current_stats                        = await self.stats()
            self.global_stats                    = sum(current_stats, PeerStatsResponse.empty())
            elapsed_time                         = current_time - self.last_recover_tick
            elapsed_time_to_save_peer_config     = current_time - self.last_to_save_peer_config
            # print("__"*50)
            self.__log.debug({
                "event":"PEER.MANAGER.TICK",
                "elapsed_time":HF.format_timespan(elapsed_time),
                "max_timeout_to_recover":params.max_timeout_to_recover,
                "available_peers":len(available_peers),
                "unavailable_peers":len(unavailable_peers),
            })
            
            if elapsed_time >= self.max_timeout_to_recover:
                await self.recover(
                    params=params
                )
                self.last_recover_tick = T.time()
                
            if elapsed_time_to_save_peer_config >= self.max_timeout_to_save_peer_config:
                res = RouterUtils.save_peers(path=params.peers_config_path,peers_config=self.peers_config)
                self.__log.debug({
                    "event":"SAVE.PEERS.CONFIG",
                    "ok":res.is_ok,
                    "path":params.peers_config_path
                })
                self.last_to_save_peer_config = T.time()
                
            # if elapsed_time >= self.max
            return Ok(0)
        except Exception as e:
            self.__log.error({
                "event":"SPM.RUN.EXCEPTION",
               "detail":str(e)
            })
            return Err(e)
        # finally:
            # self.last_tick = T.time()
    
    async def make_available(self,peer_id:str)->bool:
        unavailable_peer       = next(filter(lambda up: up.peer_id==peer_id ,self.unavailable_peers), -1)
        not_in_unavailable     = unavailable_peer == -1
        self.unavailable_peers = list(filter(lambda up: not up.peer_id==peer_id ,self.unavailable_peers))
        already_available      = len(list(filter(lambda ap: ap.peer_id == peer_id, self.available_peers))) == 1
        peers_ids = await self.get_peers_ids()
        new_peer = await self.__find_peer(peer_id=peer_id, peers= self.peers)

        if not already_available and not not_in_unavailable:
            self.__log.info({
                "event":"AVAILABLE",
                "new":False,
                "peer_id":peer_id
            })
            self.available_peers.append(unavailable_peer)
            return True

        elif not already_available and not_in_unavailable and new_peer.is_some:
            _new_peer = new_peer.unwrap()
            self.__log.info({
                "event":"AVAILABLE",
                "new":True,
                "peer_id":peer_id
            })
            self.available_peers.append(_new_peer)
            return True
        else:
            return False
    def make_unavailable(self,peer_id:str)->bool:
        available_peer         = next(filter(lambda up: up.peer_id==peer_id ,self.available_peers), None)
        if not available_peer:
            return False
        # print("AVAILABLE_PEER", available_peer)
        self.available_peers     = list(filter(lambda up: not up.peer_id==peer_id ,self.available_peers))
        already_unavailable      = len(list(filter(lambda ap: ap.peer_id == peer_id, self.unavailable_peers)))==1
        if not already_unavailable:
            self.unavailable_peers.append(available_peer)
            if peer_id in self.__peers_stats_responses:
                del self.__peers_stats_responses[peer_id]
            self.__log.info({
                "event":"UNAVAILABLE",
                "peer_id":peer_id
            })
            return True
        return False
        



    
    async def __get_ufs(self,
        peer:Peer,
        timeout:int=60,
        headers:Dict[str,str]={},
        tries:int = 100,
        delay:int = 1,
        max_delay:int = 5,
        jitter:float = 0.0,
        backoff:float =1,
        logger:Any = None
    )->Tuple[Peer, Result[GetUFSResponse,Exception]]:
        return (peer,peer.get_ufs_with_retry(
            headers=headers,
            delay=delay,
            max_delay=max_delay,
            backoff=backoff,
            jitter=jitter,
            logger=logger,
            tries=tries, timeout=timeout
        ))
    async def get_ufs_tasks(self,
        peers:List[Peer],
        timeout:int=60,
        headers:Dict[str,str]={},
        tries:int = 2,
        delay:int = 1,
        max_delay:int = 5,
        jitter:float = 0.0,
        backoff:float =1,
        logger:Any = None
    ):
        for peer in peers:
            yield asyncio.create_task(self.__get_ufs(
                peer=peer,
                headers=headers,
                delay=delay,
                max_delay=max_delay,
                backoff=backoff,
                jitter=jitter,
                logger=logger,
                tries=tries, 
                timeout=timeout
            ))
        
    async def check_peers_availability(self) -> Tuple[List[str], List[str]]:
        """Returns a tuple of the list of the peers ids -> (available, unavailable) """
        # async with self.lock.reader_lock:
        peers = (await self.get_available_peers()) + (await self.get_peers())
            # f len(self.peers) == 0 else self.peers

        counter                = 0
        # self.unavailable_peers = []
        # available              = []
        # print("PEERS",peers)
        tasks = [t async for t in self.get_ufs_tasks(peers= peers)]
        xs = await asyncio.gather(*tasks)
        for (peer, get_ufs_response) in xs:
            try:
                # get_ufs_response  = peer.get_ufs_with_retry(tries=self.max_tries,timeout=15)

                if get_ufs_response.is_ok:
                    get_uf_result              = get_ufs_response.unwrap()
                    peer_stats            = self.__peer_ufs.get(peer.peer_id,PeerStats(peer_id=peer.peer_id))
                    async with self.lock.writer_lock:
                        peer_stats.total_disk = get_uf_result.total_disk
                        peer_stats.used_disk  = get_uf_result.used_disk
                        x                     = await self.make_available(peer_id=peer.peer_id)
                        counter                +=1
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
