import asyncio
from mictlanx.logger.log import Log

import json as J
from mictlanxrouter.helpers.utils import Utils as RouterUtils
from mictlanx.utils.index import Utils as MictlanXUtils
import humanfriendly as HF
from queue import Queue
from typing import List,Dict,Literal,Tuple,Any
from option import Result,Ok,Err
import requests as R
import time as T
from option import NONE,Some,Option
import aiorwlock
from mictlanx.v4.summoner.summoner import Summoner
from mictlanx.interfaces import SummonContainerPayload,ExposedPort,MountX,SummonContainerResponse,PeerStats,PeerStatsResponse,GetUFSResponse
from mictlanx.v4.interfaces.index import AsyncPeer as Peer

from dataclasses import dataclass
from opentelemetry.trace import Span,Status,StatusCode
from opentelemetry.sdk.trace import Tracer
# from opentelemetry.sdk
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
        peer_docker_image:str = "nachocode/mictlanx:peer-0.0.159",
        peer_min_interval_time:int = 5,
        peer_max_interval_time:int = 20,
        
        max_timeout_peers_handshake:str ="30s",
        tick_peers_handshake:str = "5s",
        recover_tick_timeout:str ="1m"
    ):
        self.tick_peers_handshake = tick_peers_handshake
        self.max_timeout_peers_handshake = max_timeout_peers_handshake
        self.peer_min_interval_time = peer_min_interval_time
        self.peer_max_interval_time = peer_max_interval_time
        self.peer_docker_image = peer_docker_image
        self.peer_elastic = peer_elastic
        self.max_retries = max_retries
        self.max_idle_time = max_idle_time
        self.queue_tick_timeout = queue_tick_timeout
        # self.max_idle_time = max_idle_time
        self.queue_tick_timeout_in_seconds = HF.parse_timespan(queue_tick_timeout)
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
        self.recover_tick_timeout = recover_tick_timeout

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
        tracer:Tracer,
        q:Queue,
        peers:List[Peer],
        summoner:Summoner,
        name: str="mictlanx-peer-manager-0",
        show_logs:bool=True,
        params:Option[StoragePeerManagerParams] = NONE,
        q_maxsize:int = 1000
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
        self.tracer:Tracer = tracer
        self.params        = params.unwrap() if params.is_some else StoragePeerManagerParams()
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
        self.last_handshakes = T.time()
        self.last_to_save_peer_config = T.time()
        self.q= q
        self.main_queue = asyncio.Queue(maxsize=q_maxsize)

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
    def run_main_queue(self):
        return asyncio.create_task(self.main_queue_worker())

    async def main_queue_worker(self):
        while self.is_running:
            event = await self.main_queue.get()
            print("EVENT",event)
            await asyncio.sleep(1)

    async def get_params(self,)->StoragePeerManagerParams:
        async with self.params_lock.reader_lock:
            return self.params
        
    async def puts(self, peer_ids:List[str], size:int=0):
        async with self.lock.writer_lock:
            for peer_id in peer_ids:
                await self.put(peer_id=peer_id, size=size)
    
    async def put(self,peer_id:str,size:int=0):
        async with self.lock.writer_lock:
            if peer_id in self.__peers_stats_responses:
                current_p_stats = self.__peers_stats_responses[peer_id]
                used = current_p_stats.used_disk

                if current_p_stats.available_disk >= size and (used+size) <= current_p_stats.total_disk: 
                    current_p_stats.used_disk+= size
                    current_p_stats.available_disk-= size
                    current_p_stats.disk_uf = MictlanXUtils.calculate_disk_uf(total=current_p_stats.total_disk, used=used,size=size)
                    self.__peers_stats_responses[peer_id]= current_p_stats
                    

                # .put(size = size)
            # self.__peers_stats_responses[peer_id]

    async def update_params(self,**kwargs):
        async with self.params_lock.writer_lock:
            self.max_timeout_to_recover         = HF.parse_timespan(kwargs.get("max_timeout_to_recover",self.params.max_timeout_to_recover))
            self.max_recover_time_until_restart = HF.parse_timespan(kwargs.get("max_recover_time_until_restart",self.params.max_recover_time_until_restart))
            self.max_timeout_to_save_peer_config = HF.parse_timespan(kwargs.get("max_timeout_to_save_peer_config", self.params.max_timeout_to_save_peer_config))

            self.params.update(**kwargs)

    async def get_current_peers_env_str(self)->str:
        peers = await self.get_available_peers()
        peers_str = list(map(lambda p:"{}:{}".format(p.peer_id,p.port), peers))
        return " ".join(peers_str)
    
    async def active_deploy_peers(
        self,
        disk:int= HF.parse_size("10GB"),
        memory:int = HF.parse_size("2GB"),
        workers:int = 2,
        rf:int = 1,
        cpu:int = 1,
        elastic:str="true",
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
            selected_node = (n+i)%len(self.params.physical_nodes_indexes)
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
                          elastic:str="true",
                          base_path:str= "/app/mictlanx",
                          network_id:str = "mictlanx",
                          max_idle_time:str = "1m"
    )->Tuple[str, int,Result[SummonContainerResponse,Exception]]:
        """return the <container_id> <port> <result>"""
        with self.tracer.start_as_current_span("spm.deploy.peer") as span:
            span:Span = span
            try:
                _start_time = T.time()
                params      = await self.get_params()
                local_path  = "{}/local".format(base_path)
                log_path    = "{}/log".format(base_path)
                data_path   = "{}/data".format(base_path)
                envs = {
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
                        "MIN_INTERVAL_TIME":str(self.params.peer_min_interval_time),
                        "MAX_INTERVAL_TIME":str(self.params.peer_max_interval_time), 
                        "WORKERS":str(workers),
                        "ELASTIC":elastic,
                        "MAX_IDLE_TIME":str(HF.parse_timespan(max_idle_time)),
                }
                payload:SummonContainerPayload         =self.peers_config.get(container_id,SummonContainerPayload(
                    container_id=container_id,
                    # image=os.environ.get("MICTLANX_PEER_IMAGE","nachocode/mictlanx:peer"),
                    image=self.params.peer_docker_image,
                    hostname    = container_id,
                    exposed_ports=[ExposedPort(NONE,port,port,NONE)],
                    envs= envs,
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
                env_peers = await self.get_current_peers_env_str()
                payload.envs["PEERS"]= env_peers
                
                response        = self.summoner.summon(
                    mode= params.summoner_mode,
                    payload=payload, 
                )
                if response.is_ok:
                    span.add_event(name="spm.deployed.peer",attributes={
                        "image":params.peer_docker_image,
                        "hostname":container_id,
                        "exposed_ports":[port],
                        "local_path":local_path,
                        "log_path":log_path,
                        "data_path":data_path,
                        "memory":HF.format_size(memory),
                        "network_id":network_id,
                        "selected_node":selected_node,
                        "force":True,
                        "PEERS":env_peers,
                        
                        **envs
                    })
                    span.set_status(status=Status(StatusCode.OK))
                else:
                    span.add_event(name="spm.deploy.failed",attributes={"detail":str(response.unwrap_err()),"container_id":container_id})
                    span.set_status(status=Status(StatusCode.ERROR))
                return (container_id,port ,response)
            except Exception as e:
                span.set_status(status=Status(StatusCode.ERROR))
                span.add_event(name="spm.deploy.peer.exception", attributes={"detail":str(e)})

    async def __find_peer(self, peer_id:str, peers:List[Peer])->Option[Peer]:
        async with self.lock.reader_lock:
            x = next(filter(lambda p: p.peer_id == peer_id, peers), -1)
            return  NONE if x == -1 else Some(x)
    
    async def sorted_by_uf(self,size:int=0)->List[str]:
        with self.tracer.start_as_current_span("spm.sorted_by_uf") as span:
            span:Span = span
            async with self.lock.reader_lock:
                p_stats = list(self.__peers_stats_responses.values())
                
                
                # ufs = dict(list(map(lambda x: (x.peer_id,J.dumps({"uf":x.disk_uf, "used":x.used_disk,"available":x.available_disk,"total":x.total_disk})), p_stats)))
                sorted_p = sorted(p_stats,key=lambda x: x.disk_uf)
                size_filtered = list(filter(
                    lambda p: p.available_disk >= size,sorted_p
                ))
                if len(size_filtered) ==0:
                    span.add_event(name="capacity.filter", attributes={"size":size,"error":True})
                peers_ids = list(map(lambda x: x.peer_id,   size_filtered ) )
                span.add_event(name="sorted.peers",attributes={"filtered_peers": peers_ids})
                for ps in p_stats:
                    span.set_attribute(key="{}.uf".format(ps.peer_id), value=ps.disk_uf)
                    span.set_attribute(key="{}.total".format(ps.peer_id), value=ps.total_disk)
                    span.set_attribute(key="{}.used".format(ps.peer_id), value=ps.used_disk)
                    span.set_attribute(key="{}.available".format(ps.peer_id), value=ps.available_disk)
                    span.set_attribute(key="{}.n_balls".format(ps.peer_id), value=len(ps.balls))
                    # span.add_event(name= "{}.stats".format(ps.peer_id), attributes={
                    #     "uf":ps.disk_uf,
                    #     "total":ps.total_disk,
                    #     "used":ps.used_disk,
                    #     "available":ps.available_disk,
                    #     "n_balls":len(ps.balls)
                    # })

                # span.set_attributes({
                    # **ufs
                # })
                return peers_ids
 
    
    
    
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

    async def get_len_available_peers(self)->int:
        async with self.lock.reader_lock:
            return len(self.available_peers)

    async def get_len_unavailable_peers(self)->int:
        async with self.lock.reader_lock:
            return len(self.unavailable_peers)

        
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
    
   
    async def leave_peer(self,peer_id:str)->bool:
        async with self.lock.writer_lock:
            n = len(self.available_peers + self.unavailable_peers)
            self.available_peers  = list(filter(lambda peer: peer.peer_id != peer_id, self.available_peers))
            self.unavailable_peers  = list(filter(lambda peer: peer.peer_id != peer_id, self.unavailable_peers))
            return n > len(self.available_peers) + len(self.unavailable_peers)

    def get_stats(self):
        return self.__peer_ufs
    
    async def stats(self)->List[PeerStatsResponse]:
        """Traverse the peers and get the stats /api/v4/peers/stats"""
        # with self.tracer.start_as_current_span("stats")
        available_peers = await self.get_available_peers()
        xs=  [(await p.get_stats()).unwrap_or(PeerStatsResponse.empty()) for p in available_peers ]
        xs_dict = dict([(p.peer_id, p) for p in xs])
        async with self.lock.writer_lock:
            self.__peers_stats_responses = xs_dict
        return xs
    
    async def recover_daemon(self):
        while True:
            current_time                         = T.time()
            elapsed_time                         = current_time - self.last_recover_tick
            if elapsed_time >= self.max_timeout_to_recover:
                recovered_peers = await self.recover(
                    params=self.params
                )
                self.__log.info({
                    "event":"RECOVER",
                    "recovered_peers":list(map(lambda x:x.peer_id,recovered_peers)),
                    "service_time":T.time()-current_time
                })
                self.last_recover_tick = T.time()
            await asyncio.sleep(self.params.recover_tick_timeout)

    async def recover(self,params:StoragePeerManagerParams):
        with self.tracer.start_as_current_span("spm.recover") as span:
            span:Span = span
            try:
                available_peers   = set(await self.get_available_peers_ids())
                unavailable_peers = set(await self.get_unavailable_peers_ids())
                to_recover_peers  = unavailable_peers.difference(available_peers)
                recovered_peers:List[Peer]   = []
                span.add_event(name="spm.recover.init",attributes={"current_peers":list(available_peers), "unavailable_peers":list(unavailable_peers), "to_recover_peers":list(to_recover_peers) })
                for unavailable_peer in self.unavailable_peers:
                    
                    if unavailable_peer.peer_id in to_recover_peers:
                        last_time = self.time_last_recover_by_peer.setdefault(unavailable_peer.peer_id,T.time())
                        elapsed   = T.time() - last_time
                        elapsed_str = HF.format_timespan(elapsed)
                        span.add_event(name="spm.recover.{}".format(unavailable_peer.peer_id), attributes={
                            "elapsed": elapsed_str,
                            "max_recover_time_until_restart": self.max_recover_time_until_restart,
                            "elastic":params.peer_elastic
                        })
                        
                        if elapsed >= self.max_recover_time_until_restart and params.peer_elastic:
                            (_,_,res) = await self.deploy_peer(container_id=unavailable_peer.peer_id,port=unavailable_peer.port)
                            self.time_last_recover_by_peer[unavailable_peer.peer_id] = T.time()
                            if res.is_err:
                                span.add_event(name="spm.recover.failed",attributes={"peer_id":unavailable_peer.peer_id})
                                span.set_status(status=Status(StatusCode.ERROR))
                            else:
                                span.add_event(name="spm.recovered",attributes={
                                    "peer_id":unavailable_peer.peer_id,
                                    "elapsed":elapsed_str,
                                    "max_timeout":self.max_recover_time_until_restart,
                                    "ok":res.is_ok
                                })
                                async with self.lock.writer_lock:
                                    recovered_peers.append(unavailable_peer)
                                    self.peers.append(unavailable_peer)
                                
                        elif elapsed >= self.max_recover_time_until_restart and not params.peer_elastic:
                            span.add_event(name="spm.elasticity.disabled",attributes={
                                "peer_id":unavailable_peer.peer_id,
                                "elapsed":elapsed_str,
                            })
                return recovered_peers
            

            except Exception as e:
                span.set_status(status=Status(StatusCode.ERROR))
                span.add_event(name="recover.exception",attributes={"detail":str(e)})
                # self.__log.error({
                #     "event":"RECOVER.EXCEPTION",
                #     "detail":str(e)
                # })
    
    async def handshake_worker(self):
        while True:
            current_time                         = T.time()
            elapsed_time_handshakes              = current_time - self.last_handshakes

            if elapsed_time_handshakes >= HF.parse_timespan(self.params.max_timeout_peers_handshake): 
                self.__log.debug({
                    "event":"HANDSHAKE",
                    "service_time":T.time()-current_time
                })
                await self.handshakes()
                self.last_handshakes = T.time()
            await asyncio.sleep(HF.parse_timespan(self.params.tick_peers_handshake))
            
    async def handshakes(self):
        with self.tracer.start_as_current_span("spm.handshakes") as span:
            span:Span = span
            availables = await self.get_available_peers()
            # print("AVAILABLES", len(availables))
            for p in availables:
                for p2 in availables:
                    if p.peer_id == p2.peer_id:
                        continue
                    stats = self.get_stats().get(p2.peer_id)
                    # print("STATS", stats)
                    if stats is None:
                        continue

                    result = await p.add_peer(
                        id   = p2.peer_id,
                        disk =stats.total_disk,
                        used_disk=0,
                        ip_addr=p2.ip_addr,
                        memory=HF.parse_size("2GB"),
                        port=p2.port,
                        used_memory=0,
                        weight=1
                    )
                    # print("RESULT", result)
                    if result.is_err:
                        span.set_status(status=Status(StatusCode.ERROR))
                        span.add_event(name="add.peer.failed", attributes={
                            "detail":str(result.unwrap_err()),
                            "peer_id":p.peer_id,
                            "added_peer_id":p2.peer_id,
                            "disk":stats.total_disk,
                            "ip_addr":p2.ip_addr,
                            "port":p2.port,
                            # "ok":result.is_ok
                        })
                    else:
                        span.add_event(name="added.peer", attributes={
                            "peer_id":p.peer_id,
                            "added_peer_id":p2.peer_id,
                            "disk":stats.total_disk,
                            "ip_addr":p2.ip_addr,
                            "port":p2.port,
                            "ok":result.is_ok
                        })
    
    async def run(self):
        with self.tracer.start_as_current_span("spm.run.healer") as span:
            span:Span = span
            try:
                current_time                         = T.time()
                params                               = await self.get_params()
                (available_peers, unavailable_peers) = await self.check_peers_availability()
                current_stats                        = await self.stats()
                self.global_stats                    = sum(current_stats, PeerStatsResponse.empty())

                elapsed_time                         = current_time - self.last_recover_tick
                elapsed_time_to_save_peer_config     = current_time - self.last_to_save_peer_config
                # elapsed_time_handshakes              = current_time - self.last_handshakes
                span.add_event(name="tick",attributes={
                    "elapsed_time":HF.format_timespan(elapsed_time),
                    "max_timeout_to_recover":params.max_timeout_to_recover,
                    "available_peers":available_peers,
                    "unavailable_peers":unavailable_peers,
                })
              
                # if elapsed_time_handshakes >= HF.parse_timespan(self.params.max_timeout_peers_handshake): 
                #     await self.handshakes()
                #     self.last_handshakes = T.time()
                    
                if elapsed_time_to_save_peer_config >= self.max_timeout_to_save_peer_config:
                    res = RouterUtils.save_peers(path=params.peers_config_path,peers_config=self.peers_config)
                    span.add_event(name="save.peers.config", attributes={"timeout":self.max_timeout_to_save_peer_config})
          
                    self.last_to_save_peer_config = T.time()
                    
                # if elapsed_time >= self.max
                return Ok(0)
            except Exception as e:
                span.add_event(name="spm.run.exception",attributes={"detail":str(e)})
                span.set_status(status=Status(StatusCode.ERROR))
                # self.__log.error({
                #     "event":"SPM.RUN.EXCEPTION",
                # "detail":str(e)
                # })
                return Err(e)
        # finally:
            # self.last_tick = T.time()
    
    async def make_available(self,peer_id:str)->bool:
        with self.tracer.start_as_current_span("spm.make.available") as span:
            span:Span = span

            unavailable_peer       = next(filter(lambda up: up.peer_id==peer_id ,self.unavailable_peers), -1)
            not_in_unavailable     = unavailable_peer == -1
            self.unavailable_peers = list(filter(lambda up: not up.peer_id==peer_id ,self.unavailable_peers))
            already_available      = len(list(filter(lambda ap: ap.peer_id == peer_id, self.available_peers))) == 1
            new_peer               = await self.__find_peer(peer_id=peer_id, peers= self.peers)

            if not already_available and not not_in_unavailable:
       
                span.add_event(name="available", attributes={
                   "peer_id":unavailable_peer.peer_id
                })
                self.available_peers.append(unavailable_peer)
                return True

            elif not already_available and not_in_unavailable and new_peer.is_some:
                _new_peer = new_peer.unwrap()
            
                span.add_event(name="available", attributes={
                   "peer_id":_new_peer.peer_id
                })

                self.available_peers.append(_new_peer)
                return True
            else:
                span.add_event(name="available.failed", attributes={"peer_id":peer_id})
                return False
            
    def make_unavailable(self,peer_id:str)->bool:
        with self.tracer.start_as_current_span("spm.make.unavailble") as span:
            span:Span = span
            available_peer         = next(filter(lambda up: up.peer_id==peer_id ,self.available_peers), None)
            if not available_peer:
                span.add_event(name="unavailable.failed",attributes={"peer_id":peer_id})
                return False
            self.available_peers     = list(filter(lambda up: not up.peer_id==peer_id ,self.available_peers))
            already_unavailable      = len(list(filter(lambda ap: ap.peer_id == peer_id, self.unavailable_peers)))==1
            if not already_unavailable:
                self.unavailable_peers.append(available_peer)
                if peer_id in self.__peers_stats_responses:
                    del self.__peers_stats_responses[peer_id]
                span.add_event(name="unavailable",attributes={"peer_id":peer_id})
            
                return True
            span.add_event(name="unavailable.failed",attributes={"peer_id":peer_id})
            return False
        
    
    async def get_unavailable_peers(self):
        async with self.lock.reader_lock:
            return self.unavailable_peers


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
        return (peer,await peer.get_ufs_with_retry(
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
        
    


    async def get_available_peers_ids(self):
        async with self.lock.reader_lock:
            return list(set(list(map(lambda x:x.peer_id, self.available_peers))))
 
    async def get_unavailable_peers_ids(self)->List[str]:
        async with self.lock.reader_lock:
            return list(set(list(map(lambda x:x.peer_id, self.unavailable_peers))))

    async def get_available_peers(self)->List[Peer]:
        async with self.lock.reader_lock:
            return self.available_peers
    async def get_peers(self):
        async with self.lock.reader_lock:
            return self.peers

    async def check_peers_availability(self) -> Tuple[List[str], List[str]]:
        """Returns a tuple of the list of the peers ids -> (available, unavailable) """
        with self.tracer.start_as_current_span("spm.check.peers.availability") as span:
            span:Span = span
            peers     = (await self.get_available_peers()) + (await self.get_peers())

            peer_available_counter   = 0
            get_ufs_tasks     = [t async for t in self.get_ufs_tasks(peers= peers)]

            get_ufs_results        = await asyncio.gather(*get_ufs_tasks)
            peers_ids = list(map(lambda x:x.peer_id, peers))
            span.add_event("spm.check.peers.availability.init",attributes={
                "peers":peers_ids
            })


            for (peer, get_ufs_result) in get_ufs_results:
                try:
                    if get_ufs_result.is_ok:
                        
                        get_uf_response              = get_ufs_result.unwrap()
                        # self.get
                        peer_stats            = self.__peer_ufs.get(peer.peer_id,PeerStats(peer_id=peer.peer_id))
                        peer_stats.total_disk = get_uf_response.total_disk
                        peer_stats.used_disk  = get_uf_response.used_disk
                        async with self.lock.writer_lock:
                            x                     = await self.make_available(peer_id=peer.peer_id)
                            peer_available_counter                +=1
                            self.__peer_ufs[peer.peer_id] = peer_stats
                        span.add_event(name="spm.peer.available",attributes={
                            "peer_id":peer.peer_id,
                            "total":get_uf_response.total_disk,
                            "used":get_uf_response.used_disk,
                            "counter":peer_available_counter
                        })
                        # self.__log.debug("Peer {} is  available".format(peer.peer_id))
                    else:
                        async with self.lock.writer_lock:
                            x = self.make_unavailable(peer_id=peer.peer_id)
                        span.add_event(name="spm.peer.unavailable",attributes={
                            "peer_id":peer.peer_id,
                            # "total":get_uf_result.total_disk,
                            # "used":get_uf_result.used_disk,
                            "counter":peer_available_counter
                        })
                        
                        # self.__log.error("Peer {} is not available.".format(peer.peer_id))
                    
                    
                    denominator = (len(await self.get_available_peers()) + len(await self.get_unavailable_peers()))

                    percentage_available_peers = 0 if denominator ==0 else (peer_available_counter / denominator ) *100 
                    if percentage_available_peers == 0:
                        span.set_status(status=Status(StatusCode.ERROR))
                        span.add_event(name="spm.peers.ALL.UNAVAIABLE", attributes={
                            "detail":"No available peers. Please contact me on jesus.castillo.b@cinvestav.mx"
                        })
                        raise Exception("No available peers. Please contact me on jesus.castillo.b@cinvestav.mx")
                    
                    # self.__log.debug("{}% of the peers are available".format(percentage_available_peers ))
            
                except R.exceptions.ConnectTimeout as e:
                    span.set_status(status=Status(StatusCode.ERROR))
                    span.add_event(name="spm.check.peer.availability.connect.timeout",attributes={
                        "peer_id":peer.peer_id,
                        "detail":str(e)
                    })
                    # self.__log.error({
                    #     "msg":"ConnectionTimeout",
                    #     "peer_id":peer.peer_id
                    # })

                except R.exceptions.ConnectionError as e:
                    span.set_status(status=Status(StatusCode.ERROR))
                    span.add_event(name="spm.check.peer.availability.connect.error",attributes={
                        "peer_id":peer.peer_id,
                        "detail":str(e)
                    })
                    # self.__log.error({
                    #     "msg":"ConnectionError",
                    #     "peer_id":peer.peer_id
                    # })
                except R.exceptions.HTTPError as e:
                    span.set_status(status=Status(StatusCode.ERROR))
                    span.add_event(name="spm.check.peer.availability.http.error",attributes={
                        "peer_id":peer.peer_id,
                        "detail":str(e)
                    })
                    # self.__log.error({
                    #     "msg":str(e.response.content.decode("utf8") ),
                    #     "peer_id":peer.peer_id
                    # })
                    # continue
                except Exception as e:
                    span.set_status(status=Status(StatusCode.ERROR))
                    span.add_event(name="spm.check.peer.availability.exception",attributes={
                        "peer_id":peer.peer_id,
                        "detail":str(e)
                    })
            # Clean peers
            async with self.lock.writer_lock:
                self.peers = []
            available_peers_ids = await self.get_available_peers_ids()
            unavailable_peers_ids = await self.get_unavailable_peers_ids()
            span.add_event(name="spm.peers.availability.checked",attributes={
                "available":available_peers_ids,
                "unavailable":unavailable_peers_ids
            })
            return (available_peers_ids, unavailable_peers_ids)
            # continue
            # finally:
            #     T.sleep(self.heartbeat)
                # print(e)
