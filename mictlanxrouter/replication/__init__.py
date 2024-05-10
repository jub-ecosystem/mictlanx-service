import os
import asyncio
from mictlanx.logger.log import Log
import time as T
import humanfriendly as HF
from mictlanx.v4.interfaces.index import Peer,PeerStats
from mictlanxrouter.interfaces.healer import PeerHealer
from concurrent.futures import ThreadPoolExecutor
from typing import List,Dict,Any,Tuple
import httpx as R


class Replicator:
    def __init__(self,ph:PeerHealer,strategy:str = "FLOOD",strategy_mode:str="STATIC",show_logs:bool=True) -> None:
        self.is_running = True
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
    
    async def __replication_process(self,bucket_id:str,key:str, peer:Peer):
        x = peer.replicate(bucket_id=bucket_id,key=key,timeout=60,headers={})
        self.__log.debug({
            "event":"REPLICATION.PROCESS",
            "bucket_id":bucket_id,
            "key":key,
            "peer_id":peer.peer_id,
            "strategy":self.strategy,
            "mode":self.strategy_mode,
            "status":x.is_ok
        })
        self.replicated_objects.setdefault("{}@{}".format(bucket_id,key) ,{
            "bucket_id":bucket_id,
            "key":key,
            "replicated_at":T.time()
        })
        # if self.strategy =="FLOOD":
            # peer.get
        # print("Replication",bucket_id, key)
    async def replication_check(self,bucket_id:str,key:str):
        for peer in self.ph.peers:
            get_size_result = peer.get_size(bucket_id=bucket_id, key=key, timeout=60)
            # if psize.

            if get_size_result.is_ok:
                ball_size_response =get_size_result.unwrap()
                ball_size =ball_size_response.size
                if ball_size == 0:
                    await self.__replication_process(bucket_id=bucket_id, key=key, peer= peer)
                # self.__log.debug({
                #     "event":"REPLICATOR.BALL.CHECK",
                #     "bucket_id": bucket_id,
                #     "key":key,
                #     "size":ball_size,
                #     "peer_id":ball_size_response.peer_id
                # })
    async def run(self):
        # responses:List[Tuple[Peer, Dict[str,Any]]] = []
        balls_global = []
        for peer in self.ph.peers:
            try:
                headers = {}
                # url = "{}/stats".format(peer.base_url())
                url = "{}/api/v{}/stats".format(peer.base_url(),self.api_version)
                response = R.get(url,headers=headers)
                _ = response.raise_for_status()
                res_json = response.json()
                balls = res_json.get("balls",[])
                balls_global+=balls
                # responses.append((peer,res_json))
            except Exception as e:
                self.__log.error({
                    "msg":str(e)
                })
                continue
        # with ThreadPoolExecutor(max_workers=4) as tp:
        tasks = []
        for ball in balls_global:
            bucket_id = ball.get("bucket_id","BUCKET_ID")
            key = ball.get("key","KEY")
            combined_key = "{}@{}".format(bucket_id,key)
            if combined_key in self.replicated_objects:
                self.__log.debug({
                    "event":"REPLICATION.PROCESS.SKIPPED",
                    "bucket_id":bucket_id,
                    "key":key
                })
                continue
            if self.strategy_mode == "DYNAMIC":
                if key in self.to_replicate:
                     tasks.append(self.replication_check(bucket_id=bucket_id,key=key))
            else:
                tasks.append(self.replication_check(bucket_id=bucket_id,key=key))
        await asyncio.gather(*tasks)
            # await asyncio.sleep(10)
        # for (peer,stats) in responses:
            # balls = stats["balls"]
            # for ball in balls:
                # bucket_id = ball["bucket_id"]
                # key       = ball["key"]
                # checksum  = ball["checksum"]
                # get_sizes = peer.get

    