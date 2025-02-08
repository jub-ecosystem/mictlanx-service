from threading import Thread
from mictlanx.logger.log import Log
# from threading import Thread
from typing import List,Dict,Any,Tuple
# import requests as R
import time as T
import humanfriendly as HF
from mictlanx.v4.interfaces.index import Peer,PeerStats
from mictlanxrouter.peer_manager.healer import StoragePeerManager
import httpx as R

class MictlanXFMGarbageCollector:
    """
        This class represents the daemon thread that executes every <hearbeat> seconds.
    """
    def __init__(self,ph:StoragePeerManager,api_version:str=4,show_logs:bool=False) -> None:
        # Thread.__init__(self,name=name,daemon=daemon)
        self.is_running = True
        # self.heartbeat = HF.parse_timespan(heartbeat)
        self.__log = Log(
            name = "mictlanx-router-gc-0",
            console_handler_filter=lambda x: show_logs,
            interval=24,
            when="h"
        )
        self.api_version = api_version
        self.ph:StoragePeerManager = ph
    async def run(self):
        responses:List[Tuple[Peer, Dict[str,Any]]] = []
        # print("CURRENT_PEERS",self.ph.peers)
        for peer in self.ph.peers:
            try:
                headers = {}
                # url = "{}/stats".format(peer.base_url())
                url = "{}/api/v{}/stats".format(peer.base_url(),self.api_version)
                response = R.get(url,headers=headers)
                _ = response.raise_for_status()
                res_json = response.json()
                responses.append((peer,res_json))
                self.__log.debug({
                    "event":"CHECK.CONSISTENCY",
                    "peer_id":peer.peer_id,
                })
            except Exception as e:
                self.__log.error({
                    "msg":str(e)
                })
                continue
        for (peer,stats) in responses:
            balls = stats["balls"]
            for ball in balls:
                bucket_id = ball["bucket_id"]
                key       = ball["key"]
                checksum  = ball["checksum"]

    # def run(self) -> None:
        # while self.is_running:
            # for peer in peers:
            #     try:
            #         responses:List[Tuple[Peer, Dict[str,Any]]] = []
            #         url = "{}/api/v{}/stats".format(peer.base_url(),MICTLANX_API_VERSION)
            #         response = R.get(url,headers=headers)
            #         response.raise_for_status()
            #         res_json = response.json()
            #         # print("CHECK_CONSISTENCY", peer.peer_id)
            #         responses.append((peer,res_json))
            #         # ________________________________
                    
            #         for (peer,stats) in responses:
            #             balls = stats["balls"]
            #             for ball in balls:
            #                 bucket_id = ball["bucket_id"]
            #                 key       = ball["key"]
            #                 checksum  = ball["checksum"]
            #     except R.exceptions.HTTPError as e:
            #         self.__log.error({
            #             "msg":str(e.response.content.decode("utf8") ),
            #         })
            #         continue
            #     except R.exceptions.ConnectionError as e:
            #         self.__log.error({
            #             "msg":"Connection error - {} / {}:{}".format(peer.peer_id,peer.ip_addr,peer.port),
            #         })
            #         continue

            #     except Exception as e:
            #         self.__log.error({
            #             "msg":str(e)
            #         })
            #         continue
            #     finally:
            #         T.sleep(self.heartbeat)
            #     # peer.get