from fastapi.routing import APIRouter
from fastapi import HTTPException,Response,Request,Depends
from mictlanxrouter.dto.index import PeerPayload
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from mictlanxrouter.peer_manager.healer import StoragePeerManager
import os
from option import Result,Ok,Err
import asyncio
import requests as R
from typing import List
import time as T
from mictlanx.logger.log import Log
from opentelemetry.trace import Tracer
from mictlanxrouter.dto.index import PeerElasticPayload
from mictlanx.v4.interfaces.index import AsyncPeer
from mictlanxrm.client import SPMClient
import mictlanxrm.models as MX

class PeersController():
    def __init__(self, 
            log:Log,
            tracer:Tracer,
            network_id:str ="mictlanx",
            max_peers_rf:int = 5
        ):
        self.log = log
        self.router = APIRouter(prefix="/api/v4")
        self.tracer = tracer
        self.network_id = network_id
        self.max_peers_rf = max_peers_rf
        # self.spm_client = SPMClient()
        self.add_routes()

    def get_spm_client(self):
        client = SPMClient(
            hostname=os.environ.get("MICTLANX_DAEMON_HOSTNAME","localhost"),
            port= int(os.environ.get("MICTLANX_DAEMON_PORT","5555")),
            protocol= os.environ.get("MICTLANX_DAEMON_PROTOCOL","tcp")
        )
        try:
            yield client
        except Exception as e: 
            self.log.error({
                "event":"GET.SPM.CLIENT.FAILED",
                "detail":str(e)
            })
            raise e
        finally:
            client.socket.close()
    async def fx(self,peer_payload:PeerPayload)->Result[str,Exception]:
        # async with peer_healer_rwlock.writer_lock:
        try:
            _p  = peer_payload.to_v4peer()
            peer = AsyncPeer(peer_id=_p.peer_id, ip_addr=_p.ip_addr,port=_p.port,protocol=_p.protocol)
            status = await self.storage_peer_manager.add_peer(peer)
            self.log.debug({
                "event":"PEER.ADDED",
                "peer_id":peer_payload.peer_id,
                "protocol":peer_payload.protocol,
                "hostname":peer_payload.hostname,
                "port":peer_payload.port,
                "status":status
            })
            return Ok(peer_payload.peer_id)
        except Exception as e:
            self.log.error({
                "detail":str(e),
                "status_code":500
            })
            return Err(e)

    def add_routes(self):
        @self.router.post("/xpeers")
        async def add_peers(
            peers:List[PeerPayload],
            spm_client:SPMClient = Depends(self.get_spm_client)
        ):
            try:
                start_time = T.time()
                res = await spm_client.add_peers(
                    params = MX.AddPeersParams(
                        peers =  [MX.PeerModel(**p.to_v4peer().__dict__) for p in peers]
                    )
                )
                # peers_ids  = list(map(lambda p: p.peer_id, peers))
                # tasks      = [ self.fx(peer_payload=p) for p in peers]
                # res        = list(filter(lambda x: len(x)>0,map(lambda r: r.unwrap_or(""),await asyncio.gather(*tasks))))
                self.log.info({
                    "event":"PEERS.ADDED",
                    "added_peers":res.unwrap_or(False),
                    "response_time": T.time() - start_time
                })
                return Response(content=None, status_code=204)

            except HTTPException as e:
                self.log.error({
                    "event":"HTTP.EXCEPTION",
                    "detail":e.detail,
                    "status_code":e.status_code
                })
                return HTTPException(status_code=500, detail = str(e))
            except Exception as e:
                self.log.error({
                    "event":"ADD.PEERS.FAILED",
                    "msg":str(e)
                })
                raise HTTPException(
                    status_code=500,
                    detail="ADD.PEERS.FAILED",

                )
            
        @self.router.get("/peers")
        async def get_peers(spm_client:SPMClient = Depends(self.get_spm_client)):
            try:
                peers_res = await spm_client.get_peers()
                if peers_res.is_err:
                    e  = peers_res.unwrap_err()
                    raise HTTPException(status_code=500, detail=str(e))
                peers = peers_res.unwrap()
                available= peers.available
                # self.storage_peer_manager.get_available_peers_ids()
                unavailable = peers.unavailable
                # await self.storage_peer_manager.get_unavailable_peers_ids()
                return JSONResponse(
                    content=jsonable_encoder({
                        "available":available,
                        "unavailable":unavailable,
                    })
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        @self.router.post("/peers")
        async def add_peer(peer:PeerPayload,spm_client:SPMClient = Depends(self.get_spm_client)):
            try:
                start_time = T.time()
                res = await spm_client.add_peers(
                    params = MX.AddPeersParams(
                        peers =  [MX.PeerModel(**peer.to_v4peer().__dict__)]
                    )
                )
                # asyncio.gather(
                #     self.fx(peer_payload=peer)
                # )
                self.log.info({
                    "event":"PEER.ADDED",
                    "peer_id":peer.peer_id,
                    "ok":res.unwrap_or(False),
                    "response_time": T.time() - start_time
                })
                return Response(content=None, status_code=204)

            except HTTPException as e:
                self.log.error({
                    "event":"HTTP.EXCEPTION",
                    "detail":e.detail,
                    "status_code":e.status_code
                })
                raise HTTPException(status_code=e.status_code, detail = e.detail, headers=e.headers)
            except R.exceptions.HTTPError as e:
                detail      = str(e.response.content.decode("utf-8") )
                status_code = e.response.status_code
                self.log.error({
                    "detail":detail,
                    "status_code":status_code,
                    "reason":e.response.reason,
                })
                raise HTTPException(status_code=status_code, detail=detail  )
            except R.exceptions.ConnectionError as e:
                detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
                self.log.error({
                    "detail":detail,
                    "status_code":500
                })
                raise HTTPException(status_code=500, detail=detail  )
            except Exception as e:
                self.log.error({
                    "event":"ADD.PEERS.FAILED",
                    "msg":str(e)
                })
                raise HTTPException(
                    status_code=500,
                    detail="ADD.PEERS.FAILED",

                )


        @self.router.delete("/peers/{peer_id}")
        async def remove_peer(peer_id:str,spm_client:SPMClient = Depends(self.get_spm_client)):
            try:
                start_time = T.time()
                res=  await spm_client.delete_peers(peer_ids=[peer_id])

                self.log.info({
                    "event":"LEAVE.PEER",
                    "peer_id":peer_id,
                    "ok":res,
                    "response_time":T.time() - start_time
                })
                return Response(content=None, status_code=204)

            except HTTPException as e:
                self.log.error({
                    "event":"HTTP.EXCEPTION",
                    "detail":e.detail,
                    "status_code":e.status_code
                })
                raise HTTPException(status_code=e.status_code, detail = e.detail, headers=e.headers)
            except R.exceptions.HTTPError as e:
                detail = str(e.response.content.decode("utf-8") )
                # e.response.reason
                status_code = e.response.status_code
                self.log.error({
                    "detail":detail,
                    "status_code":status_code,
                    "reason":e.response.reason,
                })
                raise HTTPException(status_code=status_code, detail=detail  )
            except Exception as e:
                self.log.error({
                    "msg":str(e),
                })
                raise HTTPException(status_code=500, detail="Something went wrong removin a peer {}".format(peer_id))
        @self.router.delete("/xpeers")
        async def remove_peer(peers:List[str]=[],spm_client:SPMClient = Depends(self.get_spm_client)):
            try:
                start_time = T.time()
                res=  await spm_client.delete_peers(peer_ids=peers)

                self.log.info({
                    "event":"LEAVE.PEER",
                    "peer_ids":peers,
                    "ok":res,
                    "response_time":T.time() - start_time
                })
                return Response(content=None, status_code=204)

            except HTTPException as e:
                self.log.error({
                    "event":"HTTP.EXCEPTION",
                    "detail":e.detail,
                    "status_code":e.status_code
                })
                raise HTTPException(status_code=e.status_code, detail = e.detail, headers=e.headers)
            except R.exceptions.HTTPError as e:
                detail = str(e.response.content.decode("utf-8") )
                # e.response.reason
                status_code = e.response.status_code
                self.log.error({
                    "detail":detail,
                    "status_code":status_code,
                    "reason":e.response.reason,
                })
                raise HTTPException(status_code=status_code, detail=detail  )
            except Exception as e:
                self.log.error({
                    "msg":str(e),
                })
                raise HTTPException(status_code=500, detail="Something went wrong removing a peers {}".format(peers))

 
        # > ======================= Peers ============================
        @self.router.get("/peers/stats")
        async def get_peers_stats(
            start:int =0 ,
            end:int = 100,
            spm_client:SPMClient = Depends(self.get_spm_client)
        ):
            
            try:
                with self.tracer.start_as_current_span("peers.stats") as span:
                    # responses = []
                    t1 = T.time()
                    res = await spm_client.get_stats(skip= start, end=end)
                    if res.is_err:
                        self.log.error({
                            "detail":str(res.unwrap_err())
                        })
                        return {
                        }
                    _res = res.unwrap()
                    self.log.info({
                        "event":"PEER.STATS",
                        "peer_ids":list(_res.keys()),
                        "response_time":T.time()-t1
                    })
                    return JSONResponse(
                        jsonable_encoder(_res)
                    )
            except R.exceptions.HTTPError as e:
                detail = str(e.response.content.decode("utf-8") )
                # e.response.reason
                status_code = e.response.status_code
                self.log.error({
                    "detail":detail,
                    "status_code":status_code,
                    "reason":e.response.reason,
                })
                raise HTTPException(status_code=status_code, detail=detail  )
    
 

      