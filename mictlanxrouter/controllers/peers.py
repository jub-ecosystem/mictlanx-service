from fastapi.routing import APIRouter
from fastapi import HTTPException,Response,FastAPI
from mictlanxrouter.dto.index import PeerPayload
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from mictlanxrouter.peer_manager.healer import StoragePeerManager
from option import Result,Ok,Err
import asyncio
import requests as R
from typing import List
import time as T
from mictlanx.logger.log import Log
from opentelemetry.trace import Tracer

class PeersController():
    def __init__(self, 
            log:Log,
            storage_peer_manager:StoragePeerManager,
            tracer:Tracer
        ):
        self.log = log
        self.storage_peer_manager = storage_peer_manager
        self.router = APIRouter(prefix="/api/v4")
        self.tracer = tracer
        self.add_routes()

    async def fx(self,peer_payload:PeerPayload)->Result[str,Exception]:
        # async with peer_healer_rwlock.writer_lock:
        try:
            status = await self.storage_peer_manager.add_peer(peer_payload.to_v4peer())
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
        ):
            try:
                start_time = T.time()
                peers_ids  = list(map(lambda p: p.peer_id, peers))
                tasks      = [ self.fx(peer_payload=p) for p in peers]
                res        = list(filter(lambda x: len(x)>0,map(lambda r: r.unwrap_or(""),await asyncio.gather(*tasks))))
                self.log.info({
                    "event":"PEERS.ADDED",
                    "added_peers":res,
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
        async def get_peers():
            try:
                available = await self.storage_peer_manager.get_available_peers_ids()
                unavailable = await self.storage_peer_manager.get_unavailable_peers_ids()
                return JSONResponse(
                    content=jsonable_encoder({
                        "available":available,
                        "unavailable":unavailable,
                    })
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        @self.router.post("/peers")
        async def add_peer(peer:PeerPayload):
            try:
                start_time = T.time()
                asyncio.gather(
                    self.fx(peer_payload=peer)
                )
                self.log.info({
                    "event":"PEER.ADDED",
                    "peer_id":peer.peer_id,
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
        async def remove_peer(peer_id:str):
            try:
                start_time = T.time()
                res=  await self.storage_peer_manager.remove_peer(peer_id=peer_id)

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
        async def remove_peer(peers:List[str]=[]):
            try:
                start_time = T.time()
                for peer_id in peers:
                    res=  await self.storage_peer_manager.remove_peer(peer_id=peer_id)

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

        @self.router.post("/peers/{peer_id}")
        async def leave_peer(peer_id:str):
            try:
                start_time = T.time()
                res=  await self.storage_peer_manager.leave_peer(peer_id=peer_id)

                self.log.info({
                    "event":"REMOVE.PEER",
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

        # > ======================= Peers ============================
        @self.router.get("/peers/stats")
        async def get_peers_stats(
            start:int =0 ,
            end:int = 100
        ):
            
            try:
                with self.tracer.start_as_current_span("peers.stats") as span:
                    responses = []
                    # async with peer_healer_rwlock.reader_lock:
                    peers = await self.storage_peer_manager.get_available_peers()
                    failed = 0 
                    for peer in peers:
                        try:
                            result = peer.get_stats(timeout=10, start=start, end=end)
                            
                            if result.is_ok:
                                stats_response = result.unwrap()
                                responses.append(stats_response.__dict__)
                                self.log.debug({
                                    "event":"STATS",
                                    "peer_id":peer.peer_id,
                                    "start":start,
                                    "skip":end,
                                    "used_disk":stats_response.used_disk,
                                    "total_disk":stats_response.total_disk,
                                    "available_disk":stats_response.available_disk,
                                    "disk_uf":stats_response.disk_uf,
                                    "balls":len(stats_response.balls)
                                    # "balls":len(res_json["balls"])
                                })
                        except R.exceptions.ConnectionError as e:
                            detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
                            self.log.error({
                                "detail":detail,
                                "status_code":500
                            })
                            failed+=1
                        except Exception as e:
                            failed+=1
                            self.log.error(str(e))

                    return JSONResponse(
                        content=jsonable_encoder(responses),
                        headers={
                            "N-Peers-Failed":str(failed)
                        }
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
            except R.exceptions.ConnectionError as e:
                detail = "Connection error - peers unavailable - {}".format(peer.peer_id)
                self.log.error({
                    "detail":detail,
                    "status_code":500
                })
                raise HTTPException(status_code=500, detail=detail  )
