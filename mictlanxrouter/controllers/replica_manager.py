from mictlanxrouter.replication_manager import ReplicaManager
from fastapi import APIRouter,HTTPException
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from mictlanx.logger.log import Log

class ReplicaManagerController():
    def __init__(self,log:Log,replica_manager:ReplicaManager):
        self.replica_manager = replica_manager
        self.log = log
        self.router = APIRouter()
        self.add_routes()

    def add_routes(self):
        @self.router.get("/api/v4/rm/{bucket_id}/{key}")
        async def get_replica_map_by_ball(bucket_id:str, key:str):
            try:
                available = await self.replica_manager.get_available_peers_ids(bucket_id=bucket_id,key=key,size=0)
                current_replicas = await self.replica_manager.get_current_replicas_ids(bucket_id=bucket_id,key=key)
                # unavailable = await storage_peer_manager.get_unavailable_peers_ids()
                return JSONResponse(
                    content=jsonable_encoder({
                        "bucket_id":bucket_id,
                        "key":key,
                        "available":available,
                        "replicas":current_replicas,
                    })
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.get("/api/v4/rm")
        async def get_replica_map(start:int =0, end:int = 0):
            try:
                replica_map = await self.replica_manager.get_replica_map()
                return JSONResponse(
                    content=jsonable_encoder(replica_map)
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))   
        @self.router.get("/api/v4/accessmap")
        async def get_access_map(start:int=0, end:int =0,no_access:bool = True):
            try:
                x = await self.replica_manager.get_access_replica_map()
                if end <=0:
                    _end = len(x)
                else:
                    _end = end
                
                if start >= _end:
                    _start=0
                else :
                    _start = start
                

                filtered_x = filter(lambda b: b[1] > 0 or no_access ,x.items())
                # filtered_x = filter(lambda b: b[1] > 0 or no_access ,x.items())
                xs = dict(list(  filtered_x   )[_start:_end] )
                xs = dict(sorted(xs.items(), key=lambda x:x[1]))
                
                if len(xs) ==0:
                    await self.replica_manager.q.put("")
                return JSONResponse(
                    content=jsonable_encoder( xs  )
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))