from fastapi.routing import APIRouter
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
# from mictlanxrouter.interfaces.dto.metadata import Rep
from mictlanxrouter.dto.data_replication import ReplicationEvent
from mictlanxrouter.replication_manager import DataReplicator
from mictlanx.logger.log import Log
import time as T


class DataReplicatorController():
    def __init__(self,log:Log, data_replicator:DataReplicator ):
        self.log    = log
        self.data_replicator = data_replicator
        self.router = APIRouter()
        self.add_routes()
    def add_routes(self):
        @self.router.post("/api/v4/replicate")
        async def replication(replication_event:ReplicationEvent):
            try:
                start_time = T.time()
                response_time = T.time() - start_time
                await self.data_replicator.put_task(replication_event)
                
                return JSONResponse(
                    content=jsonable_encoder(
                        {
                            "replication_event_id":replication_event.id,
                            "response_time":response_time
                        }
                    )
                )
            except Exception as e:
                self.log.error({
                    "msg":str(e)
                })
                raise e

        @self.router.get("/api/v4/replicate")
        async def get_replication():
            try:
                start_time = T.time()
                tasks = await self.data_replicator.get_tasks()
                
                response_time = T.time() - start_time
                self.log.info({
                    "event":"REPLICATION.TASKS",
                    "n":len(tasks),
                    "response_time":response_time
                })
                # print(str(replication_event))
                return JSONResponse(
                    content=jsonable_encoder(tasks)
                )
            except Exception as e:
                self.log.error({
                    "msg":str(e)
                })
                raise e


