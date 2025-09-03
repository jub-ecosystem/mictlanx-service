from mictlanx.logger.log import Log
from fastapi import APIRouter,HTTPException
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from mictlanxrouter.tasksx import TaskManagerX
class TasksController():
    def __init__(self,log:Log, tm:TaskManagerX):
        self.log = log
        self.tm = tm
        self.router = APIRouter()
        self.add_routes()
    def add_routes(self):
        @self.router.get("/api/v4/tasks")
        async def get_tasks(start:int =0, end:int = 0):
            try:
                tasks = (await self.tm.get_all_tasks())
                if end <=0:
                    _end = len(tasks)
                else:
                    _end = end
                
                if start >= _end:
                    _start=0
                else :
                    _start = start
                return JSONResponse(
                    content=jsonable_encoder({
                        "tasks":tasks[_start:_end]
                    })
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))   
        @self.router.delete("/api/v4/tasks")
        async def delete_all_tasks():
            try:
                tasks = await self.tm.remove_all()
                return JSONResponse(
                    content=jsonable_encoder({
                        "tasks":tasks.unwrap_or([])
                    })
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))   