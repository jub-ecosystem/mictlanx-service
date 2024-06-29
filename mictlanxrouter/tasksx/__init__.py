from abc import ABC, abstractmethod
from dataclasses import dataclass
from option import Result,Ok,Err
import aiorwlock

@dataclass
class TaskX:
    task_id:str
    operation:str
    bucket_id:str
    key:str
    size:int
    peer_id:str=""

class TaskManagerX(ABC):
    def __init__(self,max_tasks:int = 100,max_concurrency:int = 1,fast:bool= True ):
        self.tasks = {}
        self.max_tasks = max_tasks
        self.max_concurrency = max_concurrency
        self.lock = aiorwlock.RWLock(fast=fast)

    @abstractmethod
    async def add_task(self,task:TaskX)->Result[str,Exception]:
        return Err(Exception("add_task not implemented yet."))
    @abstractmethod
    async def delete_task(self,task_id:str)->Result[str,Exception]:
        return Err(Exception("delete_task not implemented yet."))
    @abstractmethod
    async def get_task(self, task_id: str)->Result[TaskX, Exception]:
        return Err(Exception("add_task not implemented yet."))

class DefaultTaskManager(TaskManagerX):
    def __init__(self, max_tasks: int = 100, max_concurrency: int = 1, fast: bool = True):
        super().__init__(max_tasks, max_concurrency, fast)
    async def add_task(self, task: TaskX)->Result[str,Exception]:
        try:
            async with self.lock.writer_lock:
                if self.max_tasks < len(self.tasks):
                    self.tasks[task.task_id] = task
                    return Ok(task.task_id)
        except Exception as e:
            return Err(e)
        # pass
    async def get_task(self, task_id: str)->Result[TaskX, Exception]:
        try:
            async with self.lock.reader_lock:
                if task_id in self.tasks:
                    return Ok(self.tasks[task_id])
                else:
                    return Err(Exception("Task {} not found".format(task_id)))
        except Exception as e:
            return Err(e)
    async def delete_task(self, task_id: str):
        try:
            async with self.lock.writer_lock:
                if task_id in self.tasks:
                    del self.tasks[task_id]
                    return Ok(task_id)
        except Exception as e:
            return Err(e)
        # return super().delete_task(task_id)
    