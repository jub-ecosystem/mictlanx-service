from abc import ABC, abstractmethod
from option import Result,Ok,Err
import aiorwlock
from typing import List
from mictlanxrouter.dto import TaskX

class TaskManagerX(ABC):
    def __init__(self,max_tasks:int = 100,max_concurrency:int = 1,fast:bool= True ):
        self.tasks = {}
        self.max_tasks = max_tasks
        self.max_concurrency = max_concurrency
        self.lock = aiorwlock.RWLock(fast=fast)

    @abstractmethod
    async def remove_all(self)->Result[List[str],Exception]:
        return Err(Exception("remove_all not implemented yet."))
    @abstractmethod
    async def add_task(self,task:TaskX)->Result[str,Exception]:
        return Err(Exception("add_task not implemented yet."))
    @abstractmethod
    async def delete_task(self,task_id:str)->Result[str,Exception]:
        return Err(Exception("delete_task not implemented yet."))
    @abstractmethod
    async def get_task(self, task_id: str)->Result[TaskX, Exception]:
        return Err(Exception("get_task not implemented yet."))
    @abstractmethod
    async def get_all_tasks(self, )->List[TaskX]:
        return Err(Exception("get_all_tasks not implemented yet."))

class DefaultTaskManager(TaskManagerX):
    def __init__(self, max_tasks: int = 100, max_concurrency: int = 1, fast: bool = True):
        super().__init__(max_tasks, max_concurrency, fast)
    async def remove_all(self) -> Result[List[str], Exception]:
        try:
            async with self.lock.writer_lock:
                tasks_ids = list(self.tasks.keys())
                self.tasks.clear()
                return Ok(tasks_ids)
        except Exception as e:
            return []
        # return await super().remove_all()
    async def get_all_tasks(self) -> List[TaskX]:
        async with self.lock.reader_lock:
            return list(self.tasks.values())
        # return await super().get_all_tasks()
    async def add_task(self, task: TaskX)->Result[str,Exception]:
        try:
            async with self.lock.writer_lock:
                if len(self.tasks) < self.max_tasks:
                    self.tasks[task.task_id] = task
                    return Ok(task.task_id)
                else:
                    raise Exception("Max. Queue Limit Reached.")
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
    