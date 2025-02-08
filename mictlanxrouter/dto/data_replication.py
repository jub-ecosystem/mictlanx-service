from dataclasses import dataclass
from pydantic import BaseModel,Field
from typing import Optional,List
from nanoid import generate as nanoid
import time as T
import string
from enum import Enum
from option import Result,Ok,Err,Option,NONE,Some

ALPHABET = string.digits + string.ascii_lowercase
class ReplicationTaskStatus(Enum):
    PENDING = "Pending"
    IN_PROGRESS = "In Progress"
    COMPLETED = "Completed"
    # 
    FAILED = "Failed"
    # 
    CANCELLED = "Cancelled"

class ReplicationTask(object):
    def __init__(self,
                 bucket_id:str,
                 key:str,
                 task_id:str="",
                 rf:int = 0,
                 elastic:bool=False,
                 pivot_peer_id:str="",
                 strategy:str="ACTIVE",
                 index:int =0,
                 prev_task_id:str ="",
                 status:ReplicationTaskStatus = ReplicationTaskStatus.PENDING,
                 detail:str="",
                 replicas:List[str]=[],force:bool = False
    ):
        # self.__task_id = task_id if index ==0 else base_task
        self.task_id   = nanoid() if len(task_id) == 0 else task_id
        self.status    = status
        self.bucket_id = bucket_id
        self.key       = key
        self.replication_event_id = "{}@{}".format(bucket_id,key)
        self.rf        = rf
        self.elastic = elastic
        self.pivot_peer_id = pivot_peer_id
        self.strategy = strategy
        self.created_at = T.time()
        self.prev_task_id = prev_task_id
        self.index        = index
        self.detail       = detail
        self.replicas     = replicas
        self.force        = force
    
    def set_pending(self):
        self.status= ReplicationTaskStatus.PENDING
    def set_in_progress(self):
        self.status = ReplicationTaskStatus.IN_PROGRESS
    def set_completed(self):
        self.status = ReplicationTaskStatus.COMPLETED
    def set_failed(self):
        self.status= ReplicationTaskStatus.FAILED
    def set_cancelled(self):
        self.status= ReplicationTaskStatus.CANCELLED


    def link_task(self,status:ReplicationTaskStatus = ReplicationTaskStatus.PENDING, detail:str="")->'ReplicationTask':
        return ReplicationTask(
            bucket_id=self.bucket_id,
            key=self.key,
            elastic=self.elastic,
            index=self.index+1,
            pivot_peer_id=self.pivot_peer_id,
            prev_task_id=self.task_id,
            task_id="",
            rf=self.rf,
            strategy=self.strategy,
            status= status,
            detail=detail,
            # force=
            # detail=self.detail
        )

    def to_dict(self):
        x = self.__dict__.copy()
        x["status"] = str(x.get("status","PENDING"))
        return x
    @staticmethod
    def from_replication_event(x:'ReplicationEvent'):
        return ReplicationTask(
            bucket_id=x.bucket_id,
            key= x.key,
            task_id=x.id,
            rf= x.rf,
            elastic=x.elastic=="true",
            pivot_peer_id=x.pivot_peer_id,
            strategy=x.strategy,
            force= x.force
        )
class ReplicationTasks(object):
    def __init__(self,bucket_id:str, key:str,tasks:List[ReplicationTask]=[]):
        self.id        = nanoid()
        self.bucket_id = bucket_id
        self.key       = key
        self.ckey      = "{}@{}".format(self.bucket_id,self.key)
        self.__tasks   = tasks
        self.n         = len(tasks)
        self.last_task:Option[ReplicationTask] = NONE
        self.start_at  = T.time()
        
    def add_task(self,task:ReplicationTask)->Result[str,Exception]:
        task.index = self.n
        if self.last_task.is_some:
            _last_task = self.last_task.unwrap()
            if _last_task.status == ReplicationTaskStatus.CANCELLED:
                return Err(Exception("Task is already cancelled."))
            elif _last_task.status == ReplicationTaskStatus.COMPLETED:
                return Err(Exception("Task is already completed."))
            elif _last_task.status == ReplicationTaskStatus.IN_PROGRESS and task.status == ReplicationTaskStatus.COMPLETED:
                pass
            elif _last_task.status == ReplicationTaskStatus.IN_PROGRESS and task.status == ReplicationTaskStatus.CANCELLED:
                pass
            elif _last_task.status == ReplicationTaskStatus.IN_PROGRESS and task.status == ReplicationTaskStatus.FAILED:
                pass

            # elif 
        else:
            if not task.status == ReplicationTaskStatus.PENDING:
                return Err(Exception("Invalid initial status: {} != {}".format(task.status, ReplicationTaskStatus.PENDING)))
            self.last_task = Some(task)
            self.start_at  = T.time()
        # _______________________________
        self.__tasks.append(task)
        self.n+=1

class ReplicationEvent(BaseModel):
    id:str = Field(default_factory=lambda: str(nanoid(alphabet= ALPHABET )))
    rf:int 
    elastic:Optional[bool] = False
    pivot_peer_id:Optional[str] =""
    bucket_id:Optional[str]=""
    key:Optional[str]=""
    strategy:Optional[str] = "ACTIVE"
    force:bool = False

    @staticmethod
    def from_replication_task(x:ReplicationTask)->"ReplicationEvent":
        return ReplicationEvent(
            bucket_id=x.bucket_id,
            key=x.key,
            elastic=x.elastic,
            id=x.task_id,
            pivot_peer_id=x.pivot_peer_id,
            rf=x.rf,
            strategy=x.strategy
        )
    def get_combined_key_str(self):
        return "{}@{}".format(self.bucket_id,self.key)
    def __str__(self):
        return self.model_dump_json(indent=4)
    # def get_combined_key(self):
        # return H.

@dataclass
class ReplicatedBall:
    bucket_id:str
    key:str
    combined_key:str
    size:int
    replicated_at:float
@dataclass 
class ReplicationProcessResponse:
    bucket_id:str
    key:str
    combined_key_str:str
    left_replicas:int
    replicas:List[str]
    @staticmethod
    def empty(bucket_id:str="",key:str=""):
        return ReplicationProcessResponse(
            bucket_id=bucket_id,
            key=key,
            combined_key_str="{}@{}".format(bucket_id,key),
            left_replicas=0,
            replicas=[]
        )
