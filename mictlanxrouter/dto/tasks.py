from dataclasses import dataclass,field
from typing import List
from enum import Enum
class TaskStatus(Enum):
    NEW     = 0,
    PENDING = 1
    ASSIGNED = 2
    ACCEPTED = 3
    READY = 4
    PREPARING =5
    STARTING = 6
    RUNNING = 7
    COMPLETED = 1
    FAILED  = -1
    SHUTFOWN = -2
    REJECTED = -3
    REMOVE  = -4

class Operations(Enum):
    PUT = "PUT"
    GET = "GET"
    REPLICA = "REPLICA"


@dataclass
class TaskX:
    task_id:str
    operation:Operations
    bucket_id:str
    key:str
    size:int
    group_id: str = field(default="")
    peers:List[str]=field(default=list)
    content_type:str=field(default="application/octet-stream")
    status:TaskStatus = field(default=TaskStatus.NEW)