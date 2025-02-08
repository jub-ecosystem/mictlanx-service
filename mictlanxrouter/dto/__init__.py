from pydantic import BaseModel
from typing import List
from mictlanxrouter.dto.tasks import TaskX, TaskStatus,Operations

class DisperseDataResult(BaseModel):
    base_id:str
    fragments:List[bytes]
    fragment_size:int
    n:int
    k: int
    m:int
    ec_type:str
    # output_dir:str
class DisperseFileResult(BaseModel):
    base_id:str
    fragments:List[str]
    fragment_size:int
    n:int
    k: int
    m:int
    ec_type:str
    output_dir:str