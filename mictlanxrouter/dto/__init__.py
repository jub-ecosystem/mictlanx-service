from pydantic import BaseModel
from typing import List
from mictlanxrouter.dto.index import DeletedByBallIdResponse,DeletedByKeyResponse
from mictlanxrouter.dto.tasks import TaskX, TaskStatus,Operations
from  mictlanxrouter.dto.metadata import PutMetadataDTO

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