from typing import Dict,Optional
from pydantic import BaseModel

class PutMetadataDTO(BaseModel):
    bucket_id:str
    key:str
    ball_id:str
    checksum:str
    size:int
    tags:Optional[Dict[str,str]] = {}
    producer_id:str
    content_type:Optional[str] = "application/octet-stream"
    is_disabled:Optional[bool] = False
    replication_factor:Optional[int] = 1 