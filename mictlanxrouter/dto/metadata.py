from typing import Dict,Union,List
from uuid import uuid4
from pydantic.dataclasses import dataclass

@dataclass
class Metadata:
    bucket_id:str
    key:str
    ball_id:str
    checksum:str
    size:int
    tags:Dict[str,str]
    producer_id:str
    content_type:str
    is_disabled:bool
    replication_factor:int = 1 



# {
#   "key":"bac9b6c65bb832e7a23f936f8b1fdd00051913fc0c483cf6a6f63f89e6588b80",
#   "ball_id":"bac9b6c65bb832e7a23f936f8b1fdd00051913fc0c483cf6a6f63f89e6588b80",
#   "checksum":"bac9b6c65bb832e7a23f936f8b1fdd00051913fc0c483cf6a6f63f89e6588b80",
#   "size":110857,
#   "tags":{
#     "description":"Nice you are watching the metadata about this object.",
#     "more-desc":"You can save anything you want.",
#     "more":"The idea is that you create your own logic to manage the tags in an object."    
#   },
#   "producer_id":"jcastillo", 
#   "bucket_id":"mictlanx",
#   "content_type":"application/pdf", 
#   "is_disable":false
# }