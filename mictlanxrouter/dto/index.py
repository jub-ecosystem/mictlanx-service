from typing import Optional
from pydantic import BaseModel
from pydantic.dataclasses import dataclass
from mictlanx.services import AsyncPeer
from pydantic import BaseModel


class PeerElasticPayload(BaseModel):
    rf      :Optional[int] = 1
    cpu     :Optional[int] = 1
    memory  :Optional[int] = 4000000000
    disk    :Optional[int] = 20000000000
    workers :Optional[int] = 2
    protocol:Optional[str] = "http"
    elastic :Optional[str] = "false"
    strategy:Optional[str] = "ACTIVE"

@dataclass
class PeerPayload:
    protocol:str
    peer_id:str
    hostname:str
    port:int
    def to_peer(self,api_version:int = 4):
        return AsyncPeer(ip_addr=self.hostname, peer_id=self.peer_id,port=self.port,protocol=self.protocol,api_version=api_version)


class DeletedByKeyResponse(BaseModel):
    n_deletes:int
    key:str

class DeletedByBallIdResponse(BaseModel):
    n_deletes:int
    ball_id:str