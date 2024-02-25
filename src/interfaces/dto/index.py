from typing import Dict,Union,List
from uuid import uuid4
from pydantic.dataclasses import dataclass
from mictlanx.v4.interfaces.index import Peer as PeerV4

@dataclass
class Peer:
    protocol:str
    peer_id:str
    hostname:str
    port:int
    def to_v4peer(self):
        return PeerV4(ip_addr=self.hostname, peer_id=self.peer_id,port=self.port,protocol=self.protocol)

