
import unittest as UT
import asyncio
from mictlanxrouter.replication import ReplicaManager
from mictlanxrouter.interfaces.healer import PeerHealer
from mictlanx.v4.interfaces.index import Peer,PeerStats
from queue import Queue

class MictlanXReplication(UT.TestCase):
    def test_replication_manager(self):
        q = Queue()
        peers = [
            Peer(
                peer_id="p1",
                ip_addr="p1",
                port=1,
                protocol="http"
            ),
            Peer(
                peer_id="p2",
                ip_addr="p2",
                port=2,
                protocol="http"
            ),
            Peer(
                peer_id="p3",
                ip_addr="p3",
                port=3,
                protocol="http"
            )
        ]
        rm = ReplicaManager(ph = PeerHealer(q = q, peers=peers))
        res = rm.create_replicas(bucket_id="b1", key="k1", selected_replicas=["p1"])
        print(res)
        
        rm.remove_replicas(bucket_id="b1",key="k1", to_remove_replicas=["p1"])
        res = rm.create_replicas(bucket_id="b1", key="k1", selected_replicas=["p1","p2"])
        print(res)



if __name__ == "__main__":
    UT.main()

