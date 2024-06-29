
import unittest as UT
import asyncio
from mictlanxrouter.replication import ReplicaManager
from mictlanxrouter.peer_manager.healer import StoragePeerManager
from mictlanx.v4.interfaces.index import Peer,PeerStats
from queue import Queue

class MictlanXReplication(UT.TestCase):
    def test_replication_manager(self):
        q = Queue()
        peers = [
            Peer(
                peer_id="mictlanx-peer-0",
                ip_addr="localhost",
                port=25000,
                protocol="http"
            ),
            Peer(
                peer_id="mictlanx-peer-1",
                ip_addr="localhost",
                port=25001,
                protocol="http"
            ),
            Peer(
                peer_id="mictlanx-peer-2",
                ip_addr="localhost",
                port=25002,
                protocol="http"
            )
        ]
        rm = ReplicaManager(ph = StoragePeerManager(q = q, peers=peers))
        # res = rm.create_replicas(bucket_id="b1", key="k1", selected_replicas=[])
        # res = rm.create_replicas(bucket_id="b1", key="k1", selected_replicas=[])
        # apeers = rm.get_available_peers(bucket_id="b1",key = "k1")
        # print(rm.get_replicated_peers(bucket_id="b1",key="k1"))
        # res = rm.create_replicas(bucket_id="b1", key="k1", selected_replicas=["p1"])
        # print(res)
        # # rm.remove_replicas(bucket_id="b1",key="k1", to_remove_replicas=["p1"])
        # res = rm.create_replicas(bucket_id="b1", key="k1", selected_replicas=["p1","p2"])
        # print(res)
        # res = rm.create_replicas(bucket_id="b1", key="k1", selected_replicas=["p1","p2","p3","p4"])
        # print(res)



if __name__ == "__main__":
    UT.main()

