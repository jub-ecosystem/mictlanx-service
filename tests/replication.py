
import unittest as UT
import time as T
import asyncio
from mictlanxrouter.replication_manager import ReplicaManager,Pagination,ReplicaManagerParams
from mictlanxrouter.peer_manager.healer import StoragePeerManager
from mictlanx.v4.interfaces.index import Peer,PeerStats
from mictlanx.v4.summoner.summoner import Summoner
# from mictlanxrouter._async.replica_management import ReplicaManagerParams
import humanfriendly as HF
from queue import Queue

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
summoner = Summoner(ip_addr="localhost",port=15000,protocol="http")
spm = StoragePeerManager(
    q = q, 
    peers=peers,
    show_logs=True,
    summoner=summoner 
)

class MictlanXReplication(UT.IsolatedAsyncioTestCase):

    async def test_pagination(self):
        p2 = Pagination(n = 10, batch_size=5)
        p = Pagination(n = 6, batch_size=50)
        print(p.next())
        print(p.next())
        print(p.next())
        p.reset()
        print(p.next())
        
    @UT.skip("")
    async def test_rmp(self):
        rmp = ReplicaManagerParams()
        rmp2 = ReplicaManagerParams(batch_index= 1)
        rmp_check_res = rmp.check(rmp=rmp2)
        print(str(rmp2), rmp2.elapsed_time_last_update())
        T.sleep(2)
        rmp2.update(batch_index = 1 )
        T.sleep(2)
        print(str(rmp2), rmp2.elapsed_time_last_update())
        T.sleep(2)
        rmp2.update(batch_index = 2 )
        print(str(rmp2), rmp2.elapsed_time_last_update())
        # print(str(rmp2))
        print(rmp_check_res)
        return self.assertTrue(rmp_check_res.is_some)
    @UT.skip("")
    async def test_deploy_workers(self):
        res =await spm.active_deploy_peers(rf=5)
        print("RESULT",res)
        res = await spm.run()
        print("RESULT",res)
        T.sleep(5)
        res = await spm.get_available_peers_ids()
        print("RESULT",res)
        

    @UT.skip("")
    async def test_access(self):
        spm_res = await spm.run()
        rm = ReplicaManager(q=asyncio.Queue(),spm = spm, elastic=True)
        bucket_id = "b1"
        key = "k1"
        rf =3
        x = await rm.create_replicas(
            bucket_id=bucket_id,
            key=key,
            size=HF.parse_size("10MB"),
            # peer_ids=["mictlanx-peer-2","mictlanx-peer-0"],
            rf=rf
        )
        for i in range(100):
            res = await rm.access(bucket_id=bucket_id,key=key)
        print("RES",res)
        x = await rm.get_access_replica_map()
        print(x)

    @UT.skip("")
    async def test_replication_manager2(self):
        spm_res = await spm.run()
        rm = ReplicaManager(q=asyncio.Queue(),spm = spm, elastic=True)
        bucket_id = "b1"
        key = "k1"
        rf = 2
        x = await rm.create_replicas(
            bucket_id=bucket_id,
            key=key,
            size=HF.parse_size("10MB"),
            # peer_ids=["mictlanx-peer-2","mictlanx-peer-0"],
            rf=rf
        )
        print("X",x)
        current_replicas = await rm.get_current_replicas_ids(bucket_id=bucket_id,key=key)
        print("CURRENT_REPLICAS", current_replicas)
        # x = await rm.get_available_peers_ids(bucket_id=bucket_id,key=key)
        # print("AVAP",x)
    @UT.skip("")
    async def test_replication_manager(self):

        spm_res = await spm.run()
        rm = ReplicaManager(spm = spm )
        x = await rm.create_replicas(
            bucket_id="b1",
            key="k1",
            size=HF.parse_size("1GB"),
            peer_ids=["mictlanx-peer-0"],
            rf=1
        )
        current_replicas = await rm.get_current_replicas(bucket_id="b1",key="k1")
        print("CURRENT_REPLICAS", current_replicas)
        print("sorted_by_uf ",await spm.sorted_by_uf())
        print("_"*50)
        x = await rm.create_replicas(
            bucket_id="b1",
            key="k1",
            size=HF.parse_size("1GB"),
            peer_ids=["mictlanx-peer-2","mictlanx-peer-0"],
            rf=2
        )
        T.sleep(5)
        spm_res = await spm.run()
        print("RUN",spm_res)
        print("sorted_by_uf ",await spm.sorted_by_uf())
        current_replicas = await rm.get_current_replicas(bucket_id="b1",key="k1")
        print("CURRENT_REPLICAS", current_replicas)
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

