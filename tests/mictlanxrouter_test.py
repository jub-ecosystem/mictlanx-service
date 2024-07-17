import os
import unittest as UT
from mictlanx.v4.summoner.summoner import SummonContainerPayload,MountX,ExposedPort
import json as J
from option import Some, NONE
from mictlanxrouter.helpers.utils import Utils

class MictlanXRouter(UT.TestCase):
    
    @UT.skip()
    def test_load_peers(self):
        MICTLANX_ROUTER_PEERS_JSON_PATH = "/home/nacho/Programming/Python/mictlanx-router/peers.json"
        peers_config = Utils.read_peers(path = MICTLANX_ROUTER_PEERS_JSON_PATH)
        print(peers_config)
        MICTLANX_ROUTER_PEERS_JSON_PATH = "/home/nacho/Programming/Python/mictlanx-router/peers2.json"
        Utils.save_peers(path= MICTLANX_ROUTER_PEERS_JSON_PATH, peers_config= peers_config)
        # peers_configs = {}
        # if MICTLANX_ROUTER_PEERS_JSON_PATH != -1 and os.path.exists(str(MICTLANX_ROUTER_PEERS_JSON_PATH)):
        #     with open(MICTLANX_ROUTER_PEERS_JSON_PATH,"rb") as f:
        #         data = J.loads(f.read())
        #         for k,v in data.items():
        #             peers_configs[k] = SummonContainerPayload(
        #                 container_id= v.get("container_id",k),
        #                 cpu_count=int(v.get("cpu_count","2")),
        #                 envs=v.get("envs"),
        #                 exposed_ports=map(lambda p: ExposedPort(**p, ip_addr=NONE, protocolo=NONE),v.get("exposed_ports",[])),
        #                 force=Some(v.get("force")),
        #                 hostname=v.get("hostname",k),
        #                 image=v.get("image"),
        #                 ip_addr=Some(v.get("ip_addr","0.0.0.0")),
        #                 labels=v.get("labels"),
        #                 memory=v.get("memory"),
        #                 mounts=map(lambda m: MountX(**m),v.get("mounts",[])),
        #                 network_id=v.get("network_id","mictlanx"),
        #                 selected_node=Some(v.get("selected_node")),
        #                 shm_size= NONE
        #             )

        # print(peers_configs["mictlanx-peer-0"].to_dict() )
            



if __name__ == "__main__":
    UT.main()

