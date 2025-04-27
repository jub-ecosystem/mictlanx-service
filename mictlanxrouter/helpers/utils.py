import os
from option import Some, NONE,Result,Ok,Err
import time as T
from fastapi import Request
from mictlanx.v4.summoner.summoner import SummonContainerPayload,MountX,ExposedPort
from typing import Dict,List,AsyncGenerator
import humanfriendly as HF
import json as J
import httpx
import asyncio

class Utils:
    @staticmethod
    async def safe_content_generator(peer_resp: httpx.Response,
                                    chunk_size: int,
                                    logger,
                                    ctx: str):
        """
        Relay bytes from the peer (inner HTTP call) to the outer client.
        * Never* raise after the first chunk has been yielded.
        """
        try:
            async for chunk in peer_resp.aiter_bytes(chunk_size):
                yield chunk                    # real data
                await asyncio.sleep(0)         # cooperative scheduling
        except Exception as exc:
            logger.warning({
                "event": "PEER.STREAM.FAIL",
                "ctx":   ctx,
                "detail": repr(exc)
            })
            # send a zero‑byte frame so Starlette finishes the response cleanly
            yield b""
        finally:
            await peer_resp.aclose()
    @staticmethod
    def dict_to_string(d):
        # Convert each key-value pair to a "key=value" string and join them with spaces
        return ' '.join(f"{key}={value}" for key, value in d.items())
    @staticmethod
    def is_true_from_str(x:str):
        if x is not None:
            return x.lower() in ["true", "1", "t", "y", "yes"]
        else:
            return False
    @staticmethod
    async def get_raw(request: Request) -> bytes:
        # Starlette caches the result, so this is safe *once*
        raw = await request.body()
        return raw
    @staticmethod
    def bytes_to_stream(data: bytes, chunk_size: int = 64 * 1024) -> AsyncGenerator[bytes, None]:
        """
        Lazily yield `data` in `chunk`‑sized pieces.
        Number of chunks  = ceil(len(data) / chunk).
        """
        async def _gen() -> AsyncGenerator[bytes, None]:
            for i in range(0, len(data), chunk_size):
                # zero‑copy slice, O(1)
                yield data[i : i + chunk_size]
        return _gen()
    @staticmethod
    def read_peers(path:str)->Result[Dict[str,SummonContainerPayload],Exception]:
        # MICTLANX_ROUTER_PEERS_JSON_PATH = "/home/nacho/Programming/Python/mictlanx-router/peers.json"
        try:
            peers_configs:Dict[str, SummonContainerPayload] = {}
            if  path != -1 and os.path.exists(str(path)):
                with open(path,"rb") as f:
                    data = J.loads(f.read())
                    for k,v in data.items():
                        peers_configs[k] = SummonContainerPayload(
                            container_id= v.get("container_id",k),
                            cpu_count=int(v.get("cpu_count","2")),
                            envs=v.get("envs"),
                            exposed_ports=list(map(lambda p: ExposedPort(**p, ip_addr=NONE, protocolo=NONE),v.get("exposed_ports",[]))),
                            force=Some(v.get("force")),
                            hostname=v.get("hostname",k),
                            image=v.get("image"),
                            ip_addr=Some(v.get("ip_addr","0.0.0.0")),
                            labels=v.get("labels"),
                            memory=int(v.get("memory", HF.parse_size("4GB"))),
                            mounts=list(map(lambda m: MountX(**m),v.get("mounts",[]))),
                            network_id=v.get("network_id","mictlanx"),
                            selected_node=Some(v.get("selected_node")),
                            shm_size= NONE
                        )
                        # x = peers_configs[k]
                        # print(x.__dict__)
                        # print(x.exposed_ports[0])
                        # T.sleep(100)
            return Ok(peers_configs)
        except Exception as e:
            return Err(e)
    @staticmethod
    def save_peers(path:str, peers_config:Dict[str, SummonContainerPayload]={})->Result[bool, Exception]:
        try:
            with open(path,"w") as f :
                raw = {}
                for k,v in peers_config.items():
                    raw[k] = v.to_dict()
                J.dump(raw,f, indent=4)
            return Ok(True)
        except Exception as e:
            return Err(e)
