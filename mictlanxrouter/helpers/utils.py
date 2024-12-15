import os
from option import Some, NONE,Result,Ok,Err
import time as T
from mictlanx.v4.summoner.summoner import SummonContainerPayload,MountX,ExposedPort
from typing import Dict,List
import humanfriendly as HF
import json as J
# from pyeclib.ec_iface import ECDriver
from mictlanxrouter.interfaces import DisperseDataResult,DisperseFileResult
# from uuid import uuid4
# import zlib
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding

class Utils:
    # @staticmethod
    # def preprocessing_data(
    #     data:bytes,
    #     output_dir:str,
    #     base_id:str,
    #     n:int,
    #     k:int,
    #     key:bytes,
    #     iv:bytes,
    #     level:int = 1,
    #     ec_type:str = "isa_l_rs_vand"
    # ):
    #     # Compress
    #     # with open(path,"rb") as f:
    #         # data            = f.read()
    #     compressed_data = Utils.compress(data=data,level=level)
    #     del data
    #     encrypted_data  = Utils.encrypt_data_aes(data=compressed_data,key=key, iv=iv)
    #     del compressed_data
    #     disperse_result = Utils.disperse_file(
    #         data       = encrypted_data,
    #         ec_type    = ec_type,
    #         k          = k,
    #         n          = n,
    #         output_dir = output_dir,
    #         base_id    = base_id
    #     )
    #     return disperse_result
    # @staticmethod
    # def unpreprocessing_data(
    #     fragments:List[bytes],
    #     output_file:str,
    #     n:int,
    #     k:int,
    #     key:bytes,
    #     iv:bytes,
    #     ec_type:str = "isa_l_rs_vand"
    # )->bytes:
    #     encrypted_data  = Utils.reconstruct_data(fragments=fragments,output_file=output_file,k=k,m=n-k,ec_type=ec_type)
    #     compressed_data = Utils.decrypt_data_aes(data=encrypted_data,key=key, iv=iv)
    #     data            = Utils.decompress(data=compressed_data)
    #     return data

    # @staticmethod
    # def preprocessing_file(
    #     path:str,
    #     output_dir:str,
    #     base_id:str,
    #     n:int,
    #     k:int,
    #     key:bytes,
    #     iv:bytes,
    #     level:int = 1,
    #     ec_type:str = "isa_l_rs_vand"
    # ):
    #     # Compress
    #     with open(path,"rb") as f:
    #         data            = f.read()
    #         compressed_data = Utils.compress(data=data,level=level)
    #         del data
    #         encrypted_data  = Utils.encrypt_data_aes(data=compressed_data,key=key, iv=iv)
    #         del compressed_data
    #         disperse_result = Utils.disperse_file(
    #             data       = encrypted_data,
    #             ec_type    = ec_type,
    #             k          = k,
    #             n          = n,
    #             output_dir = output_dir,
    #             base_id    = base_id
    #         )
    #         return disperse_result

    # @staticmethod
    # def compress(data:bytes,level:int=1)->bytes:
    #     return zlib.compress(data,level=level)
    # @staticmethod
    # def decompress(data:bytes,wbits:int = 15,bufsize:int = 16384)->bytes:
    #     return zlib.decompress(data,wbits=wbits,bufsize=bufsize)
    # @staticmethod
    # def encrypt_data_aes(data:bytes, key:bytes, iv:bytes):
    #     """Encrypt data using AES encryption in CBC mode."""
    #     backend = default_backend()
    #     cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=backend)
    #     encryptor = cipher.encryptor()

    #     # Apply PKCS7 padding to the data to ensure it's a multiple of the block size
    #     padder = padding.PKCS7(algorithms.AES.block_size).padder()
    #     padded_data = padder.update(data) + padder.finalize()

    #     # Encrypt the padded data
    #     encrypted_data = encryptor.update(padded_data) + encryptor.finalize()
    #     return encrypted_data
    # # Step 3: Decrypt the data (for verification)
    # @staticmethod
    # def decrypt_data_aes(encrypted_data:bytes, key:bytes, iv:bytes):
    #     """Decrypt AES-encrypted data (for testing purposes)."""
    #     backend = default_backend()
    #     cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=backend)
    #     decryptor = cipher.decryptor()

    #     decrypted_padded_data = decryptor.update(encrypted_data) + decryptor.finalize()

    #     # Unpad the data after decryption
    #     unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
    #     decrypted_data = unpadder.update(decrypted_padded_data) + unpadder.finalize()
    #     return decrypted_data
    

    # @staticmethod
    # def disperse_data(
    #     data:bytes,
    #     n:int, k:int, 
    #     # output_dir:str,
    #     ec_type:str = "jerasure_rs_vand",
    #     base_id:str =""
    # )->DisperseDataResult:
    #     """Disperse a file into n fragments, with k required to reconstruct."""
        
    #     # Create an erasure coding driver for Reed-Solomon (RS)
    #     m = n-k
    #     ec_driver = ECDriver(k=k, m=m, ec_type= ec_type)  # k = data parts, m = parity parts
    #     base_id =  uuid4().hex if base_id =="" else base_id
    #     # Read the file data
    #     # with open(file_path, 'rb') as f:
    #     #     file_data = f.read()
        
    #     # Encode the file data into n fragments
    #     encoded_fragments = ec_driver.encode(data)

    #     # Save the fragments to files
    #     # os.makedirs(output_dir, exist_ok=True)
    #     # for i, fragment in enumerate(encoded_fragments):
    #         # fragment_path = os.path.join(output_dir, f"{base_id}_{i}.part")
    #         # with open(fragment_path, 'wb') as f:
    #         #     f.write(fragment)

    #     return DisperseDataResult(
    #         fragments=encoded_fragments,
    #         fragment_size=len(encoded_fragments[0]),
    #         ec_type=ec_type,
    #         k=k,
    #         m=m,
    #         n=n,
    #         base_id=base_id,
    #         # output_dir=output_dir
    #     )
    # @staticmethod
    # def disperse_file(
    #     file_path:str,
    #     n:int, k:int, 
    #     output_dir:str,
    #     ec_type:str = "jerasure_rs_vand",
    #     base_id:str =""
    # )->DisperseFileResult:
    #     """Disperse a file into n fragments, with k required to reconstruct."""
        
    #     # Create an erasure coding driver for Reed-Solomon (RS)
    #     m = n-k
    #     ec_driver = ECDriver(k=k, m=m, ec_type= ec_type)  # k = data parts, m = parity parts
    #     base_id =  uuid4().hex if base_id =="" else base_id
    #     # Read the file data
    #     with open(file_path, 'rb') as f:
    #         data = f.read()
        
    #     # Encode the file data into n fragments
    #     encoded_fragments = ec_driver.encode(data)

    #     # Save the fragments to files
    #     os.makedirs(output_dir, exist_ok=True)
    #     fragments_path = []
    #     for i, fragment in enumerate(encoded_fragments):
    #         fragment_path = os.path.join(output_dir, f"{base_id}_{i}.part")
    #         fragments_path.append(fragment_path)
    #         with open(fragment_path, 'wb') as f:
    #             f.write(fragment)

    #     return DisperseFileResult(
    #         fragments=fragments_path,
    #         fragment_size=len(encoded_fragments[0]),
    #         ec_type=ec_type,
    #         k=k,
    #         m=m,
    #         n=n,
    #         base_id=base_id,
    #         output_dir=output_dir
    #     )
    #     # return len(encoded_fragments), len(encoded_fragments[0])
    
    # @staticmethod
    # def reconstruct_file(fragments_path:List[str], output_file:str, k:int,m:int, ec_type:str = "jerasure_rs_vand"):
    #     """Reconstruct a file from k fragments."""
        
    #     fragments = []
    #     for fragment_path in fragments_path:
    #         with open(fragment_path,"rb") as f:
    #             fragments.append(f.read())

    #     # Create an erasure coding driver for Reed-Solomon (RS)
    #     ec_driver = ECDriver(k=k, m=m, ec_type=ec_type)
        
    #     # Reconstruct the original file from the fragments
    #     reconstructed_data = ec_driver.decode(fragments)
        
    #     # Save the reconstructed file
    #     with open(output_file, 'wb') as f:
    #         f.write(reconstructed_data)
    #     return output_file
    # @staticmethod
    # def reconstruct_data(fragments:List[bytes], k:int,m:int, ec_type:str = "jerasure_rs_vand"):
    #     """Reconstruct a file from k fragments."""
        
    #     # Create an erasure coding driver for Reed-Solomon (RS)
    #     ec_driver = ECDriver(k=k, m=m, ec_type=ec_type)
        
    #     # Reconstruct the original file from the fragments
    #     reconstructed_data = ec_driver.decode(fragments)
        
    #     # Save the reconstructed file
    #     return reconstructed_data
    #     # with open(output_file, 'wb') as f:
    #         # f.write(reconstructed_data)
    #     # return output_file
    #     # print(f"File reconstructed successfully as {output_file}")
    
    @staticmethod
    def is_true_from_str(x:str):
        if x is not None:
            return x.lower() in ["true", "1", "t", "y", "yes"]
        else:
            return False
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
