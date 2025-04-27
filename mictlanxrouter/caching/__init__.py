from turtle import st
import humanfriendly as HF
from abc import ABC, abstractmethod
from typing import Dict,List,OrderedDict as ODict,Tuple,Union
import os
from collections import OrderedDict, Counter
import time as T
import heapq
from option import Option, Some, NONE
from multiprocessing import shared_memory, Lock
from mictlanx.logger import Log
import logging

LOG_PATH                    = os.environ.get("LOG_PATH","/log")
MICTLANX_ROUTER_LOG_INTERVAL             = int(os.environ.get("MICTLANX_ROUTER_LOG_INTERVAL","24"))
MICTLANX_ROUTER_LOG_WHEN                 = os.environ.get("MICTLANX_ROUTER_LOG_WHEN","h")
MICTLANX_ROUTER_LOG_SHOW                 = bool(int(os.environ.get("MICTLANX_ROUTER_LOG_SHOW","1")))


def default_console_hanlder_filter(x:logging.LogRecord):
    if MICTLANX_ROUTER_LOG_SHOW:
        return True
    else:
        if x.levelno == logging.INFO or x.levelno == logging.ERROR or x.levelno == logging.WARNING:
            return True
        return False

log                         = Log(
        name                   = "mictlanx-router-cache",
        console_handler_filter = default_console_hanlder_filter,
        path= LOG_PATH, 
        interval=MICTLANX_ROUTER_LOG_INTERVAL,
        when=MICTLANX_ROUTER_LOG_WHEN
)

class XData(ABC):
    @abstractmethod
    def get_data(self)->memoryview:
        pass
    @abstractmethod
    def get_size(self)->int:
        pass
    @abstractmethod
    def close(self):
        pass
    @abstractmethod
    def __len__(self)->int:
        pass
    @abstractmethod
    def __enter__(self)->'XData':
        pass
    @abstractmethod
    def __exit__(self,a,b,c,d):
        pass

class MemoryData(XData):
    def __init__(self, data:memoryview):
        self.data = data
        self.size = len(data)
    def get_data(self):
        return self.data
    def get_size(self):
        return self.size
    def close(self):
        return super().close()
    def __enter__(self):
        return self
    def __exit__(self, a, b, c, d):
        self.close()
    def __len__(self):
        return self.size


class SharedMemoryData(XData):
    """
    A wrapper for a SharedMemory block and its associated memoryview.
    The SharedMemory object remains open until close() is explicitly called.
    """
    def __init__(self, shm: shared_memory.SharedMemory, size: int):
        self.shm:shared_memory.SharedMemory = shm
        self.size = size
        self.data = memoryview(shm.buf[:size])

    def get_data(self):
        return self.data
    def get_size(self):
        return self.size
    def close(self):
        """
        Release the memoryview and close the shared memory.
        The caller should call close() when done using this object.
        """
        if self.data is not None:
            self.data.release()
            self.data = None
        if self.shm is not None:
            try:
                log.debug({
                    "event":"SHARED.MEMORY.DATA.CLOSED",
                    "key":self.shm.name,
                    "size":self.shm.size
                })
                self.shm.close()
                # self.shm.unlink()
            except Exception:
                pass
            self.shm = None

    
    def __len__(self):
        return self.size
    def __enter__(self):
        return self
    def __exit__(self,exc_type, exc_value, traceback):
        self.close()
    
    
    # def __del__(self):
    #     try:
    #         log.debug({
    #             "event":"DELETE.SHARED.MEMORY.DATA"
    #         })
    #         self.close()
    #     except Exception:
    #         pass
class CacheX(ABC):
    """Abstract Base Class for a simple key-value cache (Key: str -> Value: bytes/memoryview)."""
    @abstractmethod
    def get_keys(self)->List[str]:
        pass
    @abstractmethod
    def get(self, key: str) -> Option[XData]:
        """Retrieve a value from the cache."""
        pass

    @abstractmethod
    def put(self, key: str, value: bytes)->int:
        """Insert a value into the cache."""
        pass

    @abstractmethod
    def remove(self, key: str):
        """Remove a value from the cache."""
        pass

    @abstractmethod
    def __len__(self) -> int:
        """Return the current size of the cache."""
        pass

    @abstractmethod
    def clear(self):
        """Clear the cache."""
        pass
    @abstractmethod
    def get_total_storage_capacity(self):
        pass
    @abstractmethod
    def get_used_storage_capacity(self):
        pass

    @abstractmethod
    def get_uf(self):
        pass

class CacheFactory:
    @staticmethod
    def create(eviction_policy:str,capacity:int,capacity_storage:int):
        if eviction_policy == "LRU":
            return LRUCache(capacity=capacity,capacity_storage = capacity_storage)
        elif eviction_policy == "LFU":
            return LFUCache(capacity=capacity,capacity_storage = capacity_storage)
        elif eviction_policy == "LRU_SM":
            return LRUSharedMemoryCache(capacity=capacity,capacity_storage=capacity_storage)
        return LRUCache(capacity=capacity)


class LRUSharedMemoryCache(CacheX):
    """
    A cache that stores binary data in shared memory so that multiple Hypercorn workers can read from it with minimal overhead.
    Writes are protected by a lock, but reads are performed without locking.
    
    Each cache entry is stored in its own shared memory block.
    The metadata (shared memory block name and size) for each key is stored in an OrderedDict.
    """
    
    def __init__(self, capacity: int, capacity_storage: int):
        # Dictionary to hold metadata: key -> (shm_name, size)
        self.__cache: OrderedDict[str, Tuple[str, int]] = OrderedDict()
        self.capacity = capacity
        self.capacity_storage = capacity_storage
        self.used_capacity = 0
        self._lock = Lock()  # Used only for writes (put/remove)

    @staticmethod
    def _sanitize_key(key: str) -> str:
            """
            Ensures that the key is a valid shared memory name.
            On POSIX systems, shared memory names should start with a slash.
            """
            if not key.startswith("/"):
                return "/" + key
            return key
    @staticmethod
    def shared_memory_exists(name: str) -> bool:
        """
        On POSIX systems, shared memory objects created via SharedMemory
        appear under /dev/shm/ with a name equal to the shared memory name
        (without the leading slash).
        """
        # Remove the leading slash if present.
        shm_name = name.lstrip("/")
        return os.path.exists(f"/dev/shm/{shm_name}")
    def get_keys(self):
        return list(self.__cache.keys())

    def put(self, key: str, value: bytes) -> int:
        """
        Store the given bytes value in a shared memory block.
        If the key already exists, the previous entry is removed.
        Returns 0 on success, -1 on failure.
        """
        try:
            t1 = T.time()
            size = len(value)
            current_used_capacity = self.used_capacity + size
            can_store = current_used_capacity <= self.capacity_storage
            exists_in_fs = self.shared_memory_exists(name=key)
            if exists_in_fs:
                self.remove(key=key)

            exists_in_local = key in self.__cache

            log.debug({
                "event":"PUT",
                "key":key,
                "size":size,
                "used_capacity": HF.format_size(self.used_capacity),
                "available_capacity":HF.format_size(self.get_total_storage_capacity() - self.get_used_storage_capacity()),
                "total_capacity":HF.format_size(self.capacity_storage),
                "uf":self.get_uf(),
                "current_used_capacity": HF.format_size(current_used_capacity),
                "exists_fs":exists_in_fs,
                "can_store":can_store, 
                "exists_local":exists_in_local,
            })
            if exists_in_local:
                self.__cache.move_to_end(key)
                old_shm_name, _ = self.__cache[key]
                try:
                    old_shm = shared_memory.SharedMemory(name=old_shm_name)
                    old_shm.close()
                    old_shm.unlink()
                except FileNotFoundError:
                    pass

            
            # If there's not enough storage, remove least recently used items.
            elif not can_store:
                # Remove items until we can store the new value.
                while_pred = self.used_capacity + size > self.capacity_storage
                while while_pred:
                    oldest_key, (old_shm_name, old_size) = self.__cache.popitem(last=False)
                    try:
                        old_shm = shared_memory.SharedMemory(name=old_shm_name)
                        old_shm.close()
                        old_shm.unlink()
                    except FileNotFoundError:
                        pass
                    self.used_capacity -= old_size

            # Create new shared memory block.
            shm = shared_memory.SharedMemory(name=key,create=True, size=size)
            shm.buf[:size] = value
            self.__cache[key] = (shm.name, size)
            self.used_capacity += size
            # Close our local reference; other processes can open it by name.
            shm.close()
            log.info({
                "event":"PUT",
                "key":key,
                "size":size,
                "response_time":T.time() -t1
            })
            return 0
        except Exception as e:
            log.error({
                "event":"PUT.FAILED",
                "error":str(e), 
            })
            # print(e)
            return -1

    def get(self, key: str):
        """
        Retrieve the value for the given key as a SharedMemoryData object.
        First, it checks the local metadata dictionary.
        If the key is missing there but a shared memory block with the
        sanitized key exists in /dev/shm, it reconstructs the metadata.
        The caller is responsible for calling close() on the returned object.
        """
        try:
            t1 = T.time()
            # Try to get the metadata from the local dictionary.
            entry = self.__cache.get(key)
            candidate = LRUSharedMemoryCache._sanitize_key(key)
            exists = LRUSharedMemoryCache.shared_memory_exists(candidate)
            if entry is None:
                # If not in metadata, derive the candidate shared memory name.
                if exists:
                    # Get the size from the file in /dev/shm.
                    shm_path = f"/dev/shm/{candidate.lstrip('/')}"
                    st = os.stat(shm_path)
                    size = st.st_size
                    # Open the shared memory block.
                    shm = shared_memory.SharedMemory(name=candidate)
                    # Update local metadata.
                    self.__cache[key] = (candidate, size)
                    log.debug({
                        "event":"GET",
                        "key":candidate,
                        "exists_local":exists, 
                        "response_time":T.time() - t1
                    })
                    return Some(SharedMemoryData(shm, size))
                else:
                    return NONE
            else:
                shm_name, size = entry
                if not LRUSharedMemoryCache.shared_memory_exists(shm_name):
                    # If metadata exists but the shared memory block is gone,
                    # try to re-create it if possible (or return NONE).
                    return NONE
                shm = shared_memory.SharedMemory(name=shm_name)
                result = SharedMemoryData(shm, size)
                self.__cache.move_to_end(key)
                log.debug({
                    "event":"GET",
                    "key":candidate,
                    "exists_local":exists
                })
                return Some(result)
        except FileNotFoundError:
            return NONE

    def remove(self, key: str):
        t1 = T.time()
        entry = self.__cache.pop(key, None)
        if entry:
            shm_name, _ = entry
            try:
                shm = shared_memory.SharedMemory(name=shm_name)
                shm.close()
                shm.unlink()
                log.debug({"event":"REMOVE", "key":key,"response_time":T.time() - t1})
            except FileNotFoundError:
                pass

    def clear(self):
        keys = list(self.__cache.keys())
        for key in keys:
            self.remove(key)

    def get_total_storage_capacity(self) -> int:
        return self.capacity_storage

    def get_used_storage_capacity(self) -> int:
        total = 0
        for _, (_, size) in self.__cache.items():
            total += size
        return total

    def get_uf(self) -> float:
        return 1 - ((self.get_total_storage_capacity() - self.get_used_storage_capacity()) / self.get_total_storage_capacity())
    def __len__(self):
        return len(self.__cache)
    def __del__(self):
        for key,(name, size) in self.__cache.items():
            candidate = LRUSharedMemoryCache._sanitize_key(key)
            exists = LRUSharedMemoryCache.shared_memory_exists(candidate)
            if exists:
                # Get the size from the file in /dev/shm.
                shm_path = f"/dev/shm/{candidate.lstrip('/')}"
                shm = shared_memory.SharedMemory(name=candidate)
                shm.close()
                shm.unlink()
                log.debug({
                    "event":"DESTROYED.SHM",
                    "key":key,
                    "name":name,
                    "size":size
                })


class LRUCache(CacheX):
    """LRU (Least Recently Used) Cache implementation using OrderedDict."""

    def __init__(self, capacity: int, capacity_storage:int):
        self.capacity         = capacity
        self.capacity_storage = capacity_storage
        self.used_capacity    = 0
        self.cache            = OrderedDict()  # Maintains insertion order

    def get_keys(self):
        return list(self.cache.keys())
    def get(self, key: str) ->Option[MemoryData]:
        if key in self.cache:
            self.cache.move_to_end(key)  # Mark as recently used
            return Some(MemoryData(memoryview(self.cache[key])))
        return NONE  # Key not found

    def put(self, key: str, value: bytes)->int:
        try:
            size                  = len(value)
            current_used_capacity = self.used_capacity + size
            can_store             = current_used_capacity <= self.capacity_storage
            # print("CURRENT_CAP", current_used_capacity)
            # print("CAN_STORE", can_store)
            if key in self.cache:
                self.cache.move_to_end(key)  # Mark as recently used
            # elif len(self.cache) >= self.capacity :
            elif not can_store:
                (_,deleted_value) = self.cache.popitem(last=False)  # Remove the least recently used item
                self.used_capacity-=len(deleted_value)

            self.cache[key] = value  # Store new value
            self.used_capacity+= size
            return 0
        except Exception as e:
            print(e)
            return -1

    def remove(self, key: str):
        if key in self.cache:
            element = self. cache[key]
            self.used_capacity-= len(element)
            del self.cache[key]

    def __len__(self) -> int:
        return len(self.cache)

    def clear(self):
        self.cache.clear()
        self.used_capacity= 0
    def get_total_storage_capacity(self):
        return self.capacity_storage
    def get_used_storage_capacity(self):
        return self.used_capacity

    def get_uf(self):
        return 1- ((self.get_total_storage_capacity() - self.get_used_storage_capacity())/self.get_total_storage_capacity())


class LFUCache(CacheX):
    """LFU (Least Frequently Used) Cache implementation using a frequency counter and heap."""

    def __init__(self, capacity: int,capacity_storage:int):
        self.capacity = capacity
        self.capacity_storage = capacity_storage
        self.used_capacity    = 0
        self.cache = {}  # Key -> Value (bytes)
        self.freq_counter = Counter()  # Key -> Frequency
        self.freq_heap = []  # Min-heap to track least frequently used keys

    def get_keys(self):
        return list(self.cache.keys())
    def get(self, key: str) -> Option[memoryview]:
        if key in self.cache:
            self.freq_counter[key] += 1
            heapq.heappush(self.freq_heap, (self.freq_counter[key], key))
            return Some(MemoryData(memoryview(self.cache[key])))
        return NONE

    def put(self, key: str, value: bytes) -> int:
        try:
            size                  = len(value)
            current_used_capacity = self.used_capacity + size
            can_store             = current_used_capacity <= self.capacity_storage
            if key in self.cache:
                self.cache[key] = value
                self.freq_counter[key] += 1
            else:
                # if len(self.cache) >= self.capacity:
                if not can_store:
                    # Remove the least frequently used item
                    while self.freq_heap:
                        freq, least_used_key = heapq.heappop(self.freq_heap)
                        if self.freq_counter[least_used_key] == freq:
                            del self.cache[least_used_key]
                            del self.freq_counter[least_used_key]
                            break
                self.cache[key] = value
                self.freq_counter[key] = 1
            heapq.heappush(self.freq_heap, (self.freq_counter[key], key))
            return 0
        except Exception as e:
            return -1

    def remove(self, key: str):
        if key in self.cache:
            element = self.cache[key]
            self.used_capacity-= len(element)
            del self.cache[key]
            del self.freq_counter[key]

    def __len__(self) -> int:
        return len(self.cache)

    def clear(self):
        self.cache.clear()
        self.freq_counter.clear()
        self.freq_heap.clear()
        self.used_capacity = 0

    def get_total_storage_capacity(self):
        return self.capacity_storage
    def get_used_storage_capacity(self):
        return self.used_capacity

    def get_uf(self):
        return 1- ((self.get_total_storage_capacity() - self.get_used_storage_capacity())/self.get_total_storage_capacity())

class NoCache(CacheX):
    def get_keys(self):
        return super().get_keys()
    def get_total_storage_capacity(self):
        return super().get_total_storage_capacity()
    def get_used_storage_capacity(self):
        return super().get_used_storage_capacity()
    def get_uf(self):
        return super().get_uf()
    
    def get(self, key):
        return NONE
    def put(self, key, value):
        return None
    def remove(self, key):
        return super().remove(key)
    def clear(self):
        return super().clear()
    def __len__(self):
        return 0
