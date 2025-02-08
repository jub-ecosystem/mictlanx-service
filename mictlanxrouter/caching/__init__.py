from abc import ABC, abstractmethod
from typing import Dict,List
from collections import OrderedDict, Counter
import heapq
from option import Option, Some, NONE

class CacheX(ABC):
    """Abstract Base Class for a simple key-value cache (Key: str -> Value: bytes/memoryview)."""
    @abstractmethod
    def get_keys(self)->List[str]:
        pass
    @abstractmethod
    def get(self, key: str) -> Option[memoryview]:
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
        return LRUCache(capacity=capacity)


class LRUCache(CacheX):
    """LRU (Least Recently Used) Cache implementation using OrderedDict."""

    def __init__(self, capacity: int, capacity_storage:int):
        self.capacity         = capacity
        self.capacity_storage = capacity_storage
        self.used_capacity    = 0
        self.cache            = OrderedDict()  # Maintains insertion order

    def get_keys(self):
        return list(self.cache.keys())
    def get(self, key: str) ->Option[memoryview]:
        if key in self.cache:
            self.cache.move_to_end(key)  # Mark as recently used
            return Some(memoryview(self.cache[key]))
        return NONE  # Key not found

    def put(self, key: str, value: bytes)->int:
        try:
            size                  = len(value)
            current_used_capacity = self.used_capacity + size
            can_store             = current_used_capacity <= self.capacity_storage
            print("CURRENT_CAP", current_used_capacity)
            print("CAN_STORE", can_store)
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
            return Some(memoryview(self.cache[key]))
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

# Example Usage
if __name__ == "__main__":
    print("Testing LRU Cache:")
    lru = LRUCache(capacity=3)
    lru.put("a", b"data1")
    lru.put("b", b"data2")
    lru.put("c", b"data3")
    print(lru.get("a"))  # Should return data1
    lru.put("d", b"data4")  # Evicts "b"
    print(lru.get("b"))  # Should return None (evicted)

    print("\nTesting LFU Cache:")
    lfu = LFUCache(capacity=3)
    lfu.put("x", b"dataX")
    lfu.put("y", b"dataY")
    lfu.put("z", b"dataZ")
    lfu.get("x")  # Increase frequency of "x"
    lfu.put("w", b"dataW")  # Evicts the least frequently used
    print(lfu.get("y"))  # Should return None (evicted)
