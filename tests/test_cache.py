import pytest
from mictlanxrouter.caching import CacheFactory
import time as T


def test_lru_shared_memory():
    cache = CacheFactory.create(
        eviction_policy="LRU_SM",
        capacity=1000,
        capacity_storage=1000
    )
    key = 'x'
    res = cache.put(key=key, value=b"HOLAAAAAAAAAAAAAAAAAA")
    print("PUT_RESULT",res)
    res = cache.get(key).unwrap()
    print("GERT_RESULT", res.data.tobytes())
    T.sleep(30)
    cache.remove(key)
    res = cache.get(key)
    print("GERT_RESULT", res)



