import pytest
from mictlanxrouter.caching import CacheFactory
import time as T

@pytest.fixture(scope="session")
def cache():
    x = CacheFactory.create(
        eviction_policy="LRU_SM",
        capacity=1000,
        capacity_storage=1000
    )
    return x


def test_lru_shared_memory_put(cache):
    key = 'x'
    res = cache.put(key=key, value=b"HOLAAAAAAAAAAAAAAAAAA")
    assert res == 0
    T.sleep(5)

def test_lru_shared_memory_get(cache):
    res = cache.get(key="x")
    print("RES",res)
    assert res.is_some

def test_lru_shared_memory_put_duplicated(cache):
    key ="x"
    res = cache.put(key=key, value=b"ADIOS")
    # print("RES",res)
    # assert res.is_some

def test_lru_shared_memory_get2(cache):
    res = cache.get(key="x")
    print("RES",res)
    assert res.is_some
    data = res.unwrap()
    print("DATA",data.data.tobytes())
















