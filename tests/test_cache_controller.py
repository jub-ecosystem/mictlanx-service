

from fastapi.testclient import TestClient
from mictlanxrouter.server import app
from mictlanxrouter.dto import PutMetadataDTO
from mictlanx.interfaces import PutMetadataResponse
from xolo.utils.utils import Utils as XoloUtils
import io

client = TestClient(app)


def _put_object_and_fill_cache(bucket_id: str, ball_id: str, key: str, data: bytes) -> str:
    """
    Helper flow:
      1. DELETE by ball_id (cleanup).
      2. POST metadata.
      3. POST data.
      4. GET data (so the object is stored in the cache).

    Returns the expected cache key in the router: "{bucket_id}@{key}".
    """
    # 0) Clean up by ball_id
    client.delete(f"/api/v4/buckets/{bucket_id}/bid/{ball_id}")

    checksum = XoloUtils.sha256(value=data)
    content_type = "application/octet-stream"

    metadata = PutMetadataDTO(
        bucket_id=bucket_id,
        ball_id=ball_id,
        key=key,
        checksum=checksum,
        content_type=content_type,
        is_disabled=False,
        producer_id="pytest",
        replication_factor=1,
        size=len(data),
        tags={},
    )

    # 1) Create metadata
    resp = client.post(
        f"/api/v4/buckets/{bucket_id}/metadata",
        json=metadata.model_dump(),
    )
    assert resp.status_code == 200
    put_metadata_response = PutMetadataResponse.model_validate(resp.json())

    assert put_metadata_response.replicas
    peer_id = put_metadata_response.replicas[0]
    task_group_id = put_metadata_response.tasks_ids[0]

    # 2) Upload object data
    files = {
        "data": ("test.bin", io.BytesIO(data), content_type),
    }
    upload_resp = client.post(
        f"/api/v4/buckets/data/{task_group_id}",
        files=files,
        headers={"Peer-Id": peer_id},
    )
    assert upload_resp.status_code == 201

    # 3) GET data so it gets cached in the router
    get_resp = client.get(
        f"/api/v4/buckets/{bucket_id}/{key}",
        headers={"Peer-Id": peer_id},
    )
    assert get_resp.status_code == 200
    assert get_resp.content == data

    return f"{bucket_id}@{key}"


def test_cache_reset_leaves_cache_empty():
    """
    When calling DELETE /api/v4/cache/reset:
      - The cache should be cleared.
      - GET /api/v4/cache should return an empty list of keys.
      - GET /api/v4/cache/stats should be consistent:
         * total, used, available, uf present.
         * available = total - used.
         * uf within [0, 1] or total == 0.
    """
    # Reset cache
    resp = client.delete("/api/v4/cache/reset")
    assert resp.status_code == 204

    # Cache must be empty
    resp = client.get("/api/v4/cache")
    assert resp.status_code == 200
    body = resp.json()
    assert "keys" in body
    assert isinstance(body["keys"], list)
    assert body["keys"] == []

    # Stats must be consistent
    stats_resp = client.get("/api/v4/cache/stats")
    assert stats_resp.status_code == 200
    stats = stats_resp.json()

    for field in ("total", "used", "available", "uf"):
        assert field in stats

    assert isinstance(stats["total"], int)
    assert isinstance(stats["used"], int)
    assert isinstance(stats["available"], int)
    assert stats["total"] >= 0
    assert stats["used"] >= 0
    assert stats["available"] == stats["total"] - stats["used"]

    # Utilization factor must be within range, unless there is no capacity
    assert 0.0 <= stats["uf"] <= 1.0 or stats["total"] == 0


def test_cache_keys_and_stats_after_get_data():
    """
    After putting an object and performing a GET on it:
      - The cache endpoint should list the corresponding cache key.
      - The stats endpoint should report used > 0 and
        available = total - used.
    """
    # Ensure cache is initially empty
    client.delete("/api/v4/cache/reset")

    bucket_id = "pytest-cache-bucket-2"
    ball_id = "pytest-cache-ball-1"
    key = "pytest-cache-key-1"
    data = b"hola-cache-controller"

    expected_cache_key = _put_object_and_fill_cache(
        bucket_id=bucket_id,
        ball_id=ball_id,
        key=key,
        data=data,
    )

    # Cache /keys endpoint must contain our cache key
    resp = client.get("/api/v4/cache")
    assert resp.status_code == 200
    body = resp.json()
    assert "keys" in body
    keys = body["keys"]
    assert isinstance(keys, list)
    assert expected_cache_key in keys

    # Stats must reflect that something is stored
    stats_resp = client.get("/api/v4/cache/stats")
    assert stats_resp.status_code == 200
    stats = stats_resp.json()
    assert stats["used"] > 0
    assert stats["available"] == stats["total"] - stats["used"]


def test_cache_reset_after_being_populated():
    """
    When the cache has been populated by reading data:
      - The cache endpoint should list at least the expected key.
      - After calling DELETE /api/v4/cache/reset, the keys list must be empty.
    """
    bucket_id = "pytest-cache-bucket-3"
    ball_id = "pytest-cache-ball-2"
    key = "pytest-cache-key-2"
    data = b"otro-objeto-para-cache"

    expected_cache_key = _put_object_and_fill_cache(
        bucket_id=bucket_id,
        ball_id=ball_id,
        key=key,
        data=data,
    )

    # Ensure the cache is populated
    resp = client.get("/api/v4/cache")
    assert resp.status_code == 200
    keys = resp.json()["keys"]
    assert expected_cache_key in keys

    # Reset cache
    resp = client.delete("/api/v4/cache/reset")
    assert resp.status_code == 204

    # Cache must be empty again
    resp = client.get("/api/v4/cache")
    assert resp.status_code == 200
    keys = resp.json()["keys"]
    assert keys == []
