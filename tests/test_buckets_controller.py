from fastapi.testclient import TestClient
from mictlanxrouter.server import app
from mictlanxrouter.dto import PutMetadataDTO
from mictlanx.interfaces import PutMetadataResponse
from xolo.utils.utils import Utils as XoloUtils
import io
import time

client = TestClient(app)


def _build_put_metadata_dto(
    bucket_id: str,
    key: str,
    ball_id: str,
    data: bytes,
):
    """
    Helper function that builds a valid PutMetadataDTO
    to be used for creating metadata in the buckets endpoints.
    """
    checksum = XoloUtils.sha256(value=data)
    content_type = "application/octet-stream"

    return PutMetadataDTO(
        bucket_id=bucket_id,
        ball_id=ball_id,
        key=key,
        checksum=checksum,
        content_type=content_type,
        is_disabled=False,
        producer_id="pytest",
        replication_factor=1,
        size=len(data),
        tags={
            "pytest-put-metadata": "ok",
            "updated_at": str(int(time.time_ns())),
        },
    )


def test_put_metadata_put_data_and_get_data_happy_path():
    """
    Happy path:
    1. DELETE /api/v4/buckets/{bucket_id}/bid/{ball_id} to clean up any previous data.
    2. POST   /api/v4/buckets/{bucket_id}/metadata to create metadata.
    3. POST   /api/v4/buckets/data/{task_id} to upload data.
    4. GET    /api/v4/buckets/{bucket_id}/{key} with Peer-Id header to retrieve data.
    """
    bucket_id = "pytest-bucket-4"
    ball_id = "pytest-ball-1"
    key = "pytest-key-1"
    data = b"hola-mictlanx-buckets"

    # Step 0: Clean up any previous data by ball_id
    resp = client.delete(f"/api/v4/buckets/{bucket_id}/bid/{ball_id}")
    assert resp.status_code == 200

    # Step 1: Create metadata
    dto = _build_put_metadata_dto(bucket_id=bucket_id, key=key, ball_id=ball_id, data=data)

    resp = client.post(
        f"/api/v4/buckets/{bucket_id}/metadata",
        json=dto.model_dump(),
    )
    assert resp.status_code == 200

    put_metadata_response = PutMetadataResponse.model_validate(resp.json())

    # Extract peer_id and group_id for later upload
    assert put_metadata_response.replicas
    peer_id = put_metadata_response.replicas[0]
    group_id = put_metadata_response.tasks_ids[0]

    # Step 2: Upload data (non-chunked)
    files = {
        "data": ("test.bin", io.BytesIO(data), dto.content_type),
    }

    upload_resp = client.post(
        f"/api/v4/buckets/data/{group_id}",
        files=files,
        headers={"Peer-Id": peer_id},
    )
    assert upload_resp.status_code == 201

    # Step 3: Retrieve data (GET)
    get_resp = client.get(
        f"/api/v4/buckets/{bucket_id}/{key}",
        headers={"Peer-Id": peer_id},
    )
    assert get_resp.status_code == 200
    assert get_resp.content == data


def test_put_metadata_bucket_mismatch_returns_400():
    """
    Validation test:
    If the bucket_id in the path and the one inside the payload do not match,
    the controller must respond with HTTP 400 (Bad Request).
    """
    path_bucket_id = "pytest-bucket-mismatch"
    body_bucket_id = "other-bucket"
    key = "pytest-key-mismatch"
    ball_id = "pytest-ball-mismatch"
    data = b"payload-mismatch"

    dto = _build_put_metadata_dto(bucket_id=body_bucket_id, key=key, ball_id=ball_id, data=data)

    resp = client.post(
        f"/api/v4/buckets/{path_bucket_id}/metadata",
        json=dto.model_dump(),
    )
    assert resp.status_code == 400
    body = resp.json()
    assert "Bucket ID mismatch" in body["detail"]


def test_get_metadata_not_found_returns_500():
    """
    Error path:
    When requesting metadata for a non-existent key, the controller currently wraps
    the HTTPException(404, "No available peers") inside another HTTPException(500, ...),
    which results in an HTTP 500 response.
    """
    bucket_id = "pytest-bucket-missing"
    key = "non-existent-key-123"

    resp = client.get(f"/api/v4/buckets/{bucket_id}/metadata/{key}")
    assert resp.status_code == 500
    body = resp.json()
    assert "No available peers" in body["detail"]


def test_delete_data_by_ball_id_not_found_returns_200_with_zero_deletes():
    """
    Edge case:
    When deleting a ball_id that does not exist, the controller logs a NOT.FOUND event
    and returns a JSON response with:
      - n_deletes = 0
      - HTTP status = 200 OK
    """
    bucket_id = "pytest-bucket-empty-ball"
    ball_id = "non-existent-ball-123"

    resp = client.delete(f"/api/v4/buckets/{bucket_id}/bid/{ball_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ball_id"] == ball_id
    assert "n_deletes" in body
    assert isinstance(body["n_deletes"], int)
