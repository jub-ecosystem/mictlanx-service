from fastapi.testclient import TestClient
from mictlanxrouter.server import app
from mictlanxrouter.dto import MetadataDTO
from mictlanx.interfaces import PutMetadataResponse
from xolo.utils.utils import Utils as XoloUtils
import io
client = TestClient(app)

def test_stats():
    response = client.get("/api/v4/peers/stats")
                            
    assert response.status_code == 200

def test_put():
    bucket_id = "bx"
    ball_id   = "b2"
    key       = "k2"
    data      = b"Hello from Pytest"
                            
    checksum  = XoloUtils.sha256(value=data)
    content_type       = "application/octet-stream"


    json =MetadataDTO(
        bucket_id          = bucket_id,
        ball_id            = ball_id,
        key                = key,
        checksum           = checksum,
        content_type       = content_type,
        is_disabled        = False,
        producer_id        = "mictlanx",
        replication_factor = 1,
        size               = len(data),
        tags               = {"pytest-put-metadata":"ok"}

    )
    # First Delete
    response = client.delete(f"/api/v4/buckets/{bucket_id}/bid/{ball_id}")
    assert response.status_code == 200
    # Create Metadata
    response              = client.post(f"/api/v4/buckets/{bucket_id}/metadata",json=json.model_dump())
    assert response.status_code ==200
    put_metadata_response = PutMetadataResponse.model_validate(response.json())
    # Upload file
    files = {
        "data":("test.bin", io.BytesIO(data), content_type)
    }
    for task_id,peer_id in zip(put_metadata_response.tasks_ids,put_metadata_response.replicas):
        print("PUT_DATA", task_id)
        response = client.post(
            f"/api/v4/buckets/data/{task_id}",
            files=files,
            headers={"Peer-Id":peer_id}
        
        )
        print(response)
                            
        assert response.status_code ==201