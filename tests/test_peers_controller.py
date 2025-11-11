from fastapi.testclient import TestClient
from mictlanxrouter.server import app


client = TestClient(app)


def _build_peer_payload_dict(
    peer_id: str = "pytest-peer-1",
    hostname: str = "localhost",
    port: int = 6000,
    protocol: str = "http",
):
    """
    Helper that builds a valid dictionary representing a PeerPayload.
    """
    return {
        "peer_id": peer_id,
        "hostname": hostname,
        "port": port,
        "protocol": protocol,
    }


# ==========================================================
# /api/v4/peers/stats
# ==========================================================

def test_get_peers_stats_ok():
    """
    Happy path:
    GET /api/v4/peers/stats should return HTTP 200
    and a dictionary containing statistics per peer.
    """
    response = client.get("/api/v4/peers/stats")

    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, dict)


def test_get_peers_stats_with_range_params():
    """
    Happy path with pagination parameters:
    The endpoint should accept `start` and `end` query params.
    """
    response = client.get("/api/v4/peers/stats", params={"start": 0, "end": 10})

    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, dict)


def test_get_peers_stats_invalid_method():
    """
    Common error case:
    Sending POST to /api/v4/peers/stats should return 405 Method Not Allowed.
    """
    response = client.post("/api/v4/peers/stats")
    assert response.status_code in (404, 405)


def test_get_peers_ok():
    """
    Happy path:
    GET /api/v4/peers should return HTTP 200 and include
    both 'available' and 'unavailable' lists of peers.
    """
    response = client.get("/api/v4/peers")

    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, dict)
    assert "available" in body
    assert "unavailable" in body


def test_add_single_peer_happy_path():
    """
    Happy path:
    POST /api/v4/peers should add a single peer and return 204 No Content.
    """
    payload = _build_peer_payload_dict(peer_id="pytest-peer-add-1")

    response = client.post(
        "/api/v4/peers",
        json=payload,
    )

    assert response.status_code == 204
    assert response.content in (b"", None)


def test_add_single_peer_validation_error():
    """
    Validation case:
    If a required field (e.g., peer_id) is missing,
    FastAPI should return 422 Unprocessable Entity.
    """
    payload = _build_peer_payload_dict(peer_id="pytest-peer-invalid")
    payload.pop("peer_id")  # remove required field

    response = client.post("/api/v4/peers", json=payload)

    assert response.status_code == 422


def test_add_multiple_peers_happy_path():
    """
    Happy path:
    POST /api/v4/xpeers should add a list of peers and return 204 No Content.
    """
    payload = [
        _build_peer_payload_dict(peer_id="pytest-xpeer-1"),
        _build_peer_payload_dict(peer_id="pytest-xpeer-2"),
    ]

    response = client.post("/api/v4/xpeers", json=payload)

    assert response.status_code == 204
    assert response.content in (b"", None)


def test_add_multiple_peers_empty_list():
    """
    Edge case:
    Sending an empty list to /api/v4/xpeers.
    Depending on SPM implementation, it may return 204 (no-op)
    or an error. For now, assume a no-op 204 or similar.
    """
    response = client.post("/api/v4/xpeers", json=[])

    assert response.status_code in (204, 400, 500)
    


def test_delete_single_peer_happy_path():
    """
    Happy path:
    DELETE /api/v4/peers/{peer_id} should return 204 No Content.
    Whether the peer exists or not, the controller attempts to remove it
    and responds 204 if no internal error occurs.
    """
    peer_id = "pytest-peer-delete-1"

    response = client.delete(f"/api/v4/peers/{peer_id}")

    assert response.status_code == 204
    assert response.content in (b"", None)


def test_delete_multiple_peers_happy_path():
    """
    Happy path:
    DELETE /api/v4/xpeers with query parameters representing a list of peers.
    Parameter 'peers' is defined as List[str].
    """
    params = [("peers", "pytest-xpeer-del-1"), ("peers", "pytest-xpeer-del-2")]

    response = client.delete("/api/v4/xpeers", params=params)

    assert response.status_code == 204
    assert response.content in (b"", None)


def test_delete_multiple_peers_empty():
    """
    Edge case:
    DELETE /api/v4/xpeers without passing any peers (empty list).
    The controller defines `peers: List[str] = []`, so if no params are sent,
    peers will default to an empty list.
    """
    response = client.delete("/api/v4/xpeers")

    assert response.status_code in (204, 400, 500)


def test_unknown_route_returns_404():
    """
    Generic error:
    Accessing a non-existent route should return 404 Not Found.
    """
    response = client.get("/api/v4/this-route-does-not-exist")
    assert response.status_code == 404
