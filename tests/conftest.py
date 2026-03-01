import pytest
from fastapi.testclient import TestClient
from mictlanxrouter.server import app


@pytest.fixture(scope="session")
def client() -> TestClient:
    return TestClient(app)
