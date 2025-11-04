import pytest
from fastapi.testclient import TestClient
from mictlanxrouter.server import app


@pytest.fixture(scope="session")
def client() -> TestClient:
    """Test client compartido para todas las pruebas del Router."""
    return TestClient(app)
