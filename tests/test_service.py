import pytest
from fastapi.testclient import TestClient
from src.embedding_service import app

client = TestClient(app)

def test_health():
    response = client.get("/embedding/unknown")
    assert response.status_code == 404