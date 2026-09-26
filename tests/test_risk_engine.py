import pytest
from fastapi.testclient import TestClient
from src.api.main import app

client = TestClient(app)

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200

def test_risk_prediction_passable():
    payload = {
        "application_id": "TEST-APP-001",
        "dti": 0.25,
        "ltv": 0.50,
        "p_instances": 0
    }
    response = client.post("/api/v2/predict-risk", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["risk_category"] == "PASSABLE LOW RISK"
    assert data["rc_score"] < 0.45

def test_risk_prediction_critical():
    payload = {
        "application_id": "TEST-APP-002",
        "dti": 0.85,
        "ltv": 0.95,
        "p_instances": 4
    }
    response = client.post("/api/v2/predict-risk", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["risk_category"] == "CRITICAL DEFAULT RISK"
    assert data["rc_score"] >= 0.75
