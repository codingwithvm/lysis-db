from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.v1.processes.router import router

app = FastAPI()
app.include_router(router)

client = TestClient(app)

def test_process_count():
  response = client.get("/api/v1/processes/count")
  assert response.status_code == 200

def test_processes_by_origin():
  response = client.get("/api/v1/processes/by-origin")
  assert response.status_code == 200

def test_processes_by_status():
  response = client.get("/api/v1/processes/by-status")
  assert response.status_code == 200

def test_processes_by_matter():
  response = client.get("/api/v1/processes/by-matter")
  assert response.status_code == 200

def test_processes_by_group():
  response = client.get("/api/v1/processes/by-group")
  assert response.status_code == 200

def test_processes_by_organization():
  response = client.get("/api/v1/processes/by-organization")
  assert response.status_code == 200
