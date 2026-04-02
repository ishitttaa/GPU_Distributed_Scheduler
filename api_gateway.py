"""
EduGrid - API Gateway
======================
This wraps master.py into a proper REST API so the
React/Streamlit frontend can talk to it via HTTP.

Run after workers are up:
  python api_gateway.py

Endpoints:
  GET  /workers         → list all workers and their status
  POST /run             → submit a task
  GET  /benchmark       → compare distributed vs single machine
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Any
import random
import time
import requests

from master import distribute_task, get_available_workers, WORKERS

app = FastAPI(title="EduGrid API", version="1.0")

# Allow the React frontend (running on port 3000) to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Request schema ─────────────────────────────────────────────────────────
class TaskRequest(BaseModel):
    data: List[Any]
    task: str = "sort"    # sort | sum | square | filter_even


# ── GET /workers ───────────────────────────────────────────────────────────
@app.get("/workers")
def list_workers():
    """Return live status of all registered workers."""
    results = []
    for url in WORKERS:
        try:
            r = requests.get(f"{url}/status", timeout=2)
            results.append(r.json())
        except Exception:
            port = url.split(":")[-1]
            results.append({
                "node":   f"Worker-{port}",
                "port":   int(port),
                "status": "offline",
                "cpu":    0,
                "ram":    0,
            })
    return {"workers": results}


# ── POST /run ──────────────────────────────────────────────────────────────
@app.post("/run")
def run_task(req: TaskRequest):
    """Distribute a task across available workers and return the result."""
    result = distribute_task(req.data, task=req.task)
    return result


# ── GET /benchmark ─────────────────────────────────────────────────────────
@app.get("/benchmark")
def benchmark(n: int = 500, task: str = "sort"):
    """
    Compare:
      - Distributed execution (EduGrid)
      - Single-machine execution

    Returns both times so the frontend can plot the speedup.
    """
    data = random.sample(range(1, 10_001), min(n, 5000))

    # -- Distributed --
    dist_start  = time.time()
    dist_result = distribute_task(data, task=task)
    dist_time   = round(time.time() - dist_start, 4)

    # -- Single machine --
    single_start = time.time()
    if task == "sort":
        _ = sorted(data)
    elif task == "sum":
        _ = sum(data)
    elif task == "square":
        _ = [x**2 for x in data]
    elif task == "filter_even":
        _ = [x for x in data if x % 2 == 0]
    single_time = round(time.time() - single_start, 6)

    speedup = round(single_time / dist_time, 2) if dist_time > 0 else "N/A"

    return {
        "n":             len(data),
        "task":          task,
        "distributed_s": dist_time,
        "single_s":      single_time,
        "speedup":       speedup,
        "workers_used":  dist_result.get("workers_used", 0),
    }


# ── Entry point ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    print("\nEduGrid API Gateway starting on http://localhost:9000")
    print("Docs:  http://localhost:9000/docs\n")
    uvicorn.run("api_gateway:app", host="0.0.0.0", port=9000, reload=True)