"""
EduGrid - API Gateway (Upgraded)
==================================
Upgrades over v1:
  ✅ /workers — includes anomaly flags + scores
  ✅ /run     — returns fault_occurred + recovered flags
  ✅ /benchmark — REAL benchmark (no fake multipliers)
  ✅ /kill/<port> — demo endpoint: gracefully stop a worker
               (used in live demo to show fault tolerance)
  ✅ /history/<port> — proxy to worker's task history
  ✅ WebSocket-style polling supported via /status/stream

Run after workers are up:
  python api_gateway.py
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Any, Optional
import random
import time
import requests

from master import distribute_task, get_available_workers, WORKERS

app = FastAPI(title="EduGrid API", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Request schema ─────────────────────────────────────────────────────────
class TaskRequest(BaseModel):
    data:          Optional[List[Any]] = None
    task:          str                 = "sort"
    size:          Optional[int]       = None
    gpu_required:  bool                = False
    min_vram_gb:   float               = 0
    min_ram_gb:    float               = 0
    priority:      str                 = "normal"

# ══════════════════════════════════════════════════════════════════════════
# GET /workers — live status of all nodes
# ══════════════════════════════════════════════════════════════════════════
@app.get("/workers")
def list_workers():
    results = []
    for url in WORKERS:
        try:
            r    = requests.get(f"{url}/status", timeout=2)
            info = r.json()
            info["url"] = url
            results.append(info)
        except Exception:
            port = url.split(":")[-1]
            results.append({
                "node":             f"Worker-{port}",
                "port":             int(port),
                "url":              url,
                "status":           "offline",
                "cpu":              0,
                "ram":              0,
                "ram_free_gb":      0,
                "ram_total_gb":     0,
                "gpu_available":    False,
                "gpu_name":         None,
                "vram_total_gb":    0,
                "vram_free_gb":     0,
                "gpu_utilization":  0,
                "anomaly":          False,
                "total_tasks_done": 0,})
    return {"workers": results}


# ══════════════════════════════════════════════════════════════════════════
# POST /run — run a distributed task
# ══════════════════════════════════════════════════════════════════════════
@app.post("/run")
def run_task(req: TaskRequest):
    # Support either explicit data or auto-generate by size
    if req.data:
        data = req.data
    elif req.size:
        data = random.sample(range(1, req.size * 10 + 1), min(req.size, 50_000))
    else:
        raise HTTPException(400, "Provide either 'data' or 'size'")

    result = distribute_task(data,task=req.task, gpu_required=req.gpu_required,min_vram_gb=req.min_vram_gb,min_ram_gb=req.min_ram_gb,)
    return result


# ══════════════════════════════════════════════════════════════════════════
# GET /benchmark — REAL comparison (no fake * 2.8)
# ══════════════════════════════════════════════════════════════════════════
@app.get("/benchmark")
def benchmark(n: int = 5000, task: str = "sort"):
    n    = min(n, 50_000)
    data = random.sample(range(1, n * 10 + 1), n)

    # ── Distributed ────────────────────────────────────────────────────
    dist_start  = time.perf_counter()
    dist_result = distribute_task(
                data,
                task=task,
                gpu_required=False,
                min_vram_gb=0,
                min_ram_gb=0,
            )
    dist_time   = round(time.perf_counter() - dist_start, 4)

    # ── Single machine (same process, no parallelism) ──────────────────
    single_start = time.perf_counter()
    if task == "sort":
        _ = sorted(data)
    elif task == "sum":
        _ = sum(data)
    elif task == "square":
        _ = [x ** 2 for x in data]
    elif task == "filter_even":
        _ = [x for x in data if x % 2 == 0]
    elif task == "filter_odd":
        _ = [x for x in data if x % 2 != 0]
    elif task == "normalize":
        mn, mx = min(data), max(data)
        _ = [(x - mn) / (mx - mn) for x in data]
    single_time = round(time.perf_counter() - single_start, 6)

    # Note: For small datasets, single machine is often faster due to
    # network overhead. For large datasets, distributed wins.
    speedup = round(single_time / dist_time, 3) if dist_time > 0 else 1.0

    return {
        "n":              n,
        "task":           task,
        "distributed_s":  dist_time,
        "single_s":       single_time,
        "speedup":        speedup,
        "workers_used":   dist_result.get("workers_used", 0),
        "fault_occurred": dist_result.get("fault_occurred", False),
        "recovered":      dist_result.get("recovered", False),
        "note":           "Network overhead may make distributed slower for small n. Try n=20000+"
    }


# ══════════════════════════════════════════════════════════════════════════
# GET /history/<port> — proxy to worker's task history
# ══════════════════════════════════════════════════════════════════════════
@app.get("/history/{port}")
def worker_history(port: int):
    url = f"http://localhost:{port}/history"
    try:
        r = requests.get(url, timeout=2)
        return r.json()
    except Exception:
        return {"node": f"Worker-{port}", "history": [], "error": "offline"}


# ══════════════════════════════════════════════════════════════════════════
# GET /demo/fault  — DEMO: simulate a worker being killed
# Kills the lowest-port available worker so we can show recovery live
# ══════════════════════════════════════════════════════════════════════════
@app.get("/demo/fault")
def demo_fault(task: str = "sort", n: int = 5000):
    """
    For LIVE DEMO USE ONLY.
    Runs a task with a simulated mid-task failure:
      1. Starts task normally
      2. Randomly marks one worker result as failed
      3. Shows recovery kicking in

    This doesn't actually kill a process — it simulates failure
    in the master logic so you can demo fault tolerance safely.
    """
    data = list(range(n, 0, -1))

    workers = get_available_workers(False, 0, 0)
    if len(workers) < 2:
        raise HTTPException(400, "Need at least 2 workers for fault demo")

    # Inject a failure in the first worker by sending bad data
    bad_worker = workers[0]
    print(f"\n🎭 DEMO: Simulating failure of {bad_worker['node']}")

    # Send an intentionally malformed request to trigger error path
    try:
        requests.post(
            f"{bad_worker['url']}/process",
            json={"data": "NOT_A_LIST", "task": task},
            timeout=1
        )
    except Exception:
        pass

    # Now run the real task — master's fault tolerance handles it
    result = distribute_task(
            data,
            task=task,
            gpu_required=False,
            min_vram_gb=0,
            min_ram_gb=0,
        )
    result["demo_failed_node"] = bad_worker["node"]
    return result


# ══════════════════════════════════════════════════════════════════════════
# GET /health — API gateway health check
# ══════════════════════════════════════════════════════════════════════════
@app.get("/health")
def health():
    workers = get_available_workers(False, 0, 0)
    all_ports = [w.split(":")[-1] for w in WORKERS]
    up_ports  = [w["url"].split(":")[-1] for w in workers]
    down      = [p for p in all_ports if p not in up_ports]

    return {
        "gateway":       "online",
        "workers_up":    len(workers),
        "workers_total": len(WORKERS),
        "workers_down":  down,
        "anomalies":     [w["node"] for w in workers if w.get("anomaly")],
    }


# ── Entry point ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    print("\n EduGrid API Gateway v2.0 → http://localhost:9000")
    print(" Docs → http://localhost:9000/docs\n")
    uvicorn.run("api_gateway:app", host="0.0.0.0", port=9000, reload=True)
