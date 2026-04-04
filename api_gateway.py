"""
EduGrid - API Gateway
=====================
Routes dashboard requests to master + workers.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Any, Optional, Literal
import random
import time
import requests

from master import distribute_task, get_available_workers, WORKERS

app = FastAPI(title="EduGrid API", version="2.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class TaskRequest(BaseModel):
    data: Optional[List[Any]] = None
    task: str = "sort"
    size: Optional[int] = None
    mode: Literal["cpu", "gpu", "hybrid"] = "cpu"
    min_vram_gb: float = 0
    min_ram_gb: float = 0
    priority: str = "normal"


def get_worker_url_by_port(port: int) -> Optional[str]:
    for url in WORKERS:
        try:
            if int(url.split(":")[-1]) == port:
                return url
        except Exception:
            continue
    return None


@app.get("/workers")
def list_workers():
    results = []

    for url in WORKERS:
        try:
            r = requests.get(f"{url}/status", timeout=2)
            info = r.json()
            info["url"] = url
            results.append(info)

        except Exception:
            port = url.split(":")[-1]
            results.append({
                "node": f"Worker-{port}",
                "port": int(port),
                "url": url,
                "status": "offline",
                "cpu": 0,
                "ram": 0,
                "ram_free_gb": 0,
                "ram_total_gb": 0,
                "gpu_available": False,
                "gpu_name": None,
                "vram_total_gb": 0,
                "vram_free_gb": 0,
                "gpu_utilization": 0,
                "anomaly": False,
                "total_tasks_done": 0,
                "cpu_weight": 1.0,
                "gpu_weight": 0.0,
                "node_role": "unknown",
            })

    return {"workers": results}


@app.post("/run")
def run_task(req: TaskRequest):
    if req.data is not None and len(req.data) > 0:
        data = req.data
    elif req.size is not None:
        data = random.sample(range(1, req.size * 10 + 1), min(req.size, 50_000))
    else:
        raise HTTPException(status_code=400, detail="Provide either 'data' or 'size'")

    if req.min_vram_gb < 0 or req.min_ram_gb < 0:
        raise HTTPException(status_code=400, detail="RAM/VRAM requirements cannot be negative")

    return distribute_task(
        data,
        task=req.task,
        mode=req.mode,
        min_vram_gb=req.min_vram_gb,
        min_ram_gb=req.min_ram_gb,
    )


@app.get("/benchmark")
def benchmark(n: int = 5000, task: str = "sort"):
    n = min(max(n, 1), 50_000)
    data = random.sample(range(1, n * 10 + 1), n)

    dist_start = time.perf_counter()
    dist_result = distribute_task(
        data,
        task=task,
        mode="cpu",
        min_vram_gb=0,
        min_ram_gb=0,
    )
    dist_time = round(time.perf_counter() - dist_start, 4)

    single_start = time.perf_counter()

    if task == "sort":
        _ = sorted(data)
    elif task == "sum":
        _ = sum(data)
    elif task == "square":
        _ = [x ** 2 for x in data]
    elif task == "gpu_square":
        _ = [x ** 2 for x in data]
    elif task == "filter_even":
        _ = [x for x in data if x % 2 == 0]
    elif task == "filter_odd":
        _ = [x for x in data if x % 2 != 0]
    elif task == "normalize":
        mn, mx = min(data), max(data)
        rng = (mx - mn) or 1
        _ = [(x - mn) / rng for x in data]
    elif task == "preprocess":
        _ = [int(x) for x in data if isinstance(x, (int, float))]
    else:
        _ = data

    single_time = round(time.perf_counter() - single_start, 4)
    speedup = round(single_time / dist_time, 3) if dist_time > 0 else 1.0

    return {
        "n": n,
        "task": task,
        "distributed_s": dist_time,
        "single_s": single_time,
        "speedup": speedup,
        "workers_used": dist_result.get("workers_used", 0),
        "fault_occurred": dist_result.get("fault_occurred", False),
        "recovered": dist_result.get("recovered", False),
        "note": "Distributed can be slower for small inputs due to network + orchestration overhead. Try bigger n.",
    }


@app.get("/history/{port}")
def worker_history(port: int):
    worker_url = get_worker_url_by_port(port)
    if not worker_url:
        return {
            "node": f"Worker-{port}",
            "history": [],
            "error": "port not found in WORKERS registry",
        }

    try:
        r = requests.get(f"{worker_url}/history", timeout=2)
        return r.json()
    except Exception:
        return {
            "node": f"Worker-{port}",
            "history": [],
            "error": "offline",
        }


@app.get("/demo/fault")
def demo_fault(task: str = "sort", n: int = 5000):
    data = list(range(n, 0, -1))

    workers = get_available_workers(mode="cpu", min_vram_gb=0, min_ram_gb=0)
    if len(workers) < 2:
        raise HTTPException(status_code=400, detail="Need at least 2 workers for fault demo")

    bad_worker = workers[0]
    print(f"\n🎭 DEMO: Simulating failure of {bad_worker['node']}")

    try:
        requests.post(
            f"{bad_worker['url']}/process",
            json={"data": "NOT_A_LIST", "task": task, "mode": "cpu"},
            timeout=1,
        )
    except Exception:
        pass

    result = distribute_task(
        data,
        task=task,
        mode="cpu",
        min_vram_gb=0,
        min_ram_gb=0,
    )
    result["demo_failed_node"] = bad_worker["node"]
    return result


@app.get("/health")
def health():
    workers = get_available_workers(mode="cpu", min_vram_gb=0, min_ram_gb=0)
    all_ports = [w.split(":")[-1] for w in WORKERS]
    up_ports = [w["url"].split(":")[-1] for w in workers]
    down = [p for p in all_ports if p not in up_ports]

    return {
        "gateway": "online",
        "workers_up": len(workers),
        "workers_total": len(WORKERS),
        "workers_down": down,
        "anomalies": [w["node"] for w in workers if w.get("anomaly")],
    }


if __name__ == "__main__":
    import uvicorn

    print("\nEduGrid API Gateway → http://localhost:9000")
    print("Docs → http://localhost:9000/docs\n")

    uvicorn.run("api_gateway:app", host="0.0.0.0", port=9000, reload=True)