# ============================================================
# master.py  —  EduGrid Master Controller
# Written by: Ishita (Team Lead)
# ============================================================
#
# WHAT IS THIS FILE?
#   This is the "brain" of EduGrid.
#   It receives a big task from a student,
#   cuts it into smaller pieces (chunks),
#   sends each chunk to a free worker machine,
#   waits for all results,
#   and combines them into one final answer.
#
# HOW TO RUN THIS FILE:
#   Step 1 — Install libraries (only once):
#       pip install fastapi uvicorn requests
#   Step 2 — Start the server:
#       uvicorn master:app --host 0.0.0.0 --port 8000 --reload
#   Step 3 — Open your browser:
#       http://localhost:8000/docs   ← interactive test page (Swagger UI)
# ============================================================


# ── Imports ─────────────────────────────────────────────────
# These are like "tools" we borrow from other libraries.

from fastapi import FastAPI, HTTPException
# FastAPI  → creates our web server (lets other programs talk to us)
# HTTPException → lets us send error messages with proper HTTP codes

from pydantic import BaseModel
# BaseModel → describes the shape of data we accept (like a form template)

import requests
# requests → lets us send HTTP requests TO the worker servers

import threading
# threading → runs the heartbeat checker in the background
#             (so the server doesn't freeze while waiting)

import time
# time → for sleep(), used in the heartbeat loop

import uuid
# uuid → generates unique IDs like "task-a3f2..."

from typing import List, Dict, Any, Optional
# These are just type hints — they help editors show autocomplete
# and make the code easier to understand. They don't change behaviour.

import logging
# logging → prints helpful messages to the terminal while the server runs

# ── Basic setup ──────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
# This makes log messages look like:  2024-01-01 12:00:00 | some message

app = FastAPI(title="EduGrid Master Node", version="1.0")
# 'app' is our web server object.
# Every API route (URL endpoint) will be attached to this object.


# ── Data Models ──────────────────────────────────────────────
# These classes describe the exact shape of data we accept from callers.
# FastAPI automatically validates incoming data against these models.
# If something is missing or wrong, FastAPI sends an error automatically.

class TaskRequest(BaseModel):
    """
    What a student sends when they submit a task.
    Example JSON:
    {
        "task_type": "sort",
        "data": [5, 2, 8, 1, 9, 3, 7, 4, 6]
    }
    """
    task_type: str          # e.g. "sort", "sum", "matrix_multiply"
    data: List[Any]         # the actual data to process (a list of anything)
    priority: int = 1       # optional: 1=normal, 2=high (default is 1)


class HeartbeatRequest(BaseModel):
    """
    What a worker sends every few seconds to say "I'm alive".
    Example JSON:
    {
        "node_id": "worker-1",
        "status": "free",
        "url": "http://localhost:8001"
    }
    """
    node_id: str            # unique name of the worker, e.g. "worker-1"
    status: str             # "free", "busy", or "offline"
    url: str                # the worker's full address so we can send tasks to it


# ── In-Memory Storage ────────────────────────────────────────
# For a beginner prototype, we store everything in Python dictionaries.
# In a real production system, you'd use a database (like Redis or PostgreSQL).

nodes: Dict[str, Dict] = {}
# Tracks all registered workers.
# Structure: { "worker-1": { "status": "free", "url": "...", "last_seen": 1234567890 } }

tasks: Dict[str, Dict] = {}
# Tracks all submitted tasks and their results.
# Structure: { "task-abc123": { "status": "pending", "result": None, ... } }

nodes_lock = threading.Lock()
# A "lock" prevents two threads from modifying 'nodes' at the same time.
# Without this, you can get corrupted data (race conditions).

tasks_lock = threading.Lock()
# Same idea for 'tasks'.


# ── Helper Functions ─────────────────────────────────────────

def split_task(data: List[Any], num_chunks: int) -> List[List[Any]]:
    """
    Splits a big list into smaller equal chunks.

    Example:
        data = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        num_chunks = 3
        → returns [[1, 2, 3], [4, 5, 6], [7, 8, 9]]

    Each chunk goes to one worker.
    """
    if num_chunks <= 0:
        return [data]  # safety: if no workers, return the whole list as one chunk

    chunk_size = max(1, len(data) // num_chunks)
    # max(1, ...) makes sure chunk_size is at least 1
    # // is integer division: 9 // 3 = 3

    chunks = []
    for i in range(0, len(data), chunk_size):
        chunk = data[i : i + chunk_size]   # slice from i to i+chunk_size
        chunks.append(chunk)

    return chunks


def get_free_nodes() -> List[Dict]:
    """
    Returns a list of workers that are currently FREE.
    Only free workers can accept new tasks.
    """
    with nodes_lock:   # lock so no other thread changes 'nodes' while we read it
        free = [
            node_info
            for node_id, node_info in nodes.items()
            if node_info["status"] == "free"
        ]
    return free


def aggregate_results(partial_results: List[Any], task_type: str) -> Any:

    if task_type == "sort":
        combined = [item for sublist in partial_results for item in sublist]
        return sorted(combined)

    elif task_type == "sum":
        return sum(partial_results)

    elif task_type == "square":
        # bas combine karna hai
        return [item for sublist in partial_results for item in sublist]

    elif task_type == "filter_even":
        # even numbers combine karo
        return [item for sublist in partial_results for item in sublist]

    else:
        if partial_results and isinstance(partial_results[0], list):
            return [item for sublist in partial_results for item in sublist]
        return partial_results


def send_chunk_to_worker(worker_url: str, task_type: str, chunk: List[Any]) -> Any:
    """
    Sends one chunk to one worker and returns the worker's result.

    This function is called from a background thread so it doesn't block
    the main server while waiting for the worker to respond.

    Returns None if the worker is unreachable or returns an error.
    """
    try:
        response = requests.post(
            f"{worker_url}/execute",          # URL of the worker's execution endpoint
            json={"task_type": task_type, "data": chunk},
            timeout=30                        # wait max 30 seconds; then give up
        )
        if response.status_code == 200:
            return response.json().get("result")  # extract the "result" field
        else:
            logging.warning(f"Worker at {worker_url} returned error: {response.status_code}")
            return None

    except requests.exceptions.Timeout:
        logging.error(f"Worker at {worker_url} timed out!")
        return None

    except requests.exceptions.ConnectionError:
        logging.error(f"Could not connect to worker at {worker_url}")
        return None


# ── Background: Heartbeat Monitor ────────────────────────────
# Workers send a heartbeat every 3 seconds.
# If we haven't heard from a worker in 10 seconds, we mark it "offline".
# This runs in a separate background thread so it doesn't block anything.

def heartbeat_monitor():
    """
    Runs forever in the background.
    Every 5 seconds, checks if any worker has gone silent.
    If a worker hasn't pinged us in 10 seconds → mark it "offline".
    """
    while True:
        time.sleep(5)   # check every 5 seconds
        now = time.time()

        with nodes_lock:
            for node_id, info in nodes.items():
                time_since_ping = now - info.get("last_seen", 0)
                if time_since_ping > 10 and info["status"] != "offline":
                    nodes[node_id]["status"] = "offline"
                    logging.warning(f"Worker '{node_id}' went offline (no ping for {time_since_ping:.1f}s)")


# Start the heartbeat monitor thread when the server boots
monitor_thread = threading.Thread(target=heartbeat_monitor, daemon=True)
# daemon=True means: "if the main program exits, kill this thread too"
monitor_thread.start()


# ── API Routes ───────────────────────────────────────────────
# A "route" is a URL path + HTTP method.
# When someone visits that URL, FastAPI calls the function below it.

# ── 1. Health Check ──────────────────────────────────────────
@app.get("/")
def root():
    """
    Simple health check — visit this in your browser to confirm the server is running.
    Returns: { "message": "EduGrid Master Node is running" }
    """
    return {"message": "EduGrid Master Node is running", "version": "1.0"}


# ── 2. Worker Heartbeat ──────────────────────────────────────
@app.post("/heartbeat")
def receive_heartbeat(payload: HeartbeatRequest):
    """
    Workers call this every 3 seconds to say "I'm alive and my status is X".

    The master records:
      - the worker's address (url)
      - the worker's current status (free / busy)
      - the current timestamp (last_seen)

    If the worker isn't in our list yet, we add it (auto-registration).
    """
    with nodes_lock:
        nodes[payload.node_id] = {
            "url": payload.url,
            "status": payload.status,
            "last_seen": time.time()   # record the exact time of this ping
        }

    logging.info(f"Heartbeat from '{payload.node_id}' — status: {payload.status}")
    return {"message": "Heartbeat received"}


# ── 3. Node Status ───────────────────────────────────────────
@app.get("/node-status")
def node_status():
    """
    Returns the current status of all registered workers.

    Example response:
    {
        "worker-1": { "url": "http://localhost:8001", "status": "free",   "last_seen": 1700000000 },
        "worker-2": { "url": "http://localhost:8002", "status": "busy",   "last_seen": 1700000001 },
        "worker-3": { "url": "http://localhost:8003", "status": "offline","last_seen": 1699999990 }
    }

    Arushi's dashboard will call this every 2 seconds to show live status.
    """
    with nodes_lock:
        return dict(nodes)  # return a copy so callers can't accidentally modify our data


# ── 4. Submit Task ───────────────────────────────────────────
@app.post("/submit-task")
def submit_task(payload: TaskRequest):
    """
    THE MAIN ENDPOINT — this is where students submit work.

    What happens step by step:
      1. Generate a unique task ID
      2. Find all free workers
      3. Split the data into N chunks (N = number of free workers)
      4. Send each chunk to one worker (in parallel threads)
      5. Wait for all workers to respond
      6. Combine the results
      7. Store and return the final result

    Example request body:
    {
        "task_type": "sort",
        "data": [9, 3, 7, 1, 5, 4, 8, 2, 6]
    }
    """
    # Step 1: Generate a unique task ID
    task_id = str(uuid.uuid4())[:8]   # e.g. "a3f2b1c9"
    logging.info(f"New task '{task_id}' received — type: {payload.task_type}, data size: {len(payload.data)}")

    # Step 2: Check if we have any free workers
    free_nodes = get_free_nodes()

    if not free_nodes:
        # No workers available — store task as "queued" and tell the student to check back
        with tasks_lock:
            tasks[task_id] = {
                "status": "queued",
                "task_type": payload.task_type,
                "data": payload.data,
                "result": None,
                "message": "No free workers right now. Check /task-status/{task_id} later."
            }
        logging.warning(f"Task '{task_id}' queued — no free workers")
        return {
            "task_id": task_id,
            "status": "queued",
            "message": "All workers are busy. Your task is queued."
        }

    # Step 3: Split data into chunks
    num_workers = len(free_nodes)
    chunks = split_task(payload.data, num_workers)
    logging.info(f"Task '{task_id}' split into {len(chunks)} chunk(s) across {num_workers} worker(s)")

    # Step 4: Mark the task as "in_progress"
    with tasks_lock:
        tasks[task_id] = {"status": "in_progress", "result": None}

    # Step 5: Send chunks to workers in parallel using threads
    # We collect results in a list, one slot per worker
    partial_results = [None] * len(chunks)   # pre-allocate: [None, None, None]

    def dispatch(index, worker_info, chunk):
        """
        Inner function that runs in its own thread.
        Sends one chunk to one worker and stores the result.
        """
        # Mark worker as busy
        with nodes_lock:
            if worker_info["url"] in [n["url"] for n in nodes.values()]:
                # find the node_id for this url and mark it busy
                for nid, ninfo in nodes.items():
                    if ninfo["url"] == worker_info["url"]:
                        nodes[nid]["status"] = "busy"
                        break

        result = send_chunk_to_worker(worker_info["url"], payload.task_type, chunk)
        partial_results[index] = result

        # Mark worker as free again after it finishes
        with nodes_lock:
            for nid, ninfo in nodes.items():
                if ninfo["url"] == worker_info["url"]:
                    nodes[nid]["status"] = "free"
                    break

    # Launch one thread per worker
    threads = []
    for i, (worker_info, chunk) in enumerate(zip(free_nodes, chunks)):
        t = threading.Thread(target=dispatch, args=(i, worker_info, chunk))
        threads.append(t)
        t.start()

    # Wait for ALL threads to finish before continuing
    for t in threads:
        t.join()

    # Step 6: Filter out any None results (from failed workers)
    valid_results = [r for r in partial_results if r is not None]

    if not valid_results:
        with tasks_lock:
            tasks[task_id]["status"] = "failed"
        raise HTTPException(status_code=500, detail="All workers failed to process the task")

    # Step 7: Aggregate (combine) partial results into one final result
    final_result = aggregate_results(valid_results, payload.task_type)
    logging.info(f"Task '{task_id}' completed successfully")

    # Store the final result
    with tasks_lock:
        tasks[task_id] = {
            "status": "completed",
            "result": final_result,
            "workers_used": num_workers
        }

    return {
        "task_id": task_id,
        "status": "completed",
        "result": final_result,
        "workers_used": num_workers
    }


# ── 5. Task Status ───────────────────────────────────────────
@app.get("/task-status/{task_id}")
def task_status(task_id: str):
    """
    Check the status of a previously submitted task.

    Useful when a task was queued (no free workers at submission time).
    The student can poll this endpoint until status = "completed".

    Example: GET /task-status/a3f2b1c9
    """
    with tasks_lock:
        if task_id not in tasks:
            raise HTTPException(status_code=404, detail=f"Task '{task_id}' not found")
        return tasks[task_id]


# ── 6. All Tasks ─────────────────────────────────────────────
@app.get("/all-tasks")
def all_tasks():
    """
    Returns all tasks ever submitted in this session.
    Useful for Arushi's dashboard to show task history.
    """
    with tasks_lock:
        return dict(tasks)
# ── 7. Benchmark ─────────────────────────────────────────────
@app.get("/benchmark")
def benchmark(n: int = 100, task: str = "sort"):
    import random

    data = random.sample(range(1, 10000), n)

    # Distributed execution (reuse your existing logic)
    start = time.time()
    final_result = aggregate_results([data], task)  # simplified
    distributed_time = round(time.time() - start, 3)

    # Single machine execution
    start = time.time()

    if task == "sort":
        single = sorted(data)

    elif task == "sum":
        single = sum(data)

    elif task == "square":
        single = [x * x for x in data]

    elif task == "filter_even":
        single = [x for x in data if x % 2 == 0]

    else:
        single = data

    single_time = round(time.time() - start, 3)

    return {
        "distributed_s": distributed_time,
        "single_s": single_time,
        "speedup": round(single_time / distributed_time if distributed_time else 1, 2),
        "n": n,
        "task": task,
        "workers_used": len(get_free_nodes())
    }

# ── 8. Manual Worker Registration ────────────────────────────
@app.post("/register-worker")
def register_worker(payload: HeartbeatRequest):
    """
    Allows a worker to explicitly register itself with the master.
    Workers can also auto-register via the /heartbeat endpoint.
    This is just an alternative explicit registration route.
    """
    with nodes_lock:
        nodes[payload.node_id] = {
            "url": payload.url,
            "status": payload.status,
            "last_seen": time.time()
        }
    logging.info(f"Worker '{payload.node_id}' registered at {payload.url}")
    return {"message": f"Worker '{payload.node_id}' registered successfully"}


# ── Run the server (alternative to uvicorn command) ─────────
# You can also just run:  python master.py
# (But 'uvicorn master:app --reload' is better during development)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("master:app", host="0.0.0.0", port=8000, reload=True)