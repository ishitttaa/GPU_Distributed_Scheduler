"""
EduGrid - Master Node
======================
The master is the "restaurant manager":
  1. Receives the full task from the user
  2. Splits data into chunks
  3. Finds which workers are free (heartbeat check)
  4. Sends one chunk to each free worker in parallel
  5. Waits for all results and combines them

Run AFTER starting all three workers:
  python master.py
"""

import requests
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Worker registry  (add more ports = more workers) ──────────────────────
WORKERS = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
]

TIMEOUT = 10   # seconds to wait for a worker response


# ══════════════════════════════════════════════════════════════════════════
# STEP 1 — Heartbeat: find all alive + free workers
# ══════════════════════════════════════════════════════════════════════════
def get_available_workers():
    """Ping every worker; return only those that are alive and free."""
    available = []
    for url in WORKERS:
        try:
            r = requests.get(f"{url}/status", timeout=2)
            info = r.json()
            if info.get("status") == "free":
                available.append({
                    "url":  url,
                    "cpu":  info.get("cpu", 0),
                    "ram":  info.get("ram", 0),
                    "node": info.get("node"),
                })
        except Exception:
            pass   # worker is offline — skip it silently
    return available


# ══════════════════════════════════════════════════════════════════════════
# STEP 2 — Task splitting
# ══════════════════════════════════════════════════════════════════════════
def split_data(data, num_workers):
    """
    Split data into EXACTLY num_workers chunks
    """
    if num_workers == 0:
        return []

    chunk_size = len(data) // num_workers
    chunks = []

    for i in range(num_workers):
        start = i * chunk_size
        end = (i + 1) * chunk_size if i != num_workers - 1 else len(data)
        chunks.append(data[start:end])

    return chunks

# ══════════════════════════════════════════════════════════════════════════
# STEP 3 — Send one chunk to one worker (runs in a thread)
# ══════════════════════════════════════════════════════════════════════════
def send_to_worker(worker_info, chunk, task):
    """POST a data chunk to a single worker and return its result."""
    url = worker_info["url"]
    payload = {"data": chunk, "task": task}
    try:
        r = requests.post(f"{url}/process", json=payload, timeout=TIMEOUT)
        result = r.json()
        return result
    except Exception as e:
        return {
            "node":       worker_info["node"],
            "status":     "error",
            "error":      str(e),
            "result":     [],
            "time_taken": 0,
        }


# ══════════════════════════════════════════════════════════════════════════
# STEP 4 — Orchestrate: split → assign → parallel execute → aggregate
# ══════════════════════════════════════════════════════════════════════════
def distribute_task(data, task="sort"):
    """
    Full pipeline:
      find free workers → split data → send in parallel → merge results
    Returns a rich summary dict.
    """
    print(f"\n{'─'*50}")
    print(f"  MASTER: New task='{task}', {len(data)} items")
    print(f"{'─'*50}")

    # -- A. Check available workers --
    workers = get_available_workers()
    if not workers:
        print("  [MASTER] No workers available!")
        return {"error": "No workers available", "result": None}

    # Sort workers by CPU load — prefer least-busy node (smart allocation)
    workers.sort(key=lambda w: w["cpu"])
    print(f"  [MASTER] Available workers: {[w['node'] for w in workers]}")

    # -- B. Split data --
    chunks = split_data(data, len(workers))
    print(f"  [MASTER] Chunks: {[len(c) for c in chunks]} items each")

    master_start = time.time()

    # -- C. Send all chunks in parallel using threads --
    futures = {}
    results_by_node = {}
    with ThreadPoolExecutor(max_workers=len(workers)) as executor:
        for worker, chunk in zip(workers, chunks):
            future = executor.submit(send_to_worker, worker, chunk, task)
            futures[future] = worker["node"]

        # -- D. Collect results as they come in --
        for future in as_completed(futures):
            node_name = futures[future]
            result    = future.result()
            results_by_node[node_name] = result
            status = result.get("status", "?")
            t      = result.get("time_taken", "?")
            print(f"  [MASTER] {node_name} → {status}  ({t}s)")

    total_time = round(time.time() - master_start, 4)

    # -- E. Aggregate results --
    #    For "sort": merge sorted chunks (works like merge-sort's merge step)
    #    For "sum":  add all partial sums
    #    For others: concatenate results
    all_results = [r.get("result", []) for r in results_by_node.values()
                   if r.get("status") == "done"]

    if task == "sort":
        # Each chunk was already sorted — merge them
        import heapq
        final_result = list(heapq.merge(*all_results))
    elif task == "sum":
        final_result = sum(all_results)
    else:
        # flatten: [[a,b],[c,d]] → [a,b,c,d]
        final_result = [item for sublist in all_results for item in sublist]

    print(f"\n  [MASTER] Completed in {total_time}s  →  {str(final_result)[:80]}")
    print(f"{'─'*50}\n")

    return {
        "task":         task,
        "total_items":  len(data),
        "workers_used": len(workers),
        "total_time":   total_time,
        "result":       final_result,
        "per_node":     results_by_node,
    }


# ══════════════════════════════════════════════════════════════════════════
# Demo — run standalone to test
# ══════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import pandas as pd

    df = pd.read_csv("data.csv")

    test_data = df.select_dtypes(include='number').iloc[:,0].tolist()

    print(f"Loaded {len(test_data)} data points")

    print("\n🧪 Test 1: Distributed SORT")
    out = distribute_task(test_data, task="sort")
    print(f"First 10 sorted: {out['result'][:10]}")

    print("\n🧪 Test 2: Distributed SUM")
    out2 = distribute_task(test_data, task="sum")
    single_sum = sum(test_data)
    print(f"Distributed sum: {out2['result']} | Single machine: {single_sum}")
    print(f"Match: {out2['result'] == single_sum}")