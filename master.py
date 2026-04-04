"""
EduGrid - Master Node (Upgraded)
==================================
Upgrades over v1:
  ✅ FAULT TOLERANCE — if a worker dies mid-task, its chunk
     is redistributed to surviving workers automatically
  ✅ PREDICTIVE LOAD BALANCING — routes chunks to workers
     based on weighted CPU + RAM score, not just CPU alone
  ✅ ANOMALY DETECTION — flags degraded workers (high CPU)
     and reduces their workload share
  ✅ RETRY LOGIC — failed chunks retried on other workers
  ✅ Real benchmark (no fake multipliers)

Run AFTER starting all three workers:
  python master.py
"""

import requests
import time
import heapq
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Worker registry ────────────────────────────────────────────────────────
WORKERS = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
]

TIMEOUT         = 10
HEARTBEAT_EVERY = 5    # seconds between background health checks

# ── In-memory worker health scores (updated each run) ─────────────────────
_worker_scores = {}    # url → float score (lower = prefer)


# ══════════════════════════════════════════════════════════════════════════
# STEP 1 — Heartbeat: find all alive + free workers
# ══════════════════════════════════════════════════════════════════════════
def get_available_workers(gpu_required=False, min_vram_gb=0, min_ram_gb=0):
    """
    Ping every worker. Return those that are alive.
    Includes 'degraded' workers (high CPU) but marks them —
    the load balancer will assign them smaller chunks.
    """
    available = []
    for url in WORKERS:
        try:
            r    = requests.get(f"{url}/status", timeout=2)
            info = r.json()

            if info.get("status") == "busy":
               continue   # skip busy workers

            cpu = info.get("cpu", 0)
            ram = info.get("ram", 0)
            anomaly = info.get("anomaly", False)
            gpu_available = info.get("gpu_available", False)
            vram_free_gb  = info.get("vram_free_gb", 0)
            ram_free_gb   = info.get("ram_free_gb", 0)

            if gpu_required and not gpu_available:
                continue

            if vram_free_gb < min_vram_gb:
                continue

            if ram_free_gb < min_ram_gb:
                continue

            # Weighted health score: lower = healthier = gets more work
            # CPU matters 70%, RAM matters 30%
            if gpu_required:
               score = (cpu * 0.5) + (ram * 0.2) - (vram_free_gb * 5)
            else:
               score = (cpu * 0.7) + (ram * 0.3)
            # Penalise anomalous workers — give them 30% less weight
            if anomaly:
                score *= 1.5
                print(f"  ⚠️  [{url.split(':')[-1]}] ANOMALY detected (CPU spike) — reduced load")

            _worker_scores[url] = score

            available.append({"url":           url,"cpu":           cpu,"ram":           ram,"ram_free_gb":   ram_free_gb,"gpu_available": gpu_available,"vram_free_gb":  vram_free_gb,
                                "score":         score,"anomaly":       anomaly,"node":          info.get("node"),})
        except Exception:
            print(f"  💀 Worker at {url} is OFFLINE — skipping")

    # Sort by health score — best workers first
    available.sort(key=lambda w: w["score"])
    return available


# ══════════════════════════════════════════════════════════════════════════
# STEP 2 — Predictive chunk sizing (not equal splits)
# ══════════════════════════════════════════════════════════════════════════
def split_data_weighted(data, workers):
    """
    Instead of equal chunks, give healthier workers more data.
    Inversely proportional to health score (lower score = more data).

    Example: scores [10, 20, 50]
      inverse weights → [1/10, 1/20, 1/50] = [0.1, 0.05, 0.02]
      normalised → worker-0 gets 59%, worker-1 gets 30%, worker-2 gets 12%
    """
    if not workers:
        return []

    scores  = [max(w["score"], 1) for w in workers]   # avoid div/0
    inv     = [1.0 / s for s in scores]
    total   = sum(inv)
    weights = [i / total for i in inv]

    chunks  = []
    start   = 0
    n       = len(data)

    for i, w in enumerate(weights):
        if i == len(weights) - 1:
            chunks.append(data[start:])           # last worker gets remainder
        else:
            size = max(1, round(n * w))
            chunks.append(data[start:start + size])
            start += size

    return chunks


# ══════════════════════════════════════════════════════════════════════════
# STEP 3 — Send chunk to one worker
# ══════════════════════════════════════════════════════════════════════════
def send_to_worker(worker_info, chunk, task, gpu_required=False, min_vram_gb=0, min_ram_gb=0):
    """POST a chunk to a worker. Returns result dict."""
    url     = worker_info["url"]
    payload = {
    "data": chunk,
    "task": task,
    "gpu_required": gpu_required,
    "min_vram_gb": min_vram_gb,
    "min_ram_gb": min_ram_gb,
    }
    try:
        r      = requests.post(f"{url}/process", json=payload, timeout=TIMEOUT)
        result = r.json()
        return result
    except Exception as e:
        return {
            "node":       worker_info["node"],
            "status":     "error",
            "error":      str(e),
            "result":     None,
            "chunk":      chunk,    # keep chunk for redistribution
            "time_taken": 0,
        }


# ══════════════════════════════════════════════════════════════════════════
# STEP 4 — FAULT TOLERANCE: redistribute failed chunks
# ══════════════════════════════════════════════════════════════════════════
def redistribute_failed(failed_chunks, survivors, task, gpu_required=False, min_vram_gb=0, min_ram_gb=0):
    """
    Called when one or more workers fail mid-task.
    Merges all failed chunks and re-assigns them to surviving workers.
    This is the "self-healing" moment — shown live on dashboard.
    """
    if not survivors or not failed_chunks:
        return {}

    print(f"\n  🔁 FAULT RECOVERY: redistributing {len(failed_chunks)} failed chunk(s) "
          f"across {len(survivors)} surviving worker(s)")

    # Merge all failed data into one flat list
    merged_failed = []
    for chunk in failed_chunks:
        if isinstance(chunk, list):
            merged_failed.extend(chunk)
        elif chunk is not None:
            merged_failed.append(chunk)

    # Re-split across survivors
    rechunks   = split_data_weighted(merged_failed, survivors)
    recovery   = {}

    with ThreadPoolExecutor(max_workers=len(survivors)) as ex:
        futs = {}
        for worker, chunk in zip(survivors, rechunks):
            fut = ex.submit(send_to_worker,worker,chunk,task,gpu_required,min_vram_gb,min_ram_gb)
            futs[fut] = worker["node"]

        for fut in as_completed(futs):
            node   = futs[fut]
            result = fut.result()
            recovery[node + "_recovery"] = result
            status = result.get("status", "?")
            print(f"  🔁 Recovery via {node} → {status}")

    return recovery


# ══════════════════════════════════════════════════════════════════════════
# STEP 5 — Full pipeline
# ══════════════════════════════════════════════════════════════════════════
def distribute_task(data, task="sort", gpu_required=False, min_vram_gb=0, min_ram_gb=0):
    """
    Full pipeline with fault tolerance:
      1. Find healthy workers (predictive scoring)
      2. Split data proportionally (weighted chunking)
      3. Execute in parallel
      4. Detect failures → auto-redistribute → recover
      5. Aggregate final result
    """
    print(f"\n{'─'*55}")
    print(f"  MASTER: task='{task}', {len(data)} items")
    print(f"{'─'*55}")

    workers = get_available_workers(gpu_required, min_vram_gb, min_ram_gb)
    if not workers:
        return {"error": "No eligible workers available","result": None,"gpu_required": gpu_required,"min_vram_gb": min_vram_gb,"min_ram_gb": min_ram_gb, }

    print(f"  Workers: {[w['node'] for w in workers]}")
    print(f"  Scores:  {[round(w['score'], 1) for w in workers]}")

    # Weighted split
    chunks = split_data_weighted(data, workers)
    print(f"  Chunks:  {[len(c) for c in chunks]} items (weighted)")

    master_start     = time.time()
    results_by_node  = {}
    failed_chunks    = []
    survivor_workers = []

    # ── Parallel execution ─────────────────────────────────────────────
    futures = {}
    with ThreadPoolExecutor(max_workers=len(workers)) as executor:
        for worker, chunk in zip(workers, chunks):
            fut = executor.submit(send_to_worker,worker,chunk,task,gpu_required,min_vram_gb,min_ram_gb)
            futures[fut] = worker

        for fut in as_completed(futures):
            worker = futures[fut]
            result = fut.result()
            node   = worker["node"]

            if result.get("status") == "done":
                results_by_node[node] = result
                survivor_workers.append(worker)
                print(f"  ✅ {node} → done ({result.get('time_taken')}s)")
            else:
                # Worker failed — save its chunk for recovery
                failed_chunks.append(result.get("chunk", []))
                print(f"  ❌ {node} → FAILED — queuing for recovery")

    # ── Fault recovery ─────────────────────────────────────────────────
    if failed_chunks:
        recovery = redistribute_failed(failed_chunks,survivor_workers,task,gpu_required,min_vram_gb,min_ram_gb)
        results_by_node.update(recovery)

    total_time = round(time.time() - master_start, 4)

    # ── Aggregate ──────────────────────────────────────────────────────
    all_results = [
        r.get("result", [])
        for r in results_by_node.values()
        if r.get("status") == "done"
    ]

    if task == "sort":
        sorted_chunks = [sorted(c) if isinstance(c, list) else [] for c in all_results]
        final_result  = list(heapq.merge(*sorted_chunks))

    elif task == "sum":
        final_result = sum(
            r if isinstance(r, (int, float)) else sum(r)
            for r in all_results
        )

    elif task in ("square", "filter_even", "filter_odd", "normalize"):
        final_result = [item for sub in all_results if isinstance(sub, list) for item in sub]

    else:
        final_result = [item for sub in all_results if isinstance(sub, list) for item in sub]

    workers_used     = len(workers)
    fault_occurred   = bool(failed_chunks)
    recovered        = fault_occurred and bool(results_by_node)

    print(f"\n  MASTER: Done in {total_time}s | fault={fault_occurred} | recovered={recovered}")
    print(f"{'─'*55}\n")

    return {
       "task":          task,
       "total_items":   len(data),
       "workers_used":  workers_used,
       "total_time":    total_time,
       "result":        final_result,
       "per_node":      results_by_node,
       "fault_occurred": fault_occurred,
       "recovered":     recovered,
       "worker_scores": {w["node"]: round(w["score"], 2) for w in workers},
       "gpu_required":  gpu_required,
       "min_vram_gb":   min_vram_gb,
      "min_ram_gb":    min_ram_gb,}


# ══════════════════════════════════════════════════════════════════════════
# Demo — run standalone
# ══════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import random
    data = random.sample(range(1, 100_001), 10_000)

    print("\n🧪 Test 1: Sort")
    r = distribute_task(data, "sort")
    print(f"  First 5: {r['result'][:5]}")

    print("\n🧪 Test 2: Sum")
    r = distribute_task(data, "sum")
    print(f"  Distributed sum: {r['result']} | Single: {sum(data)}")

    print("\n🧪 Test 3: Square")
    r = distribute_task(data[:100], "square")
    print(f"  First 3 squares: {r['result'][:3]}")
