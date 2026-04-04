"""
EduGrid - Master Node
=====================
Features:
  - Predictive load balancing
  - CPU / GPU / Hybrid mode support
  - Fault tolerance with redistribution
  - Weighted chunk splitting
"""

import requests
import time
import heapq
from concurrent.futures import ThreadPoolExecutor, as_completed

WORKERS = [
    "http://172.16.70.87:8001",
    "http://172.16.68.7:8001",
    "http://172.16.68.38:8001",
]

TIMEOUT = 5
_worker_scores = {}


def get_available_workers(mode="cpu", min_vram_gb=0, min_ram_gb=0):
    available = []

    for url in WORKERS:
        try:
            r = requests.get(f"{url}/status", timeout=0.5)
            info = r.json()

            if info.get("status") == "busy":
                continue

            cpu = info.get("cpu", 0)
            ram = info.get("ram", 0)
            anomaly = info.get("anomaly", False)
            gpu_available = info.get("gpu_available", False)
            vram_free_gb = info.get("vram_free_gb", 0)
            ram_free_gb = info.get("ram_free_gb", 0)

            base_capacity = info.get("cpu_weight", 1.0)
            gpu_capacity = info.get("gpu_weight", 0.0)

            if mode == "gpu" and not gpu_available:
                continue

            if mode in ("gpu", "hybrid") and vram_free_gb < min_vram_gb:
                continue

            if ram_free_gb < min_ram_gb:
                continue

            load_penalty = (cpu * 0.7) + (ram * 0.3)

            if mode == "gpu":
                effective_capacity = base_capacity + gpu_capacity + max(vram_free_gb, 0)
            elif mode == "hybrid":
                effective_capacity = base_capacity + (gpu_capacity * 0.5)
            else:
                effective_capacity = base_capacity

            score = load_penalty / max(effective_capacity, 0.1)

            if anomaly:
                score *= 1.5
                print(f"  ⚠️ [{url.split(':')[-1]}] anomaly detected — reduced load")

            _worker_scores[url] = score

            available.append({
                "url": url,
                "cpu": cpu,
                "ram": ram,
                "ram_free_gb": ram_free_gb,
                "gpu_available": gpu_available,
                "vram_free_gb": vram_free_gb,
                "score": score,
                "anomaly": anomaly,
                "node": info.get("node", url),
            })

        except Exception:
            print(f"  💀 Worker at {url} is OFFLINE — skipping")

    available.sort(key=lambda w: w["score"])
    return available


def split_data_weighted(data, workers):
    if not workers:
        return []

    scores = [max(w["score"], 1) for w in workers]
    inv = [1.0 / s for s in scores]
    total = sum(inv)
    weights = [x / total for x in inv]

    chunks = []
    start = 0
    n = len(data)

    for i, w in enumerate(weights):
        if i == len(weights) - 1:
            chunks.append(data[start:])
        else:
            size = max(1, round(n * w))
            chunks.append(data[start:start + size])
            start += size

    return chunks


def send_to_worker(worker_info, chunk, task, mode="cpu", min_vram_gb=0, min_ram_gb=0):
    url = worker_info["url"]
    payload = {
        "data": chunk,
        "task": task,
        "mode": mode,
        "min_vram_gb": min_vram_gb,
        "min_ram_gb": min_ram_gb,
    }

    try:
        r = requests.post(f"{url}/process", json=payload, timeout=TIMEOUT)
        return r.json()
    except Exception as e:
        return {
            "node": worker_info["node"],
            "status": "error",
            "error": str(e),
            "result": None,
            "chunk": chunk,
            "time_taken": 0,
            "mode": mode,
        }


def redistribute_failed(failed_chunks, survivors, task, mode="cpu", min_vram_gb=0, min_ram_gb=0):
    if not survivors or not failed_chunks:
        return {}

    print(
        f"\n  🔁 FAULT RECOVERY: redistributing {len(failed_chunks)} failed chunk(s) "
        f"across {len(survivors)} surviving worker(s)"
    )

    merged_failed = []
    for chunk in failed_chunks:
        if isinstance(chunk, list):
            merged_failed.extend(chunk)
        elif chunk is not None:
            merged_failed.append(chunk)

    rechunks = split_data_weighted(merged_failed, survivors)
    recovery = {}

    with ThreadPoolExecutor(max_workers=len(survivors)) as ex:
        futs = {}
        for worker, chunk in zip(survivors, rechunks):
            fut = ex.submit(send_to_worker, worker, chunk, task, mode, min_vram_gb, min_ram_gb)
            futs[fut] = worker["node"]

        for fut in as_completed(futs):
            node = futs[fut]
            result = fut.result()
            recovery[node + "_recovery"] = result
            print(f"  🔁 Recovery via {node} → {result.get('status', '?')}")

    return recovery


def distribute_task_hybrid(data, task="square", min_vram_gb=0, min_ram_gb=0):
    print(f"\n{'─' * 55}")
    print(f"  MASTER: task='{task}', {len(data)} items | mode='hybrid'")
    print(f"{'─' * 55}")

    cpu_workers = get_available_workers("cpu", 0, min_ram_gb)
    if not cpu_workers:
        return {
            "error": "No CPU workers available for hybrid stage",
            "result": None,
            "mode": "hybrid",
            "min_vram_gb": min_vram_gb,
            "min_ram_gb": min_ram_gb,
        }

    chunks = split_data_weighted(data, cpu_workers)
    master_start = time.time()
    per_node = {}
    stage1_results = []
    failed_chunks = []
    survivor_workers = []

    with ThreadPoolExecutor(max_workers=len(cpu_workers)) as executor:
        futures = {}
        for worker, chunk in zip(cpu_workers, chunks):
            fut = executor.submit(
                send_to_worker,
                worker,
                chunk,
                "preprocess",
                "cpu",
                0,
                min_ram_gb
            )
            futures[fut] = worker

        for fut in as_completed(futures):
            worker = futures[fut]
            result = fut.result()
            node = worker["node"]

            if result.get("status") == "done":
                per_node[node] = result
                survivor_workers.append(worker)
                if isinstance(result.get("result"), list):
                    stage1_results.extend(result["result"])
                print(f"  ✅ {node} → preprocess done ({result.get('time_taken')}s)")
            else:
                failed_chunks.append(result.get("chunk", []))
                print(f"  ❌ {node} → preprocess failed")

    if failed_chunks and survivor_workers:
        recovery = redistribute_failed(
            failed_chunks,
            survivor_workers,
            "preprocess",
            "cpu",
            0,
            min_ram_gb
        )
        per_node.update(recovery)
        for r in recovery.values():
            if r.get("status") == "done" and isinstance(r.get("result"), list):
                stage1_results.extend(r["result"])

    gpu_workers = get_available_workers("gpu", min_vram_gb, min_ram_gb)
    if not gpu_workers:
        return {
            "error": "No GPU worker available for hybrid final stage",
            "result": None,
            "mode": "hybrid",
            "per_node": per_node,
            "min_vram_gb": min_vram_gb,
            "min_ram_gb": min_ram_gb,
        }

    best_gpu_worker = gpu_workers[0]
    final_gpu_task = task if task in ("square", "gpu_square") else "square"

    gpu_result = send_to_worker(
        best_gpu_worker,
        stage1_results,
        final_gpu_task,
        "gpu",
        min_vram_gb,
        min_ram_gb
    )

    per_node[best_gpu_worker["node"] + "_gpu_final"] = gpu_result

    if gpu_result.get("status") != "done":
        return {
            "error": "Hybrid GPU final stage failed",
            "result": None,
            "mode": "hybrid",
            "per_node": per_node,
            "fault_occurred": True,
            "recovered": False,
            "min_vram_gb": min_vram_gb,
            "min_ram_gb": min_ram_gb,
        }

    total_time = round(time.time() - master_start, 4)

    print(f"\n  MASTER: Hybrid done in {total_time}s")
    print(f"{'─' * 55}\n")

    return {
        "task": task,
        "mode": "hybrid",
        "total_items": len(data),
        "workers_used": len(cpu_workers) + 1,
        "total_time": total_time,
        "result": gpu_result.get("result", []),
        "per_node": per_node,
        "fault_occurred": bool(failed_chunks),
        "recovered": True if gpu_result.get("status") == "done" else False,
        "worker_scores": {w["node"]: round(w["score"], 2) for w in cpu_workers + [best_gpu_worker]},
        "min_vram_gb": min_vram_gb,
        "min_ram_gb": min_ram_gb,
    }


def distribute_task(data, task="sort", mode="cpu", min_vram_gb=0, min_ram_gb=0):
    if mode == "hybrid":
        return distribute_task_hybrid(data, task, min_vram_gb, min_ram_gb)

    print(f"\n{'─' * 55}")
    print(f"  MASTER: task='{task}', {len(data)} items | mode='{mode}'")
    print(f"{'─' * 55}")

    workers = get_available_workers(mode, min_vram_gb, min_ram_gb)
    if not workers:
        return {
            "error": "No eligible workers available",
            "result": None,
            "mode": mode,
            "min_vram_gb": min_vram_gb,
            "min_ram_gb": min_ram_gb,
        }

    print(f"  Workers: {[w['node'] for w in workers]}")
    print(f"  Scores:  {[round(w['score'], 2) for w in workers]}")

    chunks = split_data_weighted(data, workers)
    print(f"  Chunks:  {[len(c) for c in chunks]} items (weighted)")

    master_start = time.time()
    results_by_node = {}
    failed_chunks = []
    survivor_workers = []

    futures = {}
    with ThreadPoolExecutor(max_workers=len(workers)) as executor:
        for worker, chunk in zip(workers, chunks):
            fut = executor.submit(
                send_to_worker,
                worker,
                chunk,
                task,
                mode,
                min_vram_gb,
                min_ram_gb
            )
            futures[fut] = worker

        for fut in as_completed(futures):
            worker = futures[fut]
            result = fut.result()
            node = worker["node"]

            if result.get("status") == "done":
                results_by_node[node] = result
                survivor_workers.append(worker)
                print(f"  ✅ {node} → done ({result.get('time_taken')}s)")
            else:
                failed_chunks.append(result.get("chunk", []))
                print(f"  ❌ {node} → FAILED — queuing for recovery")

    if failed_chunks:
        recovery = redistribute_failed(
            failed_chunks,
            survivor_workers,
            task,
            mode,
            min_vram_gb,
            min_ram_gb
        )
        results_by_node.update(recovery)

    total_time = round(time.time() - master_start, 4)

    all_results = [
        r.get("result", [])
        for r in results_by_node.values()
        if r.get("status") == "done"
    ]

    if task == "sort":
        sorted_chunks = [sorted(c) if isinstance(c, list) else [] for c in all_results]
        final_result = list(heapq.merge(*sorted_chunks))

    elif task == "sum":
        final_result = sum(
            r if isinstance(r, (int, float)) else sum(r)
            for r in all_results
        )

    elif task in ("square", "gpu_square", "filter_even", "filter_odd", "normalize", "preprocess"):
        final_result = [item for sub in all_results if isinstance(sub, list) for item in sub]

    else:
        final_result = [item for sub in all_results if isinstance(sub, list) for item in sub]

    workers_used = len(workers)
    fault_occurred = bool(failed_chunks)
    recovered = fault_occurred and bool(results_by_node)

    print(f"\n  MASTER: Done in {total_time}s | fault={fault_occurred} | recovered={recovered}")
    print(f"{'─' * 55}\n")

    return {
        "task": task,
        "mode": mode,
        "total_items": len(data),
        "workers_used": workers_used,
        "total_time": total_time,
        "result": final_result,
        "per_node": results_by_node,
        "fault_occurred": fault_occurred,
        "recovered": recovered,
        "worker_scores": {w["node"]: round(w["score"], 2) for w in workers},
        "min_vram_gb": min_vram_gb,
        "min_ram_gb": min_ram_gb,
    }


if __name__ == "__main__":
    import random

    data = random.sample(range(1, 100_001), 10_000)

    print("\n🧪 Test 1: Sort")
    r = distribute_task(data, "sort", mode="cpu")
    print(f"First 5: {r['result'][:5]}")

    print("\n🧪 Test 2: Sum")
    r = distribute_task(data, "sum", mode="cpu")
    print(f"Distributed sum: {r['result']} | Single: {sum(data)}")

    print("\n🧪 Test 3: GPU")
    r = distribute_task(data[:1000], "gpu_square", mode="gpu", min_vram_gb=1)
    print(f"First 3 squares: {r.get('result', [])[:3]}")