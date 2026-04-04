"""
EduGrid - Worker Node
=====================
Supports:
  - CPU / GPU / Hybrid execution
  - Live CPU/RAM/GPU stats
  - Task history
"""

from flask import Flask, request, jsonify
import time
import sys
import psutil
import collections
import os

try:
    import GPUtil
except ImportError:
    GPUtil = None

app = Flask(__name__)

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
NODE_NAME = sys.argv[2] if len(sys.argv) > 2 else f"Worker-{PORT}"

CPU_WEIGHT = float(os.getenv("CPU_WEIGHT", "1.0"))
GPU_WEIGHT = float(os.getenv("GPU_WEIGHT", "0.0"))
NODE_ROLE = os.getenv("NODE_ROLE", "cpu")

busy = False
task_history = collections.deque(maxlen=10)
CPU_ANOMALY_THRESHOLD = 85.0
RAM_ANOMALY_THRESHOLD = 90.0
total_tasks_done = 0


def get_gpu_info():
    if GPUtil is None:
        return {
            "gpu_available": False,
            "gpu_name": None,
            "vram_total_gb": 0,
            "vram_free_gb": 0,
            "gpu_utilization": 0,
        }

    try:
        gpus = GPUtil.getGPUs()
        if not gpus:
            return {
                "gpu_available": False,
                "gpu_name": None,
                "vram_total_gb": 0,
                "vram_free_gb": 0,
                "gpu_utilization": 0,
            }

        g = gpus[0]
        return {
            "gpu_available": True,
            "gpu_name": g.name,
            "vram_total_gb": round(g.memoryTotal / 1024, 2),
            "vram_free_gb": round(g.memoryFree / 1024, 2),
            "gpu_utilization": round(g.load * 100, 2),
        }

    except Exception:
        return {
            "gpu_available": False,
            "gpu_name": None,
            "vram_total_gb": 0,
            "vram_free_gb": 0,
            "gpu_utilization": 0,
        }


@app.route('/status', methods=['GET'])
def status():
    cpu = psutil.cpu_percent(interval=0.1)
    vm = psutil.virtual_memory()
    ram = vm.percent
    ram_free_gb = round(vm.available / (1024 ** 3), 2)
    ram_total_gb = round(vm.total / (1024 ** 3), 2)
    gpu_info = get_gpu_info()

    anomaly = cpu > CPU_ANOMALY_THRESHOLD or ram > RAM_ANOMALY_THRESHOLD

    return jsonify({
        "node": NODE_NAME,
        "port": PORT,
        "status": "busy" if busy else "free",
        "cpu": cpu,
        "ram": ram,
        "ram_free_gb": ram_free_gb,
        "ram_total_gb": ram_total_gb,
        "anomaly": anomaly,
        "total_tasks_done": total_tasks_done,
        "gpu_available": gpu_info["gpu_available"],
        "gpu_name": gpu_info["gpu_name"],
        "vram_total_gb": gpu_info["vram_total_gb"],
        "vram_free_gb": gpu_info["vram_free_gb"],
        "gpu_utilization": gpu_info["gpu_utilization"],
        "cpu_weight": CPU_WEIGHT,
        "gpu_weight": GPU_WEIGHT,
        "node_role": NODE_ROLE,
    })


@app.route('/process', methods=['POST'])
def process():
    global busy, total_tasks_done
    busy = True

    cpu_before = psutil.cpu_percent(interval=0.1)
    ram_before = psutil.virtual_memory().percent

    mode = "cpu"
    min_vram_gb = 0
    min_ram_gb = 0

    try:
        payload = request.json or {}

        data = payload.get("data", [])
        task = payload.get("task", "sort")
        mode = payload.get("mode", "cpu")
        min_vram_gb = payload.get("min_vram_gb", 0)
        min_ram_gb = payload.get("min_ram_gb", 0)

        print(f"\n[{NODE_NAME}] task='{task}', mode='{mode}', {len(data)} items")

        gpu_info = get_gpu_info()
        vm = psutil.virtual_memory()
        ram_free_gb = round(vm.available / (1024 ** 3), 2)

        if mode == "gpu" and not gpu_info["gpu_available"]:
            raise RuntimeError("GPU mode requested but not available on this worker")

        if mode in ("gpu", "hybrid") and min_vram_gb > 0:
            if gpu_info["vram_free_gb"] < min_vram_gb:
                raise RuntimeError(f"Insufficient VRAM: required {min_vram_gb} GB")

        if ram_free_gb < min_ram_gb:
            raise RuntimeError(f"Insufficient free RAM: required {min_ram_gb} GB")

        start = time.time()

        if task == "sort":
            result = sorted(data)

        elif task == "sum":
            result = sum(data)

        elif task == "square":
            if mode == "gpu":
                try:
                    import torch
                    if not torch.cuda.is_available():
                        raise RuntimeError("CUDA not available")
                    t = torch.tensor(data, device="cuda", dtype=torch.float32)
                    result = (t * t).cpu().tolist()
                except Exception:
                    result = [x ** 2 for x in data]
            else:
                result = [x ** 2 for x in data]

        elif task == "gpu_square":
            import torch
            if not torch.cuda.is_available():
                raise RuntimeError("CUDA not available")
            t = torch.tensor(data, device="cuda", dtype=torch.float32)
            result = (t * t).cpu().tolist()

        elif task == "filter_even":
            result = [x for x in data if x % 2 == 0]

        elif task == "filter_odd":
            result = [x for x in data if x % 2 != 0]

        elif task == "normalize":
            if data:
                mn, mx = min(data), max(data)
                rng = mx - mn or 1
                result = [round((x - mn) / rng, 4) for x in data]
            else:
                result = []

        elif task == "preprocess":
            result = [int(x) for x in data if isinstance(x, (int, float))]

        else:
            result = data

        elapsed = round(time.time() - start, 4)
        cpu_after = psutil.cpu_percent(interval=0.1)
        ram_after = psutil.virtual_memory().percent
        total_tasks_done += 1

        task_history.append({
            "task": task,
            "mode": mode,
            "items": len(data),
            "time_taken": elapsed,
            "cpu_before": cpu_before,
            "cpu_after": cpu_after,
            "ram_before": ram_before,
            "ram_after": ram_after,
            "anomaly": (cpu_after > CPU_ANOMALY_THRESHOLD) or (ram_after > RAM_ANOMALY_THRESHOLD),
            "timestamp": time.strftime("%H:%M:%S"),
            "min_vram_gb": min_vram_gb,
            "min_ram_gb": min_ram_gb,
        })

        print(f"[{NODE_NAME}] Done in {elapsed}s | CPU {cpu_before}→{cpu_after}% | mode={mode}")

        return jsonify({
            "result": result,
            "node": NODE_NAME,
            "task": task,
            "mode": mode,
            "time_taken": elapsed,
            "items_in": len(data),
            "cpu_before": cpu_before,
            "cpu_after": cpu_after,
            "ram_before": ram_before,
            "ram_after": ram_after,
            "anomaly": (cpu_after > CPU_ANOMALY_THRESHOLD) or (ram_after > RAM_ANOMALY_THRESHOLD),
            "status": "done",
            "min_vram_gb": min_vram_gb,
            "min_ram_gb": min_ram_gb,
        })

    except Exception as e:
        print(f"[{NODE_NAME}] ERROR: {e}")
        return jsonify({
            "result": [],
            "node": NODE_NAME,
            "status": "error",
            "error": str(e),
            "time_taken": 0,
            "mode": mode,
            "min_vram_gb": min_vram_gb,
            "min_ram_gb": min_ram_gb,
        }), 500

    finally:
        busy = False


@app.route('/history', methods=['GET'])
def history():
    return jsonify({
        "node": NODE_NAME,
        "history": list(task_history),
    })


if __name__ == '__main__':
    print(f"\n{'=' * 45}")
    print(f"  {NODE_NAME} starting on port {PORT}")
    print(f"{'=' * 45}")
    print(f"  Role       : {NODE_ROLE}")
    print(f"  CPU Weight : {CPU_WEIGHT}")
    print(f"  GPU Weight : {GPU_WEIGHT}")
    print(f"{'=' * 45}\n")

    app.run(host='0.0.0.0', port=PORT, debug=False)