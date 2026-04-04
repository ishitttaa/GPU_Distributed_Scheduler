"""
EduGrid - Worker Node (Upgraded)
==================================
Upgrades over v1:
  ✅ Real CPU/RAM tracking per task
  ✅ Anomaly detection (flags if CPU > threshold)
  ✅ Task history log (last 10 tasks)
  ✅ Retry-safe: always resets busy flag
  ✅ /history endpoint for dashboard

Run 3 separate terminals:
  python worker.py 8001
  python worker.py 8002
  python worker.py 8003
"""

from flask import Flask, request, jsonify
import time
import sys
import psutil
import collections

app = Flask(__name__)

PORT      = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
NODE_NAME = f"Worker-{PORT}"

# ── State ──────────────────────────────────────────────────────────────────
busy         = False
task_history = collections.deque(maxlen=10)   # ring buffer — last 10 tasks
CPU_ANOMALY_THRESHOLD = 85.0                  # % — flag if CPU spikes above this
RAM_ANOMALY_THRESHOLD = 90.0
total_tasks_done = 0


# ══════════════════════════════════════════════════════════════════════════
# /status  — Heartbeat + live metrics
# ══════════════════════════════════════════════════════════════════════════
@app.route('/status', methods=['GET'])
def status():
    cpu = psutil.cpu_percent(interval=0.1)
    ram = psutil.virtual_memory().percent

    anomaly = cpu > CPU_ANOMALY_THRESHOLD or ram > RAM_ANOMALY_THRESHOLD

    return jsonify({
        "node":             NODE_NAME,
        "port":             PORT,
        "status":           "busy" if busy else "free",
        "cpu":              cpu,
        "ram":              ram,
        "anomaly":          anomaly,          # NEW: flag for dashboard warning
        "total_tasks_done": total_tasks_done, # NEW: lifetime counter
    })


# ══════════════════════════════════════════════════════════════════════════
# /process  — Do the actual work
# ══════════════════════════════════════════════════════════════════════════
@app.route('/process', methods=['POST'])
def process():
    global busy, total_tasks_done
    busy = True
    cpu_before = psutil.cpu_percent(interval=0.1)
    ram_before = psutil.virtual_memory().percent

    try:
        payload = request.json
        data    = payload.get("data", [])
        task    = payload.get("task", "sort")

        print(f"\n[{NODE_NAME}] task='{task}', {len(data)} items")
        start = time.time()

        # ── Task execution ─────────────────────────────────────────────
        if task == "sort":
            result = sorted(data)

        elif task == "sum":
            result = sum(data)

        elif task == "square":
            result = [x ** 2 for x in data]

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

        else:
            result = data   # passthrough

        elapsed    = round(time.time() - start, 4)
        cpu_after  = psutil.cpu_percent(interval=0.1)
        ram_after  = psutil.virtual_memory().percent
        total_tasks_done += 1

        # ── Log to history ─────────────────────────────────────────────
        log_entry = {
            "task":       task,
            "items":      len(data),
            "time_taken": elapsed,
            "cpu_before": cpu_before,
            "cpu_after":  cpu_after,
            "ram_before": ram_before,
            "ram_after":  ram_after,
            "anomaly":    cpu_after > CPU_ANOMALY_THRESHOLD,
            "timestamp":  time.strftime("%H:%M:%S"),
        }
        task_history.append(log_entry)

        print(f"[{NODE_NAME}] Done in {elapsed}s | CPU {cpu_before}→{cpu_after}%")

        return jsonify({
            "result":     result,
            "node":       NODE_NAME,
            "task":       task,
            "time_taken": elapsed,
            "items_in":   len(data),
            "cpu_before": cpu_before,
            "cpu_after":  cpu_after,
            "ram_before": ram_before,
            "ram_after":  ram_after,
            "anomaly":    cpu_after > CPU_ANOMALY_THRESHOLD,
            "status":     "done",
        })

    except Exception as e:
        print(f"[{NODE_NAME}] ERROR: {e}")
        return jsonify({
            "result":     [],
            "node":       NODE_NAME,
            "status":     "error",
            "error":      str(e),
            "time_taken": 0,
        }), 500

    finally:
        busy = False   # ALWAYS free the node


# ══════════════════════════════════════════════════════════════════════════
# /history  — Last 10 tasks (for dashboard timeline)
# ══════════════════════════════════════════════════════════════════════════
@app.route('/history', methods=['GET'])
def history():
    return jsonify({
        "node":    NODE_NAME,
        "history": list(task_history),
    })


# ── Run ────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print(f"\n{'='*45}")
    print(f"  {NODE_NAME}  starting on  port {PORT}")
    print(f"{'='*45}\n")
    app.run(host='0.0.0.0', port=PORT, debug=False)
