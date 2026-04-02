"""
EduGrid - Worker Node
=====================
Each worker is a "chef" — it receives a chunk of data,
processes it, and sends back the result.

Run 3 separate terminals:
  python worker.py 8001
  python worker.py 8002
  python worker.py 8003
"""

from flask import Flask, request, jsonify
import time
import sys
import os
import psutil   # pip install psutil  (for real CPU/RAM stats)

app = Flask(__name__)

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
NODE_NAME = f"Worker-{PORT}"

# ── Track whether this node is currently busy ──────────────────────────────
busy = False


# ── /status  (Heartbeat) ───────────────────────────────────────────────────
# Master pings this every few seconds to know if the worker is alive/free.
@app.route('/status', methods=['GET'])
def status():
    cpu  = psutil.cpu_percent(interval=0.1)
    ram  = psutil.virtual_memory().percent
    return jsonify({
        "node":   NODE_NAME,
        "port":   PORT,
        "status": "busy" if busy else "free",
        "cpu":    cpu,
        "ram":    ram,
    })


# ── /process  (Do the actual work) ────────────────────────────────────────
# Master sends a JSON body: { "data": [...], "task": "sort" }
@app.route('/process', methods=['POST'])
def process():
    global busy
    busy = True
    try:
        payload  = request.json
        data     = payload.get("data", [])
        task     = payload.get("task", "sort")

        print(f"\n[{NODE_NAME}] Received task='{task}', {len(data)} items")
        start = time.time()

        # ── Supported task types ─────────────────────────────────────────
        if task == "sort":
            result = sorted(data)

        elif task == "sum":
            result = sum(data)

        elif task == "square":          # simulate ML preprocessing
            result = [x ** 2 for x in data]

        elif task == "filter_even":
            result = [x for x in data if x % 2 == 0]

        else:
            result = data               # passthrough for unknown tasks

        elapsed = time.time() - start
        print(f"[{NODE_NAME}] Done in {elapsed:.4f}s  →  {str(result)[:60]}")

        return jsonify({
            "result":     result,
            "node":       NODE_NAME,
            "task":       task,
            "time_taken": round(elapsed, 4),
            "items_in":   len(data),
            "status":     "done",
        })

    finally:
        busy = False   # always free the node, even if an error occurred


# ── Run ────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print(f"\n{'='*45}")
    print(f"  {NODE_NAME}  starting on  port {PORT}")
    print(f"{'='*45}\n")
    app.run(host='0.0.0.0', port=PORT, debug=False)