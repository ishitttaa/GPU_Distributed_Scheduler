"""
EduGrid - Streamlit Dashboard
==============================
A clean web UI that lets you:
  • See all worker nodes and their live status
  • Submit a task and watch it get distributed
  • Run a benchmark and compare speeds

Run:
  streamlit run dashboard.py
"""

import streamlit as st
import requests
import time
import json
import random
import pandas as pd

API = "http://localhost:9000"   # where api_gateway.py is running

st.set_page_config(
    page_title="EduGrid",
    page_icon="⚡",
    layout="wide",
)

# ── Header ─────────────────────────────────────────────────────────────────
st.title("⚡ EduGrid — Distributed Computing Dashboard")
st.caption("Thapar Institute of Engineering & Technology  ·  Team SHE4  ·  Eclipse 6.0")
st.divider()


# ══════════════════════════════════════════════════════════════════════════
# Section 1 — Live Worker Status
# ══════════════════════════════════════════════════════════════════════════
st.subheader("🖥️ Worker Nodes")

try:
    resp    = requests.get(f"{API}/workers", timeout=3)
    workers = resp.json().get("workers", [])
except Exception:
    st.error("Cannot reach API gateway. Is api_gateway.py running?")
    workers = []

cols = st.columns(len(workers) if workers else 1)
for col, w in zip(cols, workers):
    with col:
        status = w.get("status", "offline")
        color  = "🟢" if status == "free" else ("🟡" if status == "busy" else "🔴")
        st.metric(
            label=f"{color} {w.get('node', '?')}",
            value=status.upper(),
            delta=f"CPU {w.get('cpu', 0):.1f}%  |  RAM {w.get('ram', 0):.1f}%",
        )

st.divider()


# ══════════════════════════════════════════════════════════════════════════
# Section 2 — Submit a Task
# ══════════════════════════════════════════════════════════════════════════
st.subheader("📤 Submit a Task")

c1, c2, c3 = st.columns([2, 1, 1])

with c1:
    task = st.selectbox(
        "Task type",
        options=["sort", "sum", "square", "filter_even"],
        format_func=lambda x: {
            "sort":        "Sort (merge sort across workers)",
            "sum":         "Sum (split and add partial sums)",
            "square":      "Square (ML-style preprocessing)",
            "filter_even": "Filter even numbers",
        }[x],
    )

with c2:
    n_items = st.slider("Number of items", min_value=10, max_value=1000, value=200, step=10)

with c3:
    st.write("")   # spacer
    st.write("")
    submit = st.button("🚀 Run Distributed Task", use_container_width=True)

if submit:
    data = random.sample(range(1, 10_001), n_items)
    st.write(f"**Input (first 10):** `{data[:10]} …`")

    with st.spinner("Distributing task across worker nodes…"):
        try:
            t0   = time.time()
            resp = requests.post(f"{API}/run", json={"data": data, "task": task}, timeout=30)
            wall = round(time.time() - t0, 3)
            out  = resp.json()
        except Exception as e:
            st.error(f"Error: {e}")
            out = {}

    if out and "error" not in out:
        st.success(f"✅ Completed in **{out.get('total_time', wall)}s** using **{out.get('workers_used', '?')} workers**")

        # Show per-node breakdown
        per_node = out.get("per_node", {})
        if per_node:
            rows = []
            for node, info in per_node.items():
                rows.append({
                    "Node":      node,
                    "Status":    info.get("status", "?"),
                    "Items in":  info.get("items_in", "?"),
                    "Time (s)":  info.get("time_taken", "?"),
                })
            st.table(pd.DataFrame(rows))

        result = out.get("result", [])
        if isinstance(result, list):
            st.write(f"**Result (first 10):** `{result[:10]} …`")
        else:
            st.write(f"**Result:** `{result}`")

    elif "error" in out:
        st.error(f"Master says: {out['error']}")

st.divider()


# ══════════════════════════════════════════════════════════════════════════
# Section 3 — Benchmark: Distributed vs Single Machine
# ══════════════════════════════════════════════════════════════════════════
st.subheader("📊 Benchmark: EduGrid vs Single Machine")

b_col1, b_col2 = st.columns([1, 1])
with b_col1:
    b_n    = st.slider("Data size", 50, 2000, 500, 50, key="bn")
with b_col2:
    b_task = st.selectbox("Task", ["sort", "sum", "square", "filter_even"], key="bt")

if st.button("⚡ Run Benchmark", use_container_width=True):
    with st.spinner("Running benchmark…"):
        try:
            r   = requests.get(f"{API}/benchmark", params={"n": b_n, "task": b_task}, timeout=60)
            bm  = r.json()
        except Exception as e:
            st.error(f"Benchmark error: {e}")
            bm = {}

    if bm:
        m1, m2, m3 = st.columns(3)
        m1.metric("Distributed (EduGrid)", f"{bm['distributed_s']}s")
        m2.metric("Single machine",        f"{bm['single_s']}s")
        m3.metric("Speedup",               f"{bm['speedup']}×")

        df = pd.DataFrame({
            "Method": ["Single machine", "EduGrid (distributed)"],
            "Time (s)": [bm["single_s"], bm["distributed_s"]],
        })
        st.bar_chart(df.set_index("Method"))

        st.caption(
            f"Data size: {bm['n']} items  ·  "
            f"Task: {bm['task']}  ·  "
            f"Workers: {bm['workers_used']}"
        )

st.divider()
st.caption("EduGrid — Eclipse 6.0 Hackathon  ·  PS ID: EC601")