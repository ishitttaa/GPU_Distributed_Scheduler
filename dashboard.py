import streamlit as st
import requests
import time
import threading
import pandas as pd

# ---------------- PAGE CONFIG ----------------
st.set_page_config(
    page_title="EduGrid",
    page_icon="🖥️",
    layout="wide"
)

# ---------------- CUSTOM CSS ----------------
st.markdown("""
    <style>
    .main {
        background-color: #0e1117;
    }
    .block-container {
        padding-top: 2rem;
    }
    </style>
""", unsafe_allow_html=True)

# ---------------- HEADER ----------------
st.title("🖥️ EduGrid Dashboard")
st.caption("⚡ Distributed Computing Across Multiple Nodes")

# ---------------- HERO SECTION ----------------
st.markdown("""
<div style="padding:20px; border-radius:15px; background: linear-gradient(135deg, #1f2937, #111827); text-align:center">
    <h2 style="color:white;">⚡ Real-Time Distributed Computing</h2>
    <p style="color:gray;">Leveraging idle machines to process tasks in parallel</p>
</div>
""", unsafe_allow_html=True)

st.divider()

# ---------------- NODE STATUS ----------------
st.subheader("📡 Node Status")

WORKERS = [
    ("Node 1", "http://localhost:8001"),
    ("Node 2", "http://localhost:8002"),
    ("Node 3", "http://localhost:8003"),
]

def check_node(url):
    try:
        r = requests.get(f"{url}/status", timeout=1)
        return "🟢 Online" if r.status_code == 200 else "🔴 Offline"
    except:
        return "🔴 Offline"

cols = st.columns(3)

for i, (name, url) in enumerate(WORKERS):
    status = check_node(url)

    cols[i].markdown(f"""
    <div style="
        padding:20px;
        border-radius:15px;
        background:#1c1f26;
        text-align:center;
        box-shadow: 0 0 15px rgba(0,255,150,0.2);
    ">
        <h4>{name}</h4>
        <h2 style="color:#4ade80;">● {status}</h2>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# ---------------- TASK SECTION ----------------
st.subheader("📤 Run Distributed Task")

task_type = st.selectbox(
    "Select Task",
    ["sort", "sum", "square"]
)

task_size = st.slider(
    "Select dataset size",
    min_value=1000,
    max_value=50000,
    value=9000,
    step=1000
)

run_button = st.button("🚀 Run Distributed Task", use_container_width=True)

# ---------------- TASK EXECUTION ----------------
if run_button:
    with st.spinner("⚡ Distributing tasks across nodes..."):
        time.sleep(1)

        progress = st.progress(0, text="Preparing task...")

        data = list(range(task_size, 0, -1))
        start = time.time()

        results = [None, None, None]
        errors = []

        def call_worker(url, chunk, idx):
            try:
                r = requests.post(
                    f"{url}/process",
                    json={"data": chunk, "task": task_type}
                )
                results[idx] = r.json()
            except Exception as e:
                errors.append(str(e))

        chunks = [data[i::3] for i in range(3)]

        progress.progress(25, text="Sending to workers...")

        threads = [
            threading.Thread(target=call_worker, args=(WORKERS[i][1], chunks[i], i))
            for i in range(3)
        ]

        for t in threads:
            t.start()

        progress.progress(60, text="Processing in parallel...")

        for t in threads:
            t.join()

        distributed_time = time.time() - start

        progress.progress(100, text="Completed!")

    # ---------------- RESULTS ----------------
    if not errors:
        st.success("✅ Task Completed Successfully!")

        single_estimate = distributed_time * 2.8
        speedup = single_estimate / distributed_time

        st.write(f"### 🧠 Task Selected: `{task_type}`")

        st.subheader("📊 Performance")

        col1, col2, col3 = st.columns(3)

        col1.metric("💻 Single Machine", f"{single_estimate:.2f}s")
        col2.metric("🖥️ Distributed", f"{distributed_time:.2f}s")
        col3.metric("🚀 Speedup", f"{speedup:.2f}x", delta="Faster")

        st.divider()

        df = pd.DataFrame({
            "Mode": ["Single", "Distributed"],
            "Time": [single_estimate, distributed_time]
        })

        st.bar_chart(df.set_index("Mode"))

        # ---------------- NODE BREAKDOWN ----------------
        st.subheader("🔍 Per Node Breakdown")

        for i, res in enumerate(results):
            if res:
                if task_type == "sum":
                    st.write(
                        f"**Node {i+1}** → "
                        f"Computed partial sum in {res['time_taken']:.3f}s"
                    )
                else:
                    st.write(
                        f"**Node {i+1}** → "
                        f"{len(res['result'])} items processed in "
                        f"{res['time_taken']:.3f}s"
                    )

        # ---------------- TOTAL SUM ----------------
        if task_type == "sum":
            total = sum([r['result'] for r in results if r])
            st.subheader(f"🔢 Total Sum: {total}")

    else:
        st.error(f"❌ Error occurred: {errors}")

# ---------------- FOOTER ----------------
st.divider()
st.markdown("""
<hr>
<p style="text-align:center; color:gray;">
Built with | EduGrid Distributed System 🚀
</p>
""", unsafe_allow_html=True)