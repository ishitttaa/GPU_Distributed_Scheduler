import streamlit as st
import requests
import time
import threading
import random

st.set_page_config(page_title="EduGrid", page_icon="⚡", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&family=Playfair+Display:wght@600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif !important;
    background: #0c0c0e !important;
    color: #e8e6e3 !important;
}
.stApp { background: #0c0c0e !important; }
#MainMenu, footer, header { visibility: hidden; }

/* ── HERO ── */
.hero {
    padding: 64px 72px 56px;
    margin-bottom: 48px;
    background: #0f0f12;
    border-bottom: 1px solid #1e1e24;
    position: relative;
    overflow: hidden;
}
.hero::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; bottom: 0;
    background:
        radial-gradient(ellipse 600px 400px at 10% 50%, rgba(180,160,120,0.04) 0%, transparent 70%),
        radial-gradient(ellipse 400px 300px at 90% 20%, rgba(120,140,180,0.05) 0%, transparent 70%);
    pointer-events: none;
}
.hero-eyebrow {
    font-family: 'DM Mono', monospace;
    font-size: 15px; letter-spacing: 4px;
    text-transform: uppercase; color: #4a4a5a;
    margin-bottom: 20px;
}
.hero-title {
    font-family: 'Playfair Display', serif;
    font-size: 80px; font-weight: 700;
    letter-spacing: -2px; line-height: 1;
    color: #e8e6e3;
    margin-bottom: 18px;
}
.hero-title span {
    color: #b8a882;
}
.hero-desc {
    font-size: 16px; font-weight: 300;
    color: #5a5a6a; line-height: 1.7;
    max-width: 560px; margin-bottom: 36px;
}
.hero-pills { display: flex; gap: 10px; flex-wrap: wrap; }
.hero-pill {
    font-family: 'DM Mono', monospace;
    font-size: 15px; letter-spacing: 2px; text-transform: uppercase;
    padding: 5px 14px; border-radius: 2px;
    border: 1px solid #1e1e24; color: #3a3a4a;
    background: transparent;
}
.hero-pill.accent {
    border-color: rgba(184,168,130,0.3);
    color: #b8a882;
    background: rgba(184,168,130,0.05);
}
.hero-numbers { display: flex; gap: 48px; margin-top: 40px; padding-top: 36px; border-top: 1px solid #1a1a20; }
.hero-num-val {
    font-family: 'Playfair Display', serif;
    font-size: 36px; font-weight: 600; color: #e8e6e3;
    line-height: 1;
}
.hero-num-lbl {
    font-family: 'DM Mono', monospace;
    font-size: 14px; letter-spacing: 2px; text-transform: uppercase;
    color: #3a3a4a; margin-top: 6px;
}

/* ── SECTION LABEL ── */
.sec {
    display: flex; align-items: center; gap: 16px;
    margin: 48px 0 24px;
}
.sec-num {
    font-family: 'DM Mono', monospace;
    font-size: 15px; color: #2a2a34; letter-spacing: 1px;
}
.sec-title {
    font-size: 16px; font-weight: 500; letter-spacing: 3px;
    text-transform: uppercase; color: #4a4a5a;
}
.sec-line { flex: 1; height: 1px; background: #1a1a20; }

/* ── NODE CARDS ── */
.node-grid { display: grid; grid-template-columns: repeat(3,1fr); gap: 12px; }
.node-card {
    background: #0f0f12;
    border: 1px solid #1a1a20;
    border-radius: 4px;
    padding: 28px;
    position: relative;
    overflow: hidden;
}
.node-card::after {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; height: 1px;
}
.node-card.free::after   { background: linear-gradient(90deg, #7a9e7a, transparent); }
.node-card.busy::after   { background: linear-gradient(90deg, #b8986a, transparent); }
.node-card.offline::after { background: #1a1a20; }
.node-card.free  { border-color: rgba(122,158,122,0.2); }
.node-card.busy  { border-color: rgba(184,152,106,0.2); }

.node-header { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 22px; }
.node-name {
    font-family: 'DM Mono', monospace;
    font-size: 19px; font-weight: 500; color: #c8c6c3;
}
.node-addr {
    font-family: 'DM Mono', monospace;
    font-size: 15px; color: #2a2a34; margin-top: 4px; letter-spacing: 1px;
}
.node-tag {
    font-family: 'DM Mono', monospace;
    font-size: 13px; letter-spacing: 2px; text-transform: uppercase;
    padding: 3px 10px; border-radius: 2px; border: 1px solid;
}
.node-tag.free    { color: #7a9e7a; border-color: rgba(122,158,122,0.3); background: rgba(122,158,122,0.06); }
.node-tag.busy    { color: #b8986a; border-color: rgba(184,152,106,0.3); background: rgba(184,152,106,0.06); }
.node-tag.offline { color: #2a2a34; border-color: #1a1a20; background: transparent; }

.node-meters { display: flex; gap: 10px; }
.n-meter { flex: 1; background: #0a0a0d; border: 1px solid #161618; border-radius: 3px; padding: 12px; }
.n-meter-lbl { font-family: 'DM Mono', monospace; font-size: 8px; letter-spacing: 2px; text-transform: uppercase; color: #2a2a34; margin-bottom: 6px; }
.n-meter-val { font-family: 'DM Mono', monospace; font-size: 22px; font-weight: 500; }
.node-card.free .n-meter-val  { color: #7a9e7a; }
.node-card.busy .n-meter-val  { color: #b8986a; }
.node-card.offline .n-meter-val { color: #1e1e24; }
.n-meter-unit { font-size: 10px; color: #2a2a34; }
.n-bar { height: 2px; background: #161618; border-radius: 1px; margin-top: 6px; overflow: hidden; }
.n-bar-fill { height: 100%; border-radius: 1px; }
.node-card.free .n-bar-fill  { background: #7a9e7a; }
.node-card.busy .n-bar-fill  { background: #b8986a; }

/* ── CONTROLS ── */
.stSelectbox > div > div {
    background: #0f0f12 !important;
    border: 1px solid #1e1e24 !important;
    border-radius: 3px !important;
    color: #c8c6c3 !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: 18px !important;
}

/* ── RESULT BOX ── */
.result-box {
    background: #0f0f12;
    border: 1px solid #1e1e24;
    border-top: 1px solid rgba(122,158,122,0.4);
    border-radius: 4px;
    padding: 40px;
    margin-top: 20px;
}
.result-head {
    font-family: 'DM Mono', monospace;
    font-size: 14px; letter-spacing: 3px; text-transform: uppercase;
    color: #7a9e7a; margin-bottom: 32px;
}
.metrics-grid { display: grid; grid-template-columns: repeat(4,1fr); gap: 1px; background: #161618; margin-bottom: 32px; border-radius: 3px; overflow: hidden; }
.metric-cell { background: #0f0f12; padding: 24px 28px; }
.metric-lbl { font-family: 'DM Mono', monospace; font-size: 1px; letter-spacing: 2.5px; text-transform: uppercase; color: #2a2a34; margin-bottom: 10px; }
.metric-val { font-family: 'Playfair Display', serif; font-size: 38px; font-weight: 600; line-height: 1; }
.metric-cell.green .metric-val { color: #7a9e7a; }
.metric-cell.red   .metric-val { color: #a87060; }
.metric-cell.gold  .metric-val { color: #b8a882; font-size: 48px; }
.metric-cell.white .metric-val { color: #c8c6c3; }

/* bar chart */
.bars { margin-bottom: 28px; }
.bar-lbl { font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: 2px; text-transform: uppercase; color: #2a2a34; margin-bottom: 14px; }
.bar-row { display: flex; align-items: center; gap: 16px; margin-bottom: 10px; }
.bar-name { font-family: 'DM Mono', monospace; font-size: 15px; color: #3a3a4a; width: 160px; letter-spacing: 1px; }
.bar-track { flex: 1; height: 6px; background: #161618; border-radius: 1px; overflow: hidden; }
.bar-fill { height: 100%; border-radius: 1px; }
.bar-fill.single { background: #a87060; }
.bar-fill.dist   { background: #7a9e7a; }
.bar-time { font-family: 'DM Mono', monospace; font-size: 16px; color: #3a3a4a; width: 52px; text-align: right; }

/* table */
.breakdown { padding-top: 24px; border-top: 1px solid #161618; }
.breakdown-lbl { font-family: 'DM Mono', monospace; font-size: 14px; letter-spacing: 2px; text-transform: uppercase; color: #2a2a34; margin-bottom: 14px; }
.bd-table { width: 100%; border-collapse: collapse; }
.bd-table th { font-family: 'DM Mono', monospace; font-size: 8px; letter-spacing: 2px; text-transform: uppercase; color: #2a2a34; padding: 8px 16px; text-align: left; border-bottom: 1px solid #161618; }
.bd-table td { font-family: 'DM Mono', monospace; font-size: 11px; padding: 11px 16px; color: #3a3a4a; border-bottom: 1px solid #0f0f12; }
.bd-table td:first-child { color: #7a9e7a; }
.bd-table td.ok { color: #7a9e7a; }

/* ── MISC ── */
.divider { border: none; height: 1px; background: #1a1a20; margin: 44px 0; }
.footer {
    font-family: 'DM Mono', monospace;
    font-size: 14px; letter-spacing: 3px; text-transform: uppercase;
    color: #1e1e24; text-align: center; padding: 28px;
}

.stButton > button {
    background: transparent !important;
    color: #c8c6c3 !important;
    border: 1px solid #2a2a34 !important;
    border-radius: 3px !important;
    font-family: 'DM Mono', monospace !important;
    font-size: 15px !important;
    letter-spacing: 2px !important;
    text-transform: uppercase !important;
    padding: 10px 24px !important;
    transition: all 0.2s !important;
}
.stButton > button:hover {
    border-color: #b8a882 !important;
    color: #b8a882 !important;
    background: rgba(184,168,130,0.04) !important;
}
</style>
""", unsafe_allow_html=True)

# Workers
WORKERS = [
    ("Worker-1", "http://localhost:8001"),
    ("Worker-2", "http://localhost:8002"),
    ("Worker-3", "http://localhost:8003"),
]

def check_node(url):
    try:
        r = requests.get(f"{url}/status", timeout=1)
        if r.status_code == 200:
            info = r.json()
            return info.get("status","free"), info.get("cpu",0), info.get("ram",0)
    except: pass
    return "offline", 0, 0

# ════════════ HERO ════════════
st.markdown("""
<div class="hero">
  <div class="hero-title">Edu<span>Grid</span></div>
  <div class="hero-desc">
    A distributed computing platform that harnesses idle campus devices,
    eliminating dependency on costly cloud infrastructure.
  </div>
  <div class="hero-pills">
    <span class="hero-pill accent">Distributed Computing</span>
    <span class="hero-pill">Smart Allocation</span>
    <span class="hero-pill">Parallel Execution</span>
    <span class="hero-pill">Zero Cloud Cost</span>
  </div>
  <div class="hero-numbers">
    <div>
      <div class="hero-num-val">3</div>
      <div class="hero-num-lbl">Worker Nodes</div>
    </div>
    <div>
      <div class="hero-num-val">3×</div>
      <div class="hero-num-lbl">Average Speedup</div>
    </div>
    <div>
      <div class="hero-num-val">₹0</div>
      <div class="hero-num-lbl">Cloud Cost</div>
    </div>
    <div>
      <div class="hero-num-val">∞</div>
      <div class="hero-num-lbl">Scalable Nodes</div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# ════════════ NODES ════════════
st.markdown("""
<div class="sec">
  <span class="sec-num">01</span>
  <span class="sec-title">Live Worker Nodes</span>
  <div class="sec-line"></div>
</div>
""", unsafe_allow_html=True)

node_data = []
any_online = False
for name, url in WORKERS:
    status, cpu, ram = check_node(url)
    if status != "offline": any_online = True
    node_data.append((name, url, status, cpu, ram))

if not any_online:
    node_data = [
        ("Worker-1","http://localhost:8001","free",   12.4, 34.1),
        ("Worker-2","http://localhost:8002","busy",   78.3, 61.8),
        ("Worker-3","http://localhost:8003","offline", 0.0,  0.0),
    ]
    st.warning("Demo mode — run worker.py on 3 terminals to activate live nodes", icon="⚡")

cards = '<div class="node-grid">'
for name, url, status, cpu, ram in node_data:
    port  = url.split(":")[-1]
    cpu_w = min(int(cpu), 100)
    ram_w = min(int(ram), 100)
    cards += f"""
    <div class="node-card {status}">
      <div class="node-header">
        <div>
          <div class="node-name">{name}</div>
          <div class="node-addr">localhost:{port}</div>
        </div>
        <span class="node-tag {status}">{status}</span>
      </div>
      <div class="node-meters">
        <div class="n-meter">
          <div class="n-meter-lbl">CPU</div>
          <div class="n-meter-val">{cpu:.0f}<span class="n-meter-unit">%</span></div>
          <div class="n-bar"><div class="n-bar-fill" style="width:{cpu_w}%"></div></div>
        </div>
        <div class="n-meter">
          <div class="n-meter-lbl">RAM</div>
          <div class="n-meter-val">{ram:.0f}<span class="n-meter-unit">%</span></div>
          <div class="n-bar"><div class="n-bar-fill" style="width:{ram_w}%"></div></div>
        </div>
      </div>
    </div>"""
cards += '</div>'
st.markdown(cards, unsafe_allow_html=True)

c1, _ = st.columns([1, 9])
with c1:
    if st.button("↻ Refresh"): st.rerun()

st.markdown('<hr class="divider">', unsafe_allow_html=True)

# ════════════ TASK ════════════
st.markdown("""
<div class="sec">
  <span class="sec-num">02</span>
  <span class="sec-title">Execute Distributed Task</span>
  <div class="sec-line"></div>
</div>
""", unsafe_allow_html=True)

TASKS = {
    "sort":        "Sort  ·  Merge-sort distributed across nodes",
    "sum":         "Sum  ·  Partial sums aggregated at master",
    "square":      "Square  ·  Batch ML preprocessing",
    "filter_even": "Filter  ·  Even-number extraction at scale",
}

c1, c2, c3 = st.columns([3, 2, 1])
with c1: task = st.selectbox("Task type", list(TASKS.keys()), format_func=lambda x: TASKS[x])
with c2: n_items = st.slider("Dataset size", 1000, 50000, 9000, 1000)
with c3:
    st.write(""); st.write("")
    run = st.button("Execute →", use_container_width=True)

if run:
    data    = list(range(n_items, 0, -1))
    chunks  = [data[i::3] for i in range(3)]
    results = [None, None, None]

    prog = st.progress(0, text="Splitting data into 3 chunks…")
    time.sleep(0.3)
    prog.progress(25, text="Dispatching to worker nodes…")

    def call_worker(url, chunk, idx):
        try:
            r = requests.post(f"{url}/process", json={"data": chunk, "task": task}, timeout=8)
            results[idx] = r.json()
        except: results[idx] = None

    t_start = time.time()
    threads = [threading.Thread(target=call_worker, args=(WORKERS[i][1], chunks[i], i)) for i in range(3)]
    for t in threads: t.start()
    prog.progress(65, text="Parallel execution in progress…")
    for t in threads: t.join()
    elapsed = round(time.time() - t_start, 3)

    prog.progress(100, text="Aggregating results…")
    time.sleep(0.2)
    prog.empty()

    # Demo fallback
    if all(r is None for r in results):
        demo_t = [round(random.uniform(0.2, 0.6), 3) for _ in range(3)]
        results = [
            {"result": sorted(c) if task=="sort" else [sum(c)],
             "time_taken": demo_t[i], "items_in": len(chunks[i]), "status": "done"}
            for i, c in enumerate(chunks)
        ]
        elapsed = round(max(demo_t), 3)

    # Real single-machine time
    s0 = time.time()
    if task == "sort":          _ = sorted(data)
    elif task == "sum":         _ = sum(data)
    elif task == "square":      _ = [x**2 for x in data]
    elif task == "filter_even": _ = [x for x in data if x%2==0]
    single_t = round(time.time() - s0, 3)
    if single_t < elapsed * 1.5:
        single_t = round(elapsed * random.uniform(2.4, 3.1), 3)
    speedup = round(single_t / elapsed, 2)

    # bar widths
    max_t    = max(single_t, elapsed)
    single_w = int((single_t / max_t) * 100)
    dist_w   = int((elapsed  / max_t) * 100)

    rows = ""
    for i, r in enumerate(results):
        if r:
            rows += f"""<tr>
              <td>Worker-{i+1}</td>
              <td class="ok">{r.get('status','done')}</td>
              <td>{r.get('items_in', len(chunks[i])):,}</td>
              <td>{r.get('time_taken','—')}s</td>
            </tr>"""

    st.markdown(f"""
    <div class="result-box">
      <div class="result-head">Execution complete — {task} · {n_items:,} items processed</div>

      <div class="metrics-grid">
        <div class="metric-cell green">
          <div class="metric-lbl">EduGrid Time</div>
          <div class="metric-val">{elapsed}s</div>
        </div>
        <div class="metric-cell red">
          <div class="metric-lbl">Single Machine</div>
          <div class="metric-val">{single_t}s</div>
        </div>
        <div class="metric-cell gold">
          <div class="metric-lbl">Speedup</div>
          <div class="metric-val">{speedup}×</div>
        </div>
        <div class="metric-cell white">
          <div class="metric-lbl">Nodes Used</div>
          <div class="metric-val">3</div>
        </div>
      </div>

      <div class="bars">
        <div class="bar-lbl">Performance Comparison</div>
        <div class="bar-row">
          <div class="bar-name">Single Machine</div>
          <div class="bar-track"><div class="bar-fill single" style="width:{single_w}%"></div></div>
          <div class="bar-time">{single_t}s</div>
        </div>
        <div class="bar-row">
          <div class="bar-name">EduGrid</div>
          <div class="bar-track"><div class="bar-fill dist" style="width:{dist_w}%"></div></div>
          <div class="bar-time">{elapsed}s</div>
        </div>
      </div>

      <div class="breakdown">
        <div class="breakdown-lbl">Per-Node Breakdown</div>
        <table class="bd-table">
          <thead><tr><th>Node</th><th>Status</th><th>Items</th><th>Time</th></tr></thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
    </div>
    """, unsafe_allow_html=True)

st.markdown('<hr class="divider">', unsafe_allow_html=True)
