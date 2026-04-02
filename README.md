# 🖥️ EduGrid — Distributed Computing for Smart Resource Utilization

EduGrid is a distributed computing system that utilizes idle machines to perform large computational tasks efficiently using parallel processing.

---

## 🚀 Problem Statement

Students require high computational power for:

* Machine Learning
* Simulations
* Data processing

However:

* Personal devices lack performance
* Cloud solutions are expensive
* Many systems remain idle

---

## 💡 Solution

EduGrid connects multiple machines and distributes workloads across them to:

* Utilize idle resources
* Execute tasks in parallel
* Reduce computation time

---

## 🏗️ Architecture

* **Master Node** → Task scheduling & distribution
* **Worker Nodes** → Task execution
* **REST APIs** → Communication

---

## ⚙️ Tech Stack

* Python
* Flask
* FastAPI
* Streamlit
* Requests

---

## 🔄 Workflow

1. Load dataset
2. Split into chunks
3. Assign to worker nodes
4. Parallel processing
5. Aggregate results

---

## 📊 Features

* Parallel task execution
* Dynamic task splitting
* Multi-node simulation (different ports)
* Real-time node communication
* Distributed vs single-machine comparison

---

## ▶️ How to Run

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Start worker nodes (3 terminals)

```bash
python worker.py 8001
python worker.py 8002
python worker.py 8003
```

### 3. Run master node

```bash
python master.py
```

### 4. (Optional) Run dashboard

```bash
python -m streamlit run dashboard.py
```

---

## 📈 Example Output

* Distributed sorting and summation
* Speed improvement via parallel execution
* Correctness validation (distributed = single machine)

---

## 🔮 Future Scope

* ML model distribution
* GPU support
* Multi-device network scaling
* Fault tolerance & security

---

## 👩‍💻 Team

* Ishita Rajput
* Manya Bansal
* Tripti
* Arushi Bassi

---

## 🌟 Conclusion

EduGrid provides a scalable, cost-effective alternative to traditional cloud computing by leveraging unused computing power across devices.
