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

## Features
- Master-worker distributed architecture
- GPU-aware task scheduling
- CPU and RAM resource monitoring using psutil
- Multi-node distributed task execution
- Web-based dashboard for task monitoring and node status

## Tech Stack
- Python
- Flask (backend APIs)
- HTML, CSS, JavaScript (frontend dashboard)
- psutil (system resource monitoring)

## Notes
This project was developed during a hackathon as part of a team.