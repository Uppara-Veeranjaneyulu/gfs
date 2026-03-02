# GFS-Inspired Distributed File Storage System

> **Academic simulation of Google File System (GFS)** demonstrating chunking, replication, fault detection, and auto-recovery using a Master–Client–Chunk Server architecture — implemented entirely in Python 3.

---

## Architecture

```
┌────────────────────────────────────────────────────────┐
│                        CLIENT                          │
│  upload(file) ──► splits into 64 KB chunks             │
│  download(file) ──► reassembles from chunk servers     │
└────────────────────┬───────────────────────────────────┘
       metadata only │ (no data passes through master)
┌────────────────────▼───────────────────────────────────┐
│                       MASTER NODE                      │
│  • file → [chunk_id, ...]           (Algorithm 2)      │
│  • chunk_id → [server_id, ...]      (Algorithm 2)      │
│  • Chunk placement (round-robin)    (Algorithm 1)      │
│  • Lease / primary selection        (Algorithm 3)      │
│  • Heartbeat monitor                (Algorithm 4)      │
│  • Fault recovery & re-replication  (Algorithm 5)      │
└────────────────────┬───────────────────────────────────┘
     data read/write │
┌────────────────────▼───────────────────────────────────┐
│           CHUNK SERVERS  (cs0 … cs4)                   │
│  gfs_storage/server_cs0/chunk_<id>.bin                 │
│  gfs_storage/server_cs1/chunk_<id>.bin  ← replicas    │
│  gfs_storage/server_cs2/chunk_<id>.bin                 │
└────────────────────────────────────────────────────────┘
```

---

## Algorithms Implemented

| # | Algorithm | File |
|---|-----------|------|
| 1 | **Chunk Placement & Replication** — split files into 64 KB chunks, replicate to 3 servers | `client.py` + `master.py` |
| 2 | **Master-Based Metadata Management** — file→chunks, chunk→servers | `master.py` |
| 3 | **Lease-Based Consistency** — primary writes first, secondaries follow | `client.py` + `master.py` |
| 4 | **Heartbeat Fault Detection** — background monitor, 6 s timeout | `master.py` + `chunk_server.py` |
| 5 | **Fault Recovery & Re-Replication** — detects under-replicated chunks, copies to new servers | `master.py` |

---

## File Structure

```
gfs-ds/
├── config.py          # Shared constants
├── utils.py           # Logging + SHA-256 helpers
├── master.py          # Master node (all 5 algorithms)
├── chunk_server.py    # Simulated chunk server
├── client.py          # GFS client (upload / download)
├── main.py            # Demo orchestrator
├── benchmark.py       # Performance evaluation
├── visualize.py       # Matplotlib graph generator
├── requirements.txt
└── results/           # Auto-created at runtime
    ├── benchmark_results.json
    ├── upload_time.png
    ├── download_time.png
    └── combined_performance.png
```

---

## Setup

```bash
# 1. Clone / open the project folder
cd gfs-ds

# 2. (Optional) Create a virtual environment
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # Linux / macOS

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Running

### Full Demo (upload → fail → recover → re-verify)
```bash
python main.py
```

### Performance Benchmark
```bash
python benchmark.py
```

### Generate Graphs (run benchmark first)
```bash
python visualize.py
```

---

## Configuration (`config.py`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `CHUNK_SIZE` | 64 KB | Size of each chunk |
| `REPLICATION_FACTOR` | 3 | Number of replicas per chunk |
| `NUM_CHUNK_SERVERS` | 5 | Number of simulated servers |
| `HEARTBEAT_INTERVAL` | 2 s | How often servers send heartbeats |
| `HEARTBEAT_TIMEOUT` | 6 s | Silence threshold → server declared dead |

---

## What `main.py` Demonstrates

1. ✅ Boot 5 chunk servers + master
2. ✅ Upload 2 MB file (32 chunks × 64 KB, each replicated ×3)
3. ✅ Download and verify SHA-256 hash
4. ✅ Kill 2 servers (`cs0`, `cs1`)
5. ✅ Auto-recovery: under-replicated chunks redistributed within ~10 s
6. ✅ Re-download post-recovery — hash matches
