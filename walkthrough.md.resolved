# GFS Simulation — Walkthrough & Verification Results

## What Was Built

A fully functional **Google File System (GFS) simulation** in Python 3 across 10 files:

| File | Purpose |
|------|---------|
| [config.py](file:///c:/Users/uuppa/OneDrive/Desktop/PROJECTS/gfs-ds/config.py) | Shared constants (chunk size, replication factor, timeouts) |
| [utils.py](file:///c:/Users/uuppa/OneDrive/Desktop/PROJECTS/gfs-ds/utils.py) | Coloured logging + SHA-256 hash helpers |
| [master.py](file:///c:/Users/uuppa/OneDrive/Desktop/PROJECTS/gfs-ds/master.py) | Master node — all 5 algorithms |
| [chunk_server.py](file:///c:/Users/uuppa/OneDrive/Desktop/PROJECTS/gfs-ds/chunk_server.py) | Simulated chunk server (disk storage + heartbeat) |
| [client.py](file:///c:/Users/uuppa/OneDrive/Desktop/PROJECTS/gfs-ds/client.py) | GFS client (upload/download with lease consistency) |
| [main.py](file:///c:/Users/uuppa/OneDrive/Desktop/PROJECTS/gfs-ds/main.py) | Full demo orchestrator |
| [benchmark.py](file:///c:/Users/uuppa/OneDrive/Desktop/PROJECTS/gfs-ds/benchmark.py) | Upload/download performance measurement |
| [visualize.py](file:///c:/Users/uuppa/OneDrive/Desktop/PROJECTS/gfs-ds/visualize.py) | Matplotlib graph generator |
| [requirements.txt](file:///c:/Users/uuppa/OneDrive/Desktop/PROJECTS/gfs-ds/requirements.txt) | `matplotlib` only |
| [README.md](file:///c:/Users/uuppa/OneDrive/Desktop/PROJECTS/gfs-ds/README.md) | Setup & run guide |

---

## Algorithms Implemented

| # | Algorithm | Location |
|---|-----------|----------|
| 1 | **Chunk Placement & Replication** (RF=3) | `master._select_servers()` + `client.upload()` |
| 2 | **Master Metadata Management** | `master.create_file()` / [get_file_metadata()](file:///c:/Users/uuppa/OneDrive/Desktop/PROJECTS/gfs-ds/master.py#312-346) |
| 3 | **Lease-Based Consistency** | `master.grant_lease()` + `client._write_chunk_with_lease()` |
| 4 | **Heartbeat Fault Detection** | `master._heartbeat_monitor_loop()` + `chunk_server._heartbeat_loop()` |
| 5 | **Fault Recovery & Re-Replication** | `master._recover_under_replicated()` |

---

## Test Results

### ✅ `python main.py` — Full Demo (Exit code: 0)

Steps verified:
- ✅ Master + 5 chunk servers booted
- ✅ 2 MB file split into 32 × 64 KB chunks, each replicated to 3 servers (96 chunk writes total)
- ✅ Download reassembled correctly
- ✅ **SHA-256 integrity check PASSED**
- ✅ 2 servers (`cs0`, `cs1`) killed — heartbeat monitor detected deaths within 6 s
- ✅ Under-replicated chunks automatically re-replicated to healthy servers
- ✅ **Post-recovery integrity check PASSED** — file still downloadable after 2 server failures

---

### ✅ `python benchmark.py` — Performance Benchmark (Exit code: 0)

| File Size | Upload Time (s) | Download Time (s) |
|-----------|----------------|------------------|
| 256 KB | 0.0472 | 0.0849 |
| 512 KB | 0.1285 | 0.2321 |
| 1 MB | 0.1054 | 0.4055 |
| 2 MB | 0.1722 | 0.6093 |
| 4 MB | 0.3323 | 1.2568 |
| 8 MB | 0.6539 | 2.2274 |

> Download is slower than upload because each chunk is read sequentially from disk; upload writes to primary only (secondaries receive in parallel in real GFS, but are sequential in this simulation).

---

### ✅ `python visualize.py` — Graph Generation (Exit code: 0)

Three PNG charts saved to [results/](file:///c:/Users/uuppa/OneDrive/Desktop/PROJECTS/gfs-ds/visualize.py#38-45):

![Upload Time vs File Size](C:\Users\uuppa\.gemini\antigravity\brain\e4e6824e-a0dc-431d-a68c-780eb57b7095\upload_time.png)

![Download Time vs File Size](C:\Users\uuppa\.gemini\antigravity\brain\e4e6824e-a0dc-431d-a68c-780eb57b7095\download_time.png)

![Combined Performance](C:\Users\uuppa\.gemini\antigravity\brain\e4e6824e-a0dc-431d-a68c-780eb57b7095\combined_performance.png)

---

## How to Run

```bash
# Demo (upload → fail → recover → re-verify)
python main.py

# Benchmark
python benchmark.py

# Graphs (run benchmark first)
python visualize.py
```
