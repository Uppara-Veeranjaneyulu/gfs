"""
benchmark.py — Performance Evaluation Script for the GFS Simulation.

Measures upload and download times for increasing file sizes.
Results are saved to results/benchmark_results.json.

Usage:
    python benchmark.py
"""

import json
import os
import shutil
import time

from config import BASE_DIR, RESULTS_DIR, NUM_CHUNK_SERVERS
from master import Master
from chunk_server import ChunkServer
from client import GFSClient
from utils import setup_logger

log = setup_logger("Benchmark")

# File sizes to test (in bytes)
FILE_SIZES = [
    256 * 1024,         #  256 KB
    512 * 1024,         #  512 KB
    1 * 1024 * 1024,    #    1 MB
    2 * 1024 * 1024,    #    2 MB
    4 * 1024 * 1024,    #    4 MB
    8 * 1024 * 1024,    #    8 MB
]


def create_fresh_cluster():
    """Boot a fresh Master + chunk servers."""
    master = Master()
    for i in range(NUM_CHUNK_SERVERS):
        sid = f"cs{i}"
        srv = ChunkServer(sid, master)
        master.register_server(sid, srv)
    time.sleep(0.3)  # let threads start
    return master


def generate_file(path: str, size: int) -> None:
    """Write *size* bytes of repeating pattern to *path*."""
    block = bytes(range(256)) * 256   # 64 KB block
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "wb") as fh:
        written = 0
        while written < size:
            chunk = block[: min(len(block), size - written)]
            fh.write(chunk)
            written += len(chunk)


def stop_cluster_servers(master) -> None:
    """Stop all chunk server heartbeat threads before touching storage."""
    with master._lock:
        server_objs = list(master.servers.values())
    for srv in server_objs:
        srv.is_alive = False   # stops the _heartbeat_loop while-loop
    time.sleep(0.5)            # wait for threads to exit their sleep cycle


def safe_rmtree(path: str, retries: int = 5, delay: float = 0.5) -> None:
    """Remove *path* with retries to handle Windows file-lock delays."""
    for attempt in range(retries):
        try:
            if os.path.exists(path):
                shutil.rmtree(path, ignore_errors=False)
            return
        except OSError:
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                shutil.rmtree(path, ignore_errors=True)


def run_benchmark() -> list:
    """Run upload/download benchmark for each file size. Returns results list."""
    os.makedirs(RESULTS_DIR, exist_ok=True)
    results = []

    for size in FILE_SIZES:
        size_label = (
            f"{size // (1024 * 1024)} MB"
            if size >= 1024 * 1024
            else f"{size // 1024} KB"
        )
        log.info("=" * 60)
        log.info("Benchmarking  size = %s", size_label)

        # Fresh cluster per file size
        safe_rmtree(BASE_DIR)
        os.makedirs(BASE_DIR, exist_ok=True)

        master = create_fresh_cluster()
        client = GFSClient(master)

        remote_name = f"bench_{size}.bin"
        local_in = os.path.join(RESULTS_DIR, f"bench_in_{size}.bin")
        local_out = os.path.join(RESULTS_DIR, f"bench_out_{size}.bin")

        # Generate input file
        generate_file(local_in, size)

        # ---- Upload ----
        t0 = time.perf_counter()
        client.upload(local_in, remote_name)
        upload_time = time.perf_counter() - t0

        # ---- Download ----
        t0 = time.perf_counter()
        client.download(remote_name, local_out)
        download_time = time.perf_counter() - t0

        result = {
            "size_bytes": size,
            "size_label": size_label,
            "upload_time": round(upload_time, 6),
            "download_time": round(download_time, 6),
        }
        results.append(result)
        log.info(
            "Result: upload=%.4f s  download=%.4f s",
            upload_time, download_time,
        )

        # Stop heartbeat threads BEFORE touching the filesystem
        stop_cluster_servers(master)
        master.shutdown()

        # Clean up temporary result files
        for p in (local_in, local_out):
            try:
                if os.path.exists(p):
                    os.remove(p)
            except OSError:
                pass

    # Final storage clean-up
    safe_rmtree(BASE_DIR)

    return results


def main() -> None:
    print("\n" + "=" * 60)
    print("  GFS Benchmark — Upload & Download Performance")
    print("=" * 60 + "\n")

    results = run_benchmark()

    # Save JSON
    out_path = os.path.join(RESULTS_DIR, "benchmark_results.json")
    with open(out_path, "w") as fh:
        json.dump(results, fh, indent=2)

    print("\n" + "=" * 60)
    print("  Results Summary")
    print("=" * 60)
    print(f"  {'Size':<10}  {'Upload (s)':<14}  {'Download (s)'}")
    print(f"  {'-'*8:<10}  {'-'*12:<14}  {'-'*12}")
    for r in results:
        print(
            f"  {r['size_label']:<10}  {r['upload_time']:<14.4f}  {r['download_time']:.4f}"
        )
    print(f"\n  Results saved to: {out_path}\n")


if __name__ == "__main__":
    main()
