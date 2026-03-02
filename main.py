"""
main.py — GFS Simulation Orchestrator / Demo Script.

Run this script to see a full end-to-end demo:
  1. Boot Master + 5 ChunkServers
  2. Generate a 2 MB test file
  3. Upload it to GFS
  4. Download and verify SHA-256 integrity
  5. Simulate failure of 2 chunk servers
  6. Wait for automatic re-replication (Algorithm 5)
  7. Re-download and verify again
  8. Print cluster status at each stage

Usage:
    python main.py
"""

import os
import shutil
import time

# ── Simulation imports ────────────────────────────────────────────────────────
from config import NUM_CHUNK_SERVERS, BASE_DIR, HEARTBEAT_TIMEOUT, RESULTS_DIR
from master import Master
from chunk_server import ChunkServer
from client import GFSClient
from utils import setup_logger, sha256_file

log = setup_logger("Demo")

BANNER = "=" * 70


def section(title: str) -> None:
    """Print a clearly readable section header."""
    print(f"\n{BANNER}")
    print(f"  {title}")
    print(f"{BANNER}")


def boot_cluster(master: Master) -> dict:
    """Create and register NUM_CHUNK_SERVERS ChunkServers."""
    servers = {}
    for i in range(NUM_CHUNK_SERVERS):
        sid = f"cs{i}"
        srv = ChunkServer(sid, master)
        master.register_server(sid, srv)
        servers[sid] = srv
    return servers


def generate_test_file(path: str, size_bytes: int) -> None:
    """Write *size_bytes* of pseudo-random data to *path*."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "wb") as fh:
        # Use repeating pattern for speed while still being non-trivial
        block = bytes(range(256)) * 256          # 64 KB block
        written = 0
        while written < size_bytes:
            chunk = block[: min(len(block), size_bytes - written)]
            fh.write(chunk)
            written += len(chunk)
    log.info("Generated test file: %s  (%d bytes)", path, size_bytes)


def print_cluster_status(master: Master) -> None:
    status = master.cluster_status()
    print(
        f"\n  Cluster Status:\n"
        f"    Total Servers : {status['total_servers']}\n"
        f"    Alive Servers : {status['alive_servers']}\n"
        f"    Dead  Servers : {status['dead_servers']}\n"
        f"    Total Files   : {status['total_files']}\n"
        f"    Total Chunks  : {status['total_chunks']}\n"
    )


def print_chunk_locations(master: Master) -> None:
    """Print each chunk and its current replica locations."""
    print("\n  Chunk Replica Map:")
    with master._lock:
        for cid, locs in master.chunk_locations.items():
            live = [s for s in locs if s not in master.dead_servers]
            dead_mark = [s for s in locs if s in master.dead_servers]
            print(
                f"    [{cid[:16]}...]  live={live}  dead_replicas={dead_mark}"
            )


def main() -> None:
    # ── Clean up any previous run ─────────────────────────────────────────────
    if os.path.exists(BASE_DIR):
        shutil.rmtree(BASE_DIR)
    os.makedirs(BASE_DIR, exist_ok=True)
    os.makedirs(RESULTS_DIR, exist_ok=True)

    section("STEP 1 — Boot Master + Chunk Servers")
    master = Master()
    servers = boot_cluster(master)
    time.sleep(0.5)   # let heartbeats arrive once
    print_cluster_status(master)

    # ── Generate a 2 MB test file ─────────────────────────────────────────────
    test_input = os.path.join(RESULTS_DIR, "test_input.bin")
    generate_test_file(test_input, 2 * 1024 * 1024)   # 2 MB

    # ── Upload ────────────────────────────────────────────────────────────────
    section("STEP 2 — Upload test_input.bin to GFS")
    client = GFSClient(master)
    upload_time = client.upload(test_input, "test_input.bin")
    print(f"\n  ✅  Upload complete  ({upload_time:.4f} s)")
    print_cluster_status(master)

    # ── Download & Verify ─────────────────────────────────────────────────────
    section("STEP 3 — Download and verify integrity (SHA-256)")
    download_out = os.path.join(RESULTS_DIR, "test_output.bin")
    download_time = client.download("test_input.bin", download_out)

    hash_in = sha256_file(test_input)
    hash_out = sha256_file(download_out)

    print(f"\n  SHA-256 original : {hash_in}")
    print(f"  SHA-256 received : {hash_out}")

    if hash_in == hash_out:
        print("  ✅  INTEGRITY CHECK PASSED — hashes match!")
    else:
        print("  ❌  INTEGRITY CHECK FAILED — hashes DO NOT match!")

    print_chunk_locations(master)

    # ── Simulate server failures ──────────────────────────────────────────────
    section("STEP 4 — Simulate failure of 2 chunk servers (cs0, cs1)")
    servers["cs0"].simulate_failure()
    servers["cs1"].simulate_failure()

    # Wait long enough for heartbeat monitor to detect & trigger recovery
    wait_secs = HEARTBEAT_TIMEOUT + 4
    print(
        f"\n  Waiting {wait_secs}s for heartbeat monitor to detect failures "
        f"and trigger re-replication...\n"
    )
    for remaining in range(wait_secs, 0, -1):
        print(f"    {remaining} s remaining...", end="\r", flush=True)
        time.sleep(1)
    print()

    print_cluster_status(master)
    print_chunk_locations(master)

    # ── Post-recovery download & re-verify ───────────────────────────────────
    section("STEP 5 — Re-download after recovery and verify integrity")
    download_out2 = os.path.join(RESULTS_DIR, "test_output_post_recovery.bin")
    download_time2 = client.download("test_input.bin", download_out2)

    hash_out2 = sha256_file(download_out2)
    print(f"\n  SHA-256 original          : {hash_in}")
    print(f"  SHA-256 post-recovery     : {hash_out2}")

    if hash_in == hash_out2:
        print("  ✅  POST-RECOVERY INTEGRITY CHECK PASSED!")
    else:
        print("  ❌  POST-RECOVERY INTEGRITY CHECK FAILED!")

    # ── Summary ───────────────────────────────────────────────────────────────
    section("SUMMARY")
    print(f"  Upload time        : {upload_time:.4f} s")
    print(f"  Download time      : {download_time:.4f} s")
    print(f"  Post-recovery dl   : {download_time2:.4f} s")
    print(f"  File size          : 2 MB")
    print(f"  Replication factor : 3")
    print(f"  Chunk size         : 64 KB")
    print(f"\n  All output files saved to: {RESULTS_DIR}")
    print(f"{BANNER}\n")

    master.shutdown()


if __name__ == "__main__":
    main()
