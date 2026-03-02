"""
config.py — Shared configuration constants for the GFS simulation.
All tunable parameters live here so every module reads from a single source of truth.
"""

import os

# ---------------------------------------------------------------------------
# Chunk / Replication settings
# ---------------------------------------------------------------------------
CHUNK_SIZE = 64 * 1024          # 64 KB per chunk
REPLICATION_FACTOR = 3          # Number of replicas per chunk

# ---------------------------------------------------------------------------
# Cluster topology
# ---------------------------------------------------------------------------
NUM_CHUNK_SERVERS = 5           # Number of simulated chunk servers at startup

# ---------------------------------------------------------------------------
# Heartbeat timing
# ---------------------------------------------------------------------------
HEARTBEAT_INTERVAL = 2          # Seconds between heartbeat sends (per server)
HEARTBEAT_TIMEOUT = 6           # Seconds without heartbeat → server declared dead

# ---------------------------------------------------------------------------
# Storage paths
# ---------------------------------------------------------------------------
BASE_DIR = os.path.join(os.path.dirname(__file__), "gfs_storage")   # root storage folder
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")    # benchmark output folder

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = "DEBUG"             # One of: DEBUG, INFO, WARNING, ERROR
