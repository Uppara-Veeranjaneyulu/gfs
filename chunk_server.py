"""
chunk_server.py — Simulated Chunk Server for the GFS simulation.

Each ChunkServer instance:
  • Stores chunk data as binary files under  gfs_storage/server_<id>/
  • Sends periodic heartbeat messages to the Master
  • Can be artificially killed / recovered to test fault-tolerance
"""

import os
import threading
import time

from config import BASE_DIR, HEARTBEAT_INTERVAL
from utils import setup_logger

log = setup_logger("ChunkServer")


class ChunkServer:
    """
    Simulates a single GFS chunk server.

    Parameters
    ----------
    server_id : str
        Unique identifier for this server (e.g. ``"cs0"``).
    master : Master
        Reference to the Master node for heartbeat registration.
    """

    def __init__(self, server_id: str, master) -> None:
        self.server_id = server_id
        self.master = master
        self.is_alive = True

        # Local storage directory for this server's chunks
        self.storage_dir = os.path.join(BASE_DIR, f"server_{server_id}")
        os.makedirs(self.storage_dir, exist_ok=True)

        # Start heartbeat background thread
        self._hb_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True, name=f"hb-{server_id}"
        )
        self._hb_thread.start()
        log.info("ChunkServer [%s] started → storage: %s", server_id, self.storage_dir)

    # ------------------------------------------------------------------
    # Heartbeat (Algorithm 4 — client side)
    # ------------------------------------------------------------------

    def _heartbeat_loop(self) -> None:
        """Continuously send heartbeats to the master while alive."""
        while self.is_alive:
            try:
                self.master.receive_heartbeat(self.server_id)
            except Exception as exc:
                log.warning("ChunkServer [%s] heartbeat error: %s", self.server_id, exc)
            time.sleep(HEARTBEAT_INTERVAL)

    # ------------------------------------------------------------------
    # Chunk I/O
    # ------------------------------------------------------------------

    def _chunk_path(self, chunk_id: str) -> str:
        return os.path.join(self.storage_dir, f"{chunk_id}.bin")

    def write_chunk(self, chunk_id: str, data: bytes) -> None:
        """
        Write *data* to disk as chunk *chunk_id*.

        Raises
        ------
        RuntimeError
            If the server is currently marked as dead.
        """
        if not self.is_alive:
            raise RuntimeError(f"ChunkServer [{self.server_id}] is DOWN — write rejected")
        path = self._chunk_path(chunk_id)
        with open(path, "wb") as fh:
            fh.write(data)
        log.debug("  [%s] wrote chunk %s (%d bytes)", self.server_id, chunk_id, len(data))

    def read_chunk(self, chunk_id: str) -> bytes:
        """
        Read and return the bytes stored for *chunk_id*.

        Raises
        ------
        RuntimeError
            If the server is down or the chunk does not exist locally.
        """
        if not self.is_alive:
            raise RuntimeError(f"ChunkServer [{self.server_id}] is DOWN — read rejected")
        path = self._chunk_path(chunk_id)
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"ChunkServer [{self.server_id}] chunk {chunk_id} not found"
            )
        with open(path, "rb") as fh:
            return fh.read()

    def delete_chunk(self, chunk_id: str) -> None:
        """Delete a chunk file from local storage (used during re-replication clean-up)."""
        path = self._chunk_path(chunk_id)
        if os.path.exists(path):
            os.remove(path)

    def list_chunks(self) -> list:
        """Return the list of chunk IDs currently stored on this server."""
        return [f[:-4] for f in os.listdir(self.storage_dir) if f.endswith(".bin")]

    # ------------------------------------------------------------------
    # Failure / Recovery simulation
    # ------------------------------------------------------------------

    def simulate_failure(self) -> None:
        """
        Simulate a server crash:
          • Stop sending heartbeats (is_alive = False)
          • The master will detect the silence within HEARTBEAT_TIMEOUT seconds
        """
        log.warning("  [%s] *** SIMULATING FAILURE ***", self.server_id)
        self.is_alive = False

    def simulate_recovery(self) -> None:
        """
        Bring the server back online — restart heartbeat thread.
        Existing chunk files on disk remain intact.
        """
        if self.is_alive:
            return
        log.info("  [%s] *** RECOVERING ***", self.server_id)
        self.is_alive = True
        self._hb_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True, name=f"hb-{self.server_id}"
        )
        self._hb_thread.start()

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        status = "ALIVE" if self.is_alive else "DOWN"
        return f"<ChunkServer id={self.server_id} status={status}>"
