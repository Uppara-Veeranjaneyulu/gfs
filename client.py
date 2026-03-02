"""
client.py — GFS Client implementation.

The client:
  • Contacts the Master for metadata (never stores data itself)
  • Splits local files into CHUNK_SIZE pieces on upload
  • Writes each chunk to the primary server; primary forwards to secondaries
    (Algorithm 3 — lease-based consistency)
  • On download, reads each chunk from any live replica with fail-over
  • Re-assembles chunks into the destination file
"""

import math
import os
import time

from config import CHUNK_SIZE
from utils import setup_logger

log = setup_logger("Client")


class GFSClient:
    """
    GFS Client — upload and download files via the Master and ChunkServers.

    Parameters
    ----------
    master : Master
        The master node reference.
    """

    def __init__(self, master) -> None:
        self.master = master

    # ------------------------------------------------------------------
    # Upload  (Algorithm 1, 2, 3)
    # ------------------------------------------------------------------

    def upload(self, local_path: str, remote_name: str) -> float:
        """
        Upload a local file to GFS.

        1. Read the file and split it into CHUNK_SIZE pieces.
        2. Ask the master to create the file and allocate chunk metadata.
        3. For each chunk, write to primary + secondaries (lease consistency).

        Parameters
        ----------
        local_path : str
            Path to the local file to upload.
        remote_name : str
            Logical filename to use inside GFS.

        Returns
        -------
        float
            Total upload wall-clock time in seconds.
        """
        if not os.path.isfile(local_path):
            raise FileNotFoundError(f"Local file not found: {local_path}")

        file_size = os.path.getsize(local_path)
        num_chunks = max(1, math.ceil(file_size / CHUNK_SIZE))
        log.info(
            "UPLOAD  '%s' → GFS:'%s'  size=%d bytes  chunks=%d",
            local_path, remote_name, file_size, num_chunks,
        )

        # Step 1 — Ask master to allocate metadata
        chunk_metas = self.master.create_file(remote_name, num_chunks)

        # Step 2 — Read & write each chunk
        t_start = time.perf_counter()

        with open(local_path, "rb") as fh:
            for idx, meta in enumerate(chunk_metas):
                chunk_id = meta["chunk_id"]
                primary_id = meta["primary"]
                server_ids = meta["servers"]

                data = fh.read(CHUNK_SIZE)
                if not data:
                    break

                secondaries = [s for s in server_ids if s != primary_id]
                self._write_chunk_with_lease(chunk_id, data, primary_id, secondaries)

                log.info(
                    "  [UPLOAD] chunk %d/%d [%s] primary=%s secondaries=%s",
                    idx + 1, num_chunks, chunk_id[:16], primary_id, secondaries,
                )

        elapsed = time.perf_counter() - t_start
        log.info("UPLOAD complete in %.4f s", elapsed)
        return elapsed

    def _write_chunk_with_lease(
        self, chunk_id: str, data: bytes, primary_id: str, secondaries: list
    ) -> None:
        """
        Algorithm 3 — Lease-Based Consistency Write.

        1. Confirm / refresh lease with master.
        2. Write data to the primary server.
        3. Primary propagates the write to all secondaries.

        Note: propagation is performed here in the client for simplicity
        (in real GFS the primary would forward directly).
        """
        # Confirm active lease
        actual_primary = self.master.grant_lease(chunk_id)
        primary_srv = self.master.servers[actual_primary]

        # Write to primary
        primary_srv.write_chunk(chunk_id, data)

        # Propagate to secondaries
        for sec_id in secondaries:
            if sec_id in self.master.dead_servers:
                log.warning(
                    "  Skipping secondary [%s] — server is DEAD", sec_id
                )
                continue
            sec_srv = self.master.servers.get(sec_id)
            if sec_srv and sec_srv.is_alive:
                sec_srv.write_chunk(chunk_id, data)

    # ------------------------------------------------------------------
    # Download  (Algorithm 2)
    # ------------------------------------------------------------------

    def download(self, remote_name: str, local_path: str) -> float:
        """
        Download a GFS file to a local path.

        1. Ask the master for the ordered chunk list + server locations.
        2. For each chunk, read from any live replica (fail-over on error).
        3. Write reassembled bytes to *local_path*.

        Parameters
        ----------
        remote_name : str
            Logical filename in GFS.
        local_path : str
            Destination path for the reconstructed file.

        Returns
        -------
        float
            Total download wall-clock time in seconds.
        """
        log.info("DOWNLOAD  GFS:'%s' → '%s'", remote_name, local_path)
        chunk_metas = self.master.get_file_metadata(remote_name)

        t_start = time.perf_counter()
        os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)

        with open(local_path, "wb") as fh:
            for idx, meta in enumerate(chunk_metas):
                chunk_id = meta["chunk_id"]
                servers = meta["servers"]

                data = self._read_chunk_with_fallback(chunk_id, servers)
                fh.write(data)
                log.info(
                    "  [DOWNLOAD] chunk %d/%d [%s] from one of %s",
                    idx + 1, len(chunk_metas), chunk_id[:16], servers,
                )

        elapsed = time.perf_counter() - t_start
        log.info("DOWNLOAD complete in %.4f s", elapsed)
        return elapsed

    def _read_chunk_with_fallback(self, chunk_id: str, servers: list) -> bytes:
        """
        Try each server in *servers* until the chunk is successfully read.

        Raises
        ------
        IOError
            If no server can serve the requested chunk.
        """
        for server_id in servers:
            srv = self.master.servers.get(server_id)
            if srv is None or not srv.is_alive:
                continue
            try:
                return srv.read_chunk(chunk_id)
            except Exception as exc:
                log.warning(
                    "  Fallback: failed to read chunk [%s] from [%s]: %s",
                    chunk_id, server_id, exc,
                )
        raise IOError(
            f"Could not read chunk [{chunk_id}] from any server: {servers}"
        )
