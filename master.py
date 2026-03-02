"""
master.py — GFS Master Node implementation.

Responsibilities:
  1.  Metadata management    — file → chunk list, chunk → server list (Algorithm 2)
  2.  Chunk placement        — assign replication servers when a new chunk is created (Algorithm 1)
  3.  Lease management       — grant primary lease per chunk (Algorithm 3)
  4.  Heartbeat monitoring   — detect dead servers (Algorithm 4)
  5.  Fault recovery         — re-replicate under-replicated chunks (Algorithm 5)
"""

import random
import threading
import time
import uuid

from config import (
    HEARTBEAT_TIMEOUT,
    HEARTBEAT_INTERVAL,
    REPLICATION_FACTOR,
)
from utils import setup_logger

log = setup_logger("Master")


class Master:
    """
    Central metadata server for the GFS simulation.

    All state is kept in-memory (Python dicts).  No actual file data
    passes through the master — only metadata.

    Attributes
    ----------
    files : dict[str, list[str]]
        Maps a remote filename to the ordered list of chunk IDs.
    chunk_locations : dict[str, list[str]]
        Maps each chunk ID to the list of server IDs holding a replica.
    chunk_primary : dict[str, str]
        Maps each chunk ID to the server ID that currently holds the lease
        (i.e., is the primary replica for writes).
    servers : dict[str, ChunkServer]
        Registry of all known chunk servers (id → object).
    last_heartbeat : dict[str, float]
        Last time (epoch seconds) a heartbeat was received per server.
    dead_servers : set[str]
        Server IDs currently considered dead by the heartbeat monitor.
    """

    def __init__(self) -> None:
        self.files: dict = {}            # filename → [chunk_id, ...]
        self.chunk_locations: dict = {}  # chunk_id → [server_id, ...]
        self.chunk_primary: dict = {}    # chunk_id → primary_server_id
        self.servers: dict = {}          # server_id → ChunkServer object
        self.last_heartbeat: dict = {}   # server_id → timestamp
        self.dead_servers: set = set()

        self._lock = threading.Lock()    # Protects all metadata writes

        # Start background heartbeat monitor thread
        self._monitor_thread = threading.Thread(
            target=self._heartbeat_monitor_loop,
            daemon=True,
            name="hb-monitor",
        )
        self._monitor_thread.start()
        log.info("Master node started.")

    # ------------------------------------------------------------------
    # Server Registration
    # ------------------------------------------------------------------

    def register_server(self, server_id: str, server_obj) -> None:
        """Register a chunk server with the master at startup."""
        with self._lock:
            self.servers[server_id] = server_obj
            self.last_heartbeat[server_id] = time.time()
        log.info("Registered chunk server: %s", server_id)

    # ------------------------------------------------------------------
    # Algorithm 4 — Heartbeat Receipt & Monitor
    # ------------------------------------------------------------------

    def receive_heartbeat(self, server_id: str) -> None:
        """
        Called by a ChunkServer to signal it is alive.
        Updates ``last_heartbeat`` timestamp for *server_id*.
        """
        with self._lock:
            self.last_heartbeat[server_id] = time.time()
            if server_id in self.dead_servers:
                # Server came back online
                self.dead_servers.discard(server_id)
                log.info("HeartbeatMonitor: server [%s] is back ONLINE", server_id)

    def _heartbeat_monitor_loop(self) -> None:
        """
        Background thread — runs every HEARTBEAT_INTERVAL seconds.

        For each registered server, checks whether the time since its
        last heartbeat exceeds HEARTBEAT_TIMEOUT.  Dead servers are
        added to ``dead_servers`` and under-replicated chunks are
        queued for re-replication (Algorithm 5).
        """
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            now = time.time()
            newly_dead = []

            with self._lock:
                for server_id, last in list(self.last_heartbeat.items()):
                    if (now - last) > HEARTBEAT_TIMEOUT:
                        if server_id not in self.dead_servers:
                            self.dead_servers.add(server_id)
                            newly_dead.append(server_id)

            for server_id in newly_dead:
                log.warning(
                    "HeartbeatMonitor: server [%s] is DEAD (no heartbeat for >%ds)",
                    server_id,
                    HEARTBEAT_TIMEOUT,
                )
                # Trigger re-replication for affected chunks
                self._recover_under_replicated()

    # ------------------------------------------------------------------
    # Algorithm 5 — Fault Recovery & Re-Replication
    # ------------------------------------------------------------------

    def _recover_under_replicated(self) -> None:
        """
        Identify chunks that now have fewer than REPLICATION_FACTOR live
        replicas and create new copies on healthy servers.

        Steps:
          1. For each chunk, compute live replicas (servers not in dead_servers).
          2. If live_count < REPLICATION_FACTOR, pick a source replica and a
             set of target servers equal to the deficit.
          3. Copy the chunk data from source → each target.
          4. Update chunk_locations and chunk_primary metadata.
        """
        with self._lock:
            chunk_locations_snapshot = {
                cid: list(locs) for cid, locs in self.chunk_locations.items()
            }
            dead = set(self.dead_servers)
            all_servers = {sid: srv for sid, srv in self.servers.items()}

        for chunk_id, locs in chunk_locations_snapshot.items():
            live_locs = [s for s in locs if s not in dead]
            deficit = REPLICATION_FACTOR - len(live_locs)
            if deficit <= 0:
                continue  # Still sufficiently replicated

            log.warning(
                "Recovery: chunk [%s] under-replicated — %d live / %d needed",
                chunk_id,
                len(live_locs),
                REPLICATION_FACTOR,
            )

            if not live_locs:
                log.error(
                    "Recovery: chunk [%s] has NO live replicas — DATA LOST!", chunk_id
                )
                continue

            # Pick a source server to copy from
            source_id = live_locs[0]
            source_srv = all_servers.get(source_id)
            if source_srv is None or not source_srv.is_alive:
                log.error("Recovery: source server [%s] unavailable.", source_id)
                continue

            # Read chunk data from the source
            try:
                data = source_srv.read_chunk(chunk_id)
            except Exception as exc:
                log.error("Recovery: failed to read chunk [%s] from [%s]: %s",
                          chunk_id, source_id, exc)
                continue

            # Find candidate target servers (alive, not already holding the chunk)
            candidates = [
                sid for sid, srv in all_servers.items()
                if sid not in locs and sid not in dead and srv.is_alive
            ]
            random.shuffle(candidates)
            targets = candidates[:deficit]

            if not targets:
                log.warning("Recovery: no free servers available for chunk [%s].", chunk_id)
                continue

            for target_id in targets:
                try:
                    all_servers[target_id].write_chunk(chunk_id, data)
                    with self._lock:
                        self.chunk_locations[chunk_id].append(target_id)
                    log.info(
                        "Recovery: chunk [%s] copied [%s] → [%s]",
                        chunk_id, source_id, target_id,
                    )
                except Exception as exc:
                    log.error(
                        "Recovery: failed to copy chunk [%s] to [%s]: %s",
                        chunk_id, target_id, exc,
                    )

            # Refresh primary if needed
            with self._lock:
                current_primary = self.chunk_primary.get(chunk_id)
                if current_primary in dead or current_primary is None:
                    live_after = [
                        s for s in self.chunk_locations[chunk_id]
                        if s not in self.dead_servers and all_servers.get(s, None) and all_servers[s].is_alive
                    ]
                    if live_after:
                        self.chunk_primary[chunk_id] = live_after[0]
                        log.info(
                            "Recovery: new primary for chunk [%s] → [%s]",
                            chunk_id, live_after[0],
                        )

    # ------------------------------------------------------------------
    # Algorithm 1 — Chunk Placement (server selection)
    # ------------------------------------------------------------------

    def _select_servers(self, count: int) -> list:
        """
        Select *count* distinct alive chunk servers for a new chunk.

        Uses a shuffled list of live servers to distribute chunks evenly.

        Raises
        ------
        RuntimeError
            If fewer than *count* servers are currently alive.
        """
        alive = [
            sid for sid, srv in self.servers.items()
            if sid not in self.dead_servers and srv.is_alive
        ]
        if len(alive) < count:
            raise RuntimeError(
                f"Not enough alive servers ({len(alive)}) to satisfy "
                f"replication factor ({count})"
            )
        random.shuffle(alive)
        return alive[:count]

    # ------------------------------------------------------------------
    # Algorithm 2 — Metadata Management: File Creation
    # ------------------------------------------------------------------

    def create_file(self, filename: str, num_chunks: int) -> list:
        """
        Register a new file with the master and allocate chunk IDs + server
        assignments for each chunk.

        Parameters
        ----------
        filename : str
            Logical name of the file in GFS.
        num_chunks : int
            Number of chunks the file will be split into.

        Returns
        -------
        list of dict
            One entry per chunk: ``{"chunk_id": str, "servers": [str, ...], "primary": str}``
        """
        with self._lock:
            if filename in self.files:
                raise FileExistsError(f"File '{filename}' already exists in GFS.")

        chunk_meta = []
        chunk_ids = []

        for _ in range(num_chunks):
            chunk_id = f"chunk_{uuid.uuid4().hex[:12]}"
            with self._lock:
                servers = self._select_servers(REPLICATION_FACTOR)
            primary = servers[0]

            with self._lock:
                self.chunk_locations[chunk_id] = servers
                self.chunk_primary[chunk_id] = primary

            chunk_ids.append(chunk_id)
            chunk_meta.append(
                {"chunk_id": chunk_id, "servers": servers, "primary": primary}
            )
            log.debug(
                "Allocated chunk [%s] → servers %s (primary: %s)",
                chunk_id, servers, primary,
            )

        with self._lock:
            self.files[filename] = chunk_ids

        log.info(
            "File '%s' created — %d chunk(s) allocated.", filename, num_chunks
        )
        return chunk_meta

    # ------------------------------------------------------------------
    # Algorithm 2 — Metadata Management: File Lookup
    # ------------------------------------------------------------------

    def get_file_metadata(self, filename: str) -> list:
        """
        Return the ordered list of chunk descriptors for *filename*.

        Parameters
        ----------
        filename : str
            Logical GFS filename.

        Returns
        -------
        list of dict
            ``{"chunk_id": str, "servers": [str, ...], "primary": str}``

        Raises
        ------
        FileNotFoundError
            If *filename* is not in the master's namespace.
        """
        with self._lock:
            if filename not in self.files:
                raise FileNotFoundError(f"File '{filename}' not found in GFS.")
            chunk_ids = list(self.files[filename])

        meta = []
        for cid in chunk_ids:
            with self._lock:
                locs = list(self.chunk_locations.get(cid, []))
                primary = self.chunk_primary.get(cid)
                dead = set(self.dead_servers)
            live = [s for s in locs if s not in dead]
            meta.append({"chunk_id": cid, "servers": live, "primary": primary})

        return meta

    # ------------------------------------------------------------------
    # Algorithm 3 — Lease Grant
    # ------------------------------------------------------------------

    def grant_lease(self, chunk_id: str) -> str:
        """
        Return the current primary server for *chunk_id*.

        If the primary is dead, elect a new one from the live replicas.

        Returns
        -------
        str
            Server ID of the primary replica.

        Raises
        ------
        RuntimeError
            If no live replicas exist for the chunk.
        """
        with self._lock:
            locs = self.chunk_locations.get(chunk_id, [])
            dead = set(self.dead_servers)
            live = [s for s in locs if s not in dead]
            primary = self.chunk_primary.get(chunk_id)

            if primary not in live:
                if not live:
                    raise RuntimeError(
                        f"No live replicas for chunk {chunk_id} — cannot grant lease."
                    )
                primary = live[0]
                self.chunk_primary[chunk_id] = primary
                log.info("Lease re-granted: chunk [%s] → new primary [%s]", chunk_id, primary)

        return primary

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def list_files(self) -> list:
        """Return all filenames currently stored in GFS."""
        with self._lock:
            return list(self.files.keys())

    def cluster_status(self) -> dict:
        """Return a snapshot of alive/dead server counts and total chunks."""
        with self._lock:
            total = len(self.servers)
            dead = len(self.dead_servers)
            return {
                "total_servers": total,
                "alive_servers": total - dead,
                "dead_servers": dead,
                "total_files": len(self.files),
                "total_chunks": len(self.chunk_locations),
            }

    def shutdown(self) -> None:
        """Gracefully note shutdown (daemon threads will exit automatically)."""
        log.info("Master shutting down.")
