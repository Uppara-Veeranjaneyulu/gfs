"""
utils.py — Shared utilities: logging setup and SHA-256 hashing helpers.
"""

import hashlib
import logging
import sys
from config import LOG_LEVEL


# ---------------------------------------------------------------------------
# ANSI colour codes for prettier console output
# ---------------------------------------------------------------------------
COLOURS = {
    "DEBUG":    "\033[36m",   # Cyan
    "INFO":     "\033[32m",   # Green
    "WARNING":  "\033[33m",   # Yellow
    "ERROR":    "\033[31m",   # Red
    "CRITICAL": "\033[35m",   # Magenta
    "RESET":    "\033[0m",
}


class _ColourFormatter(logging.Formatter):
    """Custom formatter that injects ANSI colour codes around the level name."""

    FMT = "%(asctime)s  [%(levelname)-8s]  %(name)-18s %(message)s"

    def format(self, record: logging.LogRecord) -> str:
        colour = COLOURS.get(record.levelname, COLOURS["RESET"])
        reset = COLOURS["RESET"]
        record.levelname = f"{colour}{record.levelname}{reset}"
        return super().format(record)


def setup_logger(name: str) -> logging.Logger:
    """
    Return a named logger with coloured console output.

    Parameters
    ----------
    name : str
        Logger name (usually the module name, e.g. ``"Master"``).
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    numeric_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(numeric_level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(numeric_level)
    handler.setFormatter(_ColourFormatter(fmt=_ColourFormatter.FMT, datefmt="%H:%M:%S"))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


# ---------------------------------------------------------------------------
# Hashing helpers
# ---------------------------------------------------------------------------

def sha256_bytes(data: bytes) -> str:
    """Return the hex SHA-256 digest of *data* (bytes)."""
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str) -> str:
    """
    Return the hex SHA-256 digest of the file at *path*.
    Reads in 1 MB blocks to handle large files efficiently.
    """
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()
