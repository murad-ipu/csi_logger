"""Crash-safe file writers: CSV with a growing header, JSON Lines, and errors.log.

Every writer flushes to the OS at least once per second, so data written before
a crash or power cut (minus the last second) is on disk.
"""

import csv
import json
import logging
import threading
import time
from pathlib import Path

FLUSH_INTERVAL_S = 1.0


def flatten(obj, prefix=""):
    """Flatten nested dicts into dotted keys; lists become compact JSON strings.

    {"raw_csi": {"active": true}} -> {"raw_csi.active": True}
    """
    out = {}
    for key, value in obj.items():
        name = f"{prefix}{key}"
        if isinstance(value, dict):
            out.update(flatten(value, name + "."))
        elif isinstance(value, (list, tuple)):
            out[name] = json.dumps(value, separators=(",", ":"))
        else:
            out[name] = value
    return out


class _Flushing:
    """Shared flush-at-least-every-second logic for the writers below."""

    def __init__(self):
        self._lock = threading.Lock()
        self._file = None
        self._last_flush = 0.0

    def _maybe_flush(self, force=False):
        now = time.monotonic()
        if self._file is not None and (force or now - self._last_flush >= FLUSH_INTERVAL_S):
            self._file.flush()
            self._last_flush = now

    def flush(self):
        with self._lock:
            self._maybe_flush(force=True)

    def close(self):
        with self._lock:
            if self._file is not None:
                self._file.flush()
                self._file.close()
                self._file = None


class CsvLog(_Flushing):
    """Append-only CSV writer.

    If ``fieldnames`` is given the header is fixed up front. Otherwise the header
    is taken from the first row, and when a later row brings a new field (for
    example a firmware that adds a diagnostics counter) the file is rewritten
    once with the wider header so no value is ever dropped.
    """

    def __init__(self, path, fieldnames=()):
        super().__init__()
        self.path = Path(path)
        self.fieldnames = list(fieldnames)
        self._known = set(self.fieldnames)
        self._writer = None
        self.rows = 0
        if self.fieldnames:
            self._open_new()

    def _open_new(self):
        self._file = open(self.path, "w", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._file, fieldnames=self.fieldnames, extrasaction="ignore")
        self._writer.writeheader()

    def _grow_header(self, new_keys):
        old_rows = []
        if self._file is not None:
            self._file.close()
            with open(self.path, newline="", encoding="utf-8") as handle:
                old_rows = list(csv.DictReader(handle))
        self.fieldnames.extend(new_keys)
        self._known.update(new_keys)
        self._open_new()
        self._writer.writerows(old_rows)

    def write(self, row):
        with self._lock:
            new_keys = [key for key in row if key not in self._known]
            if new_keys or self._writer is None:
                self._grow_header(new_keys)
            self._writer.writerow(row)
            self.rows += 1
            self._maybe_flush()


class JsonlLog(_Flushing):
    """One JSON object per line (used for raw_http.jsonl). Thread-safe."""

    def __init__(self, path):
        super().__init__()
        self.path = Path(path)
        self._file = open(self.path, "a", encoding="utf-8")

    def write(self, obj):
        line = json.dumps(obj, separators=(",", ":"), default=str)
        with self._lock:
            if self._file is None:
                return
            self._file.write(line + "\n")
            self._maybe_flush()


def make_error_logger(name, path):
    """Return a logger that appends timestamped lines to ``path`` (errors.log)."""
    logger = logging.getLogger(f"csi_logger.{name}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for handler in list(logger.handlers):  # avoid duplicates if called twice
        logger.removeHandler(handler)
        handler.close()
    handler = logging.FileHandler(path, encoding="utf-8")  # flushes every record
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(threadName)s: %(message)s"))
    logger.addHandler(handler)
    return logger


def close_error_logger(logger):
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
