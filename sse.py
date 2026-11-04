"""Minimal Server-Sent Events reader for GET /espectre/v1/events.

The firmware accepts at most two SSE clients, sends a ": heartbeat" comment
every 10 s, and does not replay missed events.
"""

import http.client
import socket

from http_api import BASE_PATH, DIRECT_PORT, ORIGIN

EVENTS_PATH = f"{BASE_PATH}/events"
# No line for this long means the stream is dead (heartbeat is every 10 s).
READ_TIMEOUT_S = 25.0


class SseParser:
    """Line-by-line SSE parser (the subset of the WHATWG rules ESPectre uses)."""

    def __init__(self):
        self._event = "message"
        self._data = []

    def feed_line(self, line):
        """Feed one line without its line ending.

        Returns (event_name, data_text) when an event is complete,
        (":", comment_text) for a comment such as the heartbeat, otherwise None.
        """
        if line == "":
            if not self._data:
                self._event = "message"
                return None
            item = (self._event, "\n".join(self._data))
            self._event, self._data = "message", []
            return item
        if line.startswith(":"):
            return (":", line[1:].strip())
        field, sep, value = line.partition(":")
        if sep and value.startswith(" "):
            value = value[1:]
        if field == "event":
            self._event = value
        elif field == "data":
            self._data.append(value)
        return None


class EventStream:
    """Blocking SSE connection. ``close()`` may be called from another thread."""

    def __init__(self, host, port=DIRECT_PORT, read_timeout=READ_TIMEOUT_S):
        self.host, self.port, self.read_timeout = host, port, read_timeout
        self._conn = None
        self._resp = None

    def open(self):
        """Connect and check the HTTP status. Raises RuntimeError on non-200."""
        self._conn = http.client.HTTPConnection(self.host, self.port, timeout=5.0)
        self._conn.request("GET", EVENTS_PATH, headers={
            "Accept": "text/event-stream", "Cache-Control": "no-store", "Origin": ORIGIN,
        })
        self._resp = self._conn.getresponse()
        if self._resp.status != 200:
            body = self._resp.read(512).decode("utf-8", "replace").strip()
            status = self._resp.status
            self.close()
            raise RuntimeError(f"/events returned HTTP {status}: {body}")
        if self._conn.sock is not None:
            self._conn.sock.settimeout(self.read_timeout)
        return self._resp.status

    def events(self):
        """Yield (event_name, data_text) and (":", comment) items until the stream ends."""
        parser = SseParser()
        while True:
            raw = self._resp.readline(65536)
            if not raw:
                raise ConnectionError("event stream closed by the board")
            item = parser.feed_line(raw.decode("utf-8", "replace").rstrip("\r\n"))
            if item is not None:
                yield item

    def close(self):
        conn = self._conn
        if conn is None:
            return
        sock = conn.sock
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)  # wakes a blocked readline()
            except OSError:
                pass
        try:
            conn.close()
        except OSError:
            pass
        self._conn = None
