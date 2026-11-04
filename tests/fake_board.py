"""A tiny fake ESPectre Native board for end-to-end tests (no hardware needed).

Serves the read-only Direct API resources, a GET /csi binary stream of V8
frames at ~100 records/s, and an SSE /events stream with motion events at
4 Hz. It mimics the documented rules: sensing.mode becomes "csi_collection"
while /csi is open, a second /csi returns 409.
"""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from v8 import build_frame

CAPABILITIES = {
    "protocol_version": "1.0",
    "resources": ["health", "device", "capabilities", "sensing", "wifi", "diagnostics"],
    "operations": [],
    "events": ["health", "device", "sensing", "wifi", "ota", "motion", "fault"],
    "features": {"csi": True},
    "csi": {"transport": "http", "protocol_version": 1, "record_version": 8, "frame_prefix_bytes": 60},
}


class FakeBoard:
    def __init__(self, csi_supported=True):
        self.lock = threading.Lock()
        self.csi_open = False
        self.csi_sessions = 0
        self.offline = False          # True: drop every connection (board unplugged)
        self.capabilities = json.loads(json.dumps(CAPABILITIES))
        if not csi_supported:
            self.capabilities["features"]["csi"] = False
        self.sensing = {
            "enabled": True, "ready": True, "calibrating": False, "derived_events_paused": False,
            "mode": "sensing", "detector": "lightweight", "threshold": 0.66,
            "motion_on_hits": 4, "motion_off_hits": 3, "traffic_generator_mode": "wifi_raw",
            "csi_target_pps": 100,
        }
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()

    def resource(self, name):
        now_ms = int(time.monotonic() * 1000)
        if name == "capabilities":
            return self.capabilities
        if name == "device":
            return {"device_id": "000010b41de8ec00", "name": "fake", "label": "", "frontend": "native",
                    "firmware": "3.1.0", "chip": "esp32s3", "csi_profile": "ht20"}
        if name == "sensing":
            with self.lock:
                return dict(self.sensing, mode="csi_collection" if self.csi_open else "sensing",
                            ready=not self.csi_open)
        if name == "wifi":
            return {"configured": True, "connected": True, "ssid": "lab", "bssid": "aa:bb:cc:dd:ee:ff",
                    "band": "2g", "channel": 6, "rssi_dbm": -50, "ip": "127.0.0.1"}
        if name == "health":
            return {"status": "ok", "online": True, "uptime_s": now_ms // 1000, "timestamp_ms": now_ms}
        if name == "diagnostics":
            return {"timestamp_ms": now_ms, "uptime": now_ms // 1000, "csi_accepted_pps": 100.0,
                    "csi_occupancy": 0.98, "wifi_rssi_dbm": -50,
                    "raw_csi": {"active": self.csi_open, "raw_drop_total": 0, "send_backpressure_total": 0},
                    "direct_http": {"dropped_motion_events": 0}}
        return None

    def _handler(self):
        board = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def _json(self, status, obj):
                body = json.dumps(obj).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                if board.offline:
                    self.close_connection = True
                    self.connection.close()
                    return
                length = int(self.headers.get("Content-Length") or 0)
                if length:
                    self.rfile.read(length)
                path = self.path.split("?")[0]
                if path == "/espectre/v1/csi":
                    return self._csi()
                if path == "/espectre/v1/events":
                    return self._events()
                name = path.rsplit("/", 1)[-1]
                obj = board.resource(name)
                if obj is None:
                    return self._json(404, {"code": "not_found"})
                return self._json(200, obj)

            def _csi(self):
                with board.lock:
                    if board.csi_open:
                        return self._json(409, {"code": "busy_raw_collection"})
                    board.csi_open = True
                    board.csi_sessions += 1
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Connection", "close")
                self.end_headers()
                self.close_connection = True
                seq = 0
                try:
                    while not board.offline:
                        seq += 1
                        self.wfile.write(build_frame(seq))
                        self.wfile.flush()
                        time.sleep(0.01)
                except OSError:
                    pass  # client closed the connection: collection ends
                finally:
                    with board.lock:
                        board.csi_open = False

            def _events(self):
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Connection", "close")
                self.end_headers()
                self.close_connection = True
                try:
                    self.wfile.write(b": heartbeat\n\n")
                    i = 0
                    while not board.offline:
                        i += 1
                        state = "motion" if (i // 8) % 2 else "idle"
                        data = {"timestamp_ms": int(time.monotonic() * 1000), "state": state,
                                "score": 0.9 if state == "motion" else 0.1}
                        self.wfile.write(f"event: motion\ndata: {json.dumps(data)}\n\n".encode())
                        if i == 5:
                            self.wfile.write(b'event: fault\ndata: {"timestamp_ms":1,"message":"test fault"}\n\n')
                        self.wfile.flush()
                        time.sleep(0.25)
                except OSError:
                    pass

        return Handler
