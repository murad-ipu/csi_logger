"""Small read-only client for the ESPectre Direct HTTP API (/espectre/v1).

Only GET requests exist here on purpose: the tool must never change the
board's sensing, traffic source, or Wi-Fi settings.
"""

import json
import time

import requests

DIRECT_PORT = 62587
BASE_PATH = "/espectre/v1"
# Same Origin header the ESPectre CLI sends (DEFAULT_DIRECT_ORIGIN in the repo).
ORIGIN = "https://test.espectre.dev"

# Diagnostics selections, tried in order. "*" returns everything; if a firmware
# rejects it (or the reply is too large), fall back to the fields we need.
DIAG_FIELD_SETS = [
    ["*"],
    [
        "csi_accepted_pps", "csi_callback_pps", "csi_occupancy", "csi_occupancy_slots",
        "csi_window_slots", "wifi_rssi_dbm", "wifi_channel", "generator_pps",
        "traffic_tx_pps", "traffic_rx_pps", "csi_hw_error_total",
        "csi_pending_frame_drops_total", "csi_missing_slots_total", "csi_excess_total",
        "csi_stale_total", "csi_out_of_order_total", "runtime_motion_event_drops_total",
        "raw_csi", "direct_http",
    ],
    ["csi_accepted_pps", "csi_occupancy", "wifi_rssi_dbm", "raw_csi"],
]


class BoardClient:
    """GET-only JSON client for one board. Create one per thread."""

    def __init__(self, host, port=DIRECT_PORT, raw_log=None, timeout=3.0):
        self.host = host
        self.port = port
        self.base = f"http://{host}:{port}{BASE_PATH}"
        self.raw_log = raw_log
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {"Accept": "application/json", "Cache-Control": "no-store", "Origin": ORIGIN}
        )

    def get(self, resource, body=None, timeout=None):
        """GET one resource.

        Returns (status_code, parsed_json_or_None, pc_request_t_ns, pc_t_ns).
        Raises requests.RequestException on network failure.
        Diagnostics take their field selection as a JSON body on GET (docs/API.md).
        """
        data = json.dumps(body, separators=(",", ":")) if body is not None else None
        headers = {"Content-Type": "application/json"} if data else None
        t_request = time.time_ns()
        response = self.session.get(
            f"{self.base}/{resource}", data=data, headers=headers, timeout=timeout or self.timeout
        )
        t_response = time.time_ns()
        try:
            payload = response.json()
        except ValueError:
            payload = None
        if self.raw_log is not None:
            self.raw_log.write({
                "pc_t_ns": t_response,
                "pc_request_t_ns": t_request,
                "endpoint": f"{BASE_PATH}/{resource}",
                "status": response.status_code,
                "body": payload if payload is not None else response.text,
            })
        return response.status_code, payload, t_request, t_response

    def get_json(self, resource, body=None, retries=3):
        """GET with retry and exponential backoff (1 s, 2 s, ...). Returns the dict."""
        delay = 1.0
        last_error = "no attempt"
        for attempt in range(retries):
            try:
                status, payload, _, _ = self.get(resource, body)
                if status == 200 and isinstance(payload, dict):
                    return payload
                last_error = f"HTTP {status}: {payload}"
            except requests.RequestException as exc:
                last_error = str(exc)
            if attempt + 1 < retries:
                time.sleep(delay)
                delay *= 2
        raise RuntimeError(f"GET {BASE_PATH}/{resource} failed: {last_error}")

    def read_diagnostics(self):
        """Return diagnostics using the first field selection the board accepts."""
        last_error = None
        for fields in DIAG_FIELD_SETS:
            try:
                return self.get_json("diagnostics", {"fields": fields}, retries=1)
            except RuntimeError as exc:
                last_error = exc
        raise last_error
