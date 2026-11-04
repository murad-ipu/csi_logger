"""One recording session for one ESPectre board.

Threads per board:
  * poller  - GET /diagnostics every 1 s, /health and /wifi every 5 s;
  * worker  - either the raw CSI stream (GET /csi) or the SSE stream (GET /events).
Both threads reconnect with exponential backoff and never raise; every problem
goes to errors.log in the board folder.
"""

import json
import threading
import time
from datetime import datetime
from pathlib import Path

import requests

from csi_store import CsiRecorder, merge_parts
from http_api import BASE_PATH, DIAG_FIELD_SETS, DIRECT_PORT, BoardClient
from sse import EventStream
from writers import CsvLog, JsonlLog, close_error_logger, flatten, make_error_logger

DIAG_INTERVAL_S = 1.0
HEALTH_INTERVAL_S = 5.0
CSI_RUN_SLICE_S = 1.0       # receiver.run() slice, so stop requests are noticed quickly
MAX_BACKOFF_S = 30.0
RESUME_WAIT_S = 20.0        # how long to wait for the board to leave csi_collection
# Sensing fields that change on their own; ignored when checking for setting changes.
VOLATILE_SENSING_FIELDS = {"ready", "calibrating", "mode", "derived_events_paused", "timestamp_ms"}


class SetupError(Exception):
    """The board cannot be used for the requested mode (message is shown to the user)."""


def iso(t_ns):
    return datetime.fromtimestamp(t_ns / 1e9).astimezone().isoformat(timespec="milliseconds")


def check_board(client, mode):
    """Read capabilities/device/sensing/wifi/health and verify the mode is supported.

    Returns a dict of the resources. Raises SetupError with a clear message.
    """
    try:
        caps = client.get_json("capabilities")
    except RuntimeError as exc:
        raise SetupError(f"{client.host}: board not reachable on port {client.port} ({exc})")
    info = {"capabilities": caps}
    for name in ("device", "sensing", "wifi", "health"):
        try:
            info[name] = client.get_json(name)
        except RuntimeError:
            info[name] = None

    features = caps.get("features") or {}
    sensing = info.get("sensing") or {}
    if mode == "csi":
        if features.get("csi") is not True or "csi" not in caps:
            raise SetupError(
                f"{client.host}: raw CSI collection (GET {BASE_PATH}/csi) is not supported "
                "by this firmware (capabilities.features.csi is not true)."
            )
        if sensing and not sensing.get("enabled"):
            raise SetupError(
                f"{client.host}: sensing is disabled. GET /csi needs sensing enabled; "
                "this tool does not change board settings, so enable it in the Monitor first."
            )
        if sensing.get("mode") == "csi_collection":
            raise SetupError(
                f"{client.host}: another /csi session is already open on this board "
                "(sensing.mode = csi_collection). Close it and try again."
            )
    elif mode == "events":
        if "motion" not in (caps.get("events") or []):
            raise SetupError(
                f"{client.host}: the board does not publish motion events on "
                f"GET {BASE_PATH}/events (capabilities.events = {caps.get('events')})."
            )
    return info


class BoardSession:
    def __init__(self, host, port, mode, board_dir, duration_s, label, start_delay_s, info):
        self.host, self.port, self.mode = host, port, mode
        self.dir = Path(board_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.duration_s, self.label, self.start_delay_s = duration_s, label, start_delay_s
        self.info = info                        # resources read by check_board()
        self.stop_event = threading.Event()
        self.raw_log = JsonlLog(self.dir / "raw_http.jsonl")
        self.log = make_error_logger(host, self.dir / "errors.log")
        self._threads = []
        self._lock = threading.Lock()
        self._repeat = {}                       # error message -> repeat count
        self.recorder = None
        self._stream = None                     # open EventStream (events mode)
        self._diag_set = 0                      # index into http_api.DIAG_FIELD_SETS
        self._last_status = None                # HTTP status of the last poll
        self.connections = []                 # one summary per /csi or /events connection
        self.clock_pairs = []                   # (pc_mid_t_ns, device timestamp_ms) from /health
        self.stats = {
            "records": 0, "events": 0, "motion_events": 0, "faults": 0, "reconnects": 0,
            "errors": 0, "connected": False, "state": None, "score": None,
            "occupancy": None, "accepted_pps": None, "rssi": None,
            "diag_raw_drop_total": None, "diag_dropped_motion_events": None,
        }
        self.start_ns = self.end_ns = None
        self.t0 = self.end_mono = None

    # ------------------------------------------------------------------ control
    def start(self, t0_mono):
        """Start poller and worker threads. ``t0_mono`` is shared by all boards."""
        self.t0, self.end_mono = t0_mono, t0_mono + self.duration_s
        self.start_ns = time.time_ns()
        self.write_session_json("running")
        worker = self._csi_loop if self.mode == "csi" else self._events_loop
        for target in (self._poll_loop, worker):
            thread = threading.Thread(target=self._guard, args=(target,),
                                      name=f"{self.host}-{target.__name__.strip('_')}", daemon=True)
            thread.start()
            self._threads.append(thread)

    def request_stop(self):
        """Ask both threads to stop (safe from any thread, returns immediately)."""
        self.stop_event.set()
        stream = self._stream
        if stream is not None:
            stream.close()  # unblocks the SSE readline()

    def join(self, timeout):
        deadline = time.monotonic() + timeout
        for thread in self._threads:
            thread.join(max(0.0, deadline - time.monotonic()))

    def done(self):
        return bool(self._threads) and not any(t.is_alive() for t in self._threads)

    def _active(self):
        return not self.stop_event.is_set() and time.monotonic() < self.end_mono

    def _remaining(self):
        return self.end_mono - time.monotonic()

    def _guard(self, target):
        try:
            target()
        except Exception:  # last line of defence: log, never crash the session
            self.log.exception("unexpected error in %s", target.__name__)
            self._count("errors")

    def _count(self, key, n=1):
        with self._lock:
            self.stats[key] += n

    def _error(self, message):
        """Log an error; identical repeats are logged 1st, 10th, 100th, ... time."""
        self._count("errors")
        n = self._repeat.get(message, 0) + 1
        self._repeat[message] = n
        if n in (1, 10, 100, 1000) or n % 10000 == 0:
            self.log.error("%s%s", message, f" (repeated {n} times)" if n > 1 else "")

    # ------------------------------------------------------------------ pollers
    def _poll_loop(self):
        client = BoardClient(self.host, self.port, self.raw_log)  # own HTTP session
        logs = {name: CsvLog(self.dir / f"{name}.csv") for name in ("diagnostics", "health", "wifi")}
        next_diag = next_health = time.monotonic()
        try:
            while self._active():
                now = time.monotonic()
                if now >= next_health:
                    health = self._poll(client, "health", logs["health"])
                    if health and health.get("_t") and "timestamp_ms" in health:
                        self.clock_pairs.append((health["_t"], health["timestamp_ms"]))
                    self._poll(client, "wifi", logs["wifi"])
                    next_health = max(next_health + HEALTH_INTERVAL_S, now)
                if now >= next_diag:
                    self._poll_diagnostics(client, logs["diagnostics"])
                    next_diag = max(next_diag + DIAG_INTERVAL_S, now)
                self.stop_event.wait(max(0.0, min(next_diag, next_health) - time.monotonic()))
        finally:
            for log in logs.values():
                log.close()

    def _poll(self, client, resource, csv_log, body=None):
        """GET one resource and append it (flattened) to its CSV. Returns the dict or None."""
        self._last_status = None
        try:
            status, payload, t_req, t_resp = client.get(resource, body)
        except requests.RequestException as exc:
            self._error(f"GET /{resource} failed: {type(exc).__name__}: {exc}")
            return None
        self._last_status = status
        if status != 200 or not isinstance(payload, dict):
            self._error(f"GET /{resource} returned HTTP {status}: {str(payload)[:200]}")
            return None
        row = {"pc_t_ns": t_resp, "pc_request_t_ns": t_req}
        row.update(flatten(payload))
        csv_log.write(row)
        payload["_t"] = (t_req + t_resp) // 2  # midpoint, used for clock pairs
        return payload

    def _poll_diagnostics(self, client, csv_log):
        index = self._diag_set
        diag = self._poll(client, "diagnostics", csv_log, {"fields": DIAG_FIELD_SETS[index]})
        if diag is None:
            # A rejected selection (HTTP 4xx) moves to the next, smaller field set.
            last = self._last_status
            if last is not None and 400 <= last < 500 and index + 1 < len(DIAG_FIELD_SETS):
                self._diag_set = index + 1
                self.log.warning("diagnostics field set %d rejected, using set %d", index, index + 1)
            return
        raw = diag.get("raw_csi") if isinstance(diag.get("raw_csi"), dict) else {}
        http = diag.get("direct_http") if isinstance(diag.get("direct_http"), dict) else {}
        with self._lock:
            self.stats["occupancy"] = diag.get("csi_occupancy")
            self.stats["accepted_pps"] = diag.get("csi_accepted_pps")
            self.stats["rssi"] = diag.get("wifi_rssi_dbm")
            self.stats["diag_raw_drop_total"] = raw.get("raw_drop_total")
            self.stats["diag_dropped_motion_events"] = http.get("dropped_motion_events")

    # ------------------------------------------------------------------ CSI worker
    def _csi_loop(self):
        from tools.lib.csi_io import DirectRawCSIReceiver

        self.recorder = CsiRecorder(self.dir)
        backoff = 1.0
        connection = 0
        try:
            while self._active():
                connection += 1
                receiver = DirectRawCSIReceiver(f"{self.host}:{self.port}", derive_complex=False)
                receiver.add_callback(lambda pkt, c=connection: self._on_record(pkt, c))
                before = self.recorder.count
                summary = {"connection": connection, "opened_pc_t_ns": time.time_ns()}
                self.raw_log.write({"pc_t_ns": summary["opened_pc_t_ns"], "endpoint": f"{BASE_PATH}/csi",
                                    "event": "open", "connection": connection})
                try:
                    with self._lock:
                        self.stats["connected"] = True
                    # Run in short slices so Ctrl+C / end of duration is noticed within ~1 s.
                    while self._active():
                        receiver.run(timeout=max(0.05, min(CSI_RUN_SLICE_S, self._remaining())))
                except Exception as exc:
                    message = str(exc)
                    if "409" in message:
                        message += " (another /csi session is open, or the board is busy)"
                    summary["error"] = message
                    self._error(f"/csi connection {connection}: {message}")
                finally:
                    with self._lock:
                        self.stats["connected"] = False
                    # stop() reads final raw_csi diagnostics, then closes the socket,
                    # which ends the collection so the board resumes sensing.
                    try:
                        receiver.stop()
                    except Exception as exc:
                        self._error(f"/csi stop failed: {exc}")
                    summary.update({
                        "closed_pc_t_ns": time.time_ns(),
                        "records": self.recorder.count - before,
                        "raw_drop_total": receiver.raw_drop_total,
                        "send_backpressure_total": receiver.raw_send_backpressure_total,
                        "fresh_record_total": receiver.raw_final_fresh_record_total,
                        "host_sequence_gaps": receiver.dropped_count,
                        "out_of_order": receiver.out_of_order_count,
                    })
                    self.connections.append(summary)
                    self.raw_log.write({"pc_t_ns": summary["closed_pc_t_ns"], "endpoint": f"{BASE_PATH}/csi",
                                        "event": "close", **summary})
                if summary["records"] > 0:
                    backoff = 1.0
                if self._active():
                    self._count("reconnects")
                    self.stop_event.wait(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF_S)
        finally:
            self.recorder.close()

    def _on_record(self, packet, connection):
        try:
            self.recorder.add(packet, connection)
            self.stats["records"] = self.recorder.count
        except Exception as exc:  # never let a bad record kill the stream
            self._error(f"failed to store CSI record: {exc}")

    # ------------------------------------------------------------------ SSE worker
    def _events_loop(self):
        fields = ["pc_t_ns", "device_timestamp_ms", "event", "state", "score", "message"]
        events_csv = CsvLog(self.dir / "events.csv", fields)
        backoff = 1.0
        connection = 0
        try:
            while self._active():
                connection += 1
                stream = self._stream = EventStream(self.host, self.port)
                summary = {"connection": connection, "opened_pc_t_ns": time.time_ns(), "events": 0}
                try:
                    status = stream.open()
                    self.raw_log.write({"pc_t_ns": time.time_ns(), "endpoint": f"{BASE_PATH}/events",
                                        "status": status, "event": "open", "connection": connection})
                    with self._lock:
                        self.stats["connected"] = True
                    backoff = 1.0
                    for name, text in stream.events():
                        pc_t_ns = time.time_ns()
                        if name == ":":  # heartbeat comment
                            self.raw_log.write({"pc_t_ns": pc_t_ns, "endpoint": f"{BASE_PATH}/events",
                                                "comment": text})
                            continue
                        self._on_event(events_csv, pc_t_ns, name, text)
                        summary["events"] += 1
                        if not self._active():
                            break
                except Exception as exc:
                    if self._active():
                        summary["error"] = str(exc)
                        self._error(f"/events connection {connection}: {type(exc).__name__}: {exc}")
                finally:
                    with self._lock:
                        self.stats["connected"] = False
                    stream.close()
                    self._stream = None
                    summary["closed_pc_t_ns"] = time.time_ns()
                    self.connections.append(summary)
                if self._active():
                    self._count("reconnects")
                    self.stop_event.wait(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF_S)
        finally:
            events_csv.close()

    def _on_event(self, events_csv, pc_t_ns, name, text):
        try:
            data = json.loads(text)
        except ValueError:
            data = None
        self.raw_log.write({"pc_t_ns": pc_t_ns, "endpoint": f"{BASE_PATH}/events", "status": 200,
                            "event": name, "body": data if data is not None else text})
        self._count("events")
        if not isinstance(data, dict):
            return
        if name == "motion":
            events_csv.write({"pc_t_ns": pc_t_ns, "device_timestamp_ms": data.get("timestamp_ms"),
                              "event": "motion", "state": data.get("state"), "score": data.get("score")})
            with self._lock:
                self.stats["motion_events"] += 1
                self.stats["state"], self.stats["score"] = data.get("state"), data.get("score")
        elif name == "fault":
            events_csv.write({"pc_t_ns": pc_t_ns, "device_timestamp_ms": data.get("timestamp_ms"),
                              "event": "fault", "message": data.get("message")})
            self._count("faults")
            self.log.warning("fault event from board: %s", data.get("message"))
        elif name == "sensing" and not data.get("ready", True):
            self.log.info("sensing not ready (mode=%s, calibrating=%s)",
                          data.get("mode"), data.get("calibrating"))

    # ------------------------------------------------------------------ finish
    def finalize(self, status):
        """Wait for the board to resume, merge CSI parts, write the final session.json."""
        self.end_ns = time.time_ns()
        client = BoardClient(self.host, self.port, self.raw_log)
        sensing_end = None
        deadline = time.monotonic() + (RESUME_WAIT_S if self.mode == "csi" else 0)
        while True:
            try:
                sensing_end = client.get_json("sensing", retries=1)
            except RuntimeError as exc:
                self._error(f"reading /sensing after the session failed: {exc}")
            if self.mode != "csi" or (sensing_end and sensing_end.get("mode") != "csi_collection") \
                    or time.monotonic() >= deadline:
                break
            time.sleep(1.0)
        if self.mode == "csi":
            if sensing_end and sensing_end.get("mode") == "csi_collection":
                self._error("board still reports sensing.mode = csi_collection after closing /csi")
            elif sensing_end:
                self.log.info("board resumed sensing (mode=%s, ready=%s)",
                              sensing_end.get("mode"), sensing_end.get("ready"))
        self.info["sensing_end"] = sensing_end

        npz_summary = {}
        if self.mode == "csi" and self.recorder is not None and self.recorder.count:
            try:
                path, npz_summary = merge_parts(self.dir, self._npz_meta())
                npz_summary["path"] = path.name if path else None
            except Exception as exc:
                self._error(f"merging CSI parts into csi.npz failed (parts kept): {exc}")
        self.write_session_json(status, npz_summary)
        self.raw_log.close()
        close_error_logger(self.log)

    def csi_totals(self):
        """Device-side counters summed over all /csi connections (each resets them)."""
        keys = ("raw_drop_total", "send_backpressure_total", "fresh_record_total",
                "host_sequence_gaps", "out_of_order")
        return {key: sum(int(c.get(key) or 0) for c in self.connections) for key in keys}

    def _npz_meta(self):
        return {
            "label": self.label,
            "csi_target_pps": (self.info.get("sensing") or {}).get("csi_target_pps"),
            "collected_at": iso(self.start_ns),
            "first_packet_info": self.recorder.first_packet_info,
            "totals": self.csi_totals(),
        }

    def _clock_summary(self):
        """PC-vs-device clock offset at start/end and the drift between them."""
        if not self.clock_pairs:
            return {"pairs": 0}
        (pc0, dev0), (pc1, dev1) = self.clock_pairs[0], self.clock_pairs[-1]
        out = {
            "pairs": len(self.clock_pairs),
            "note": "offset_ms = pc_time_ms - device timestamp_ms (from /health polls, see health.csv)",
            "first": {"pc_t_ns": pc0, "device_timestamp_ms": dev0, "offset_ms": pc0 / 1e6 - dev0},
            "last": {"pc_t_ns": pc1, "device_timestamp_ms": dev1, "offset_ms": pc1 / 1e6 - dev1},
        }
        if dev1 > dev0:
            out["drift_ppm"] = ((pc1 - pc0) / 1e6 - (dev1 - dev0)) / (dev1 - dev0) * 1e6
        return out

    def write_session_json(self, status, npz_summary=None):
        device = self.info.get("device") or {}
        sensing_start = self.info.get("sensing") or {}
        sensing_end = self.info.get("sensing_end")
        changed = []
        if sensing_end:
            changed = sorted(k for k in set(sensing_start) | set(sensing_end)
                             if k not in VOLATILE_SENSING_FIELDS and sensing_start.get(k) != sensing_end.get(k))
        recorded = self.recorder.count if self.recorder else 0
        with self._lock:
            stats = dict(self.stats)
        session = {
            "tool": "csi_logger", "status": status, "mode": self.mode, "label": self.label,
            "target": self.host, "port": self.port,
            "requested_duration_s": self.duration_s, "start_delay_s": self.start_delay_s,
            "start_pc_t_ns": self.start_ns, "start_iso": iso(self.start_ns) if self.start_ns else None,
            "end_pc_t_ns": self.end_ns, "end_iso": iso(self.end_ns) if self.end_ns else None,
            "actual_duration_s": (self.end_ns - self.start_ns) / 1e9 if self.end_ns else None,
            "firmware": device.get("firmware"), "chip": device.get("chip"),
            "device": device, "capabilities": self.info.get("capabilities"),
            "sensing_start": sensing_start, "sensing_end": sensing_end,
            "sensing_settings_changed": changed,
            "wifi_start": self.info.get("wifi"), "health_start": self.info.get("health"),
            "counts": {
                "records_received": recorded,
                "records_dropped_device": self.csi_totals()["raw_drop_total"] if self.mode == "csi" else None,
                "diag_last_raw_drop_total": stats["diag_raw_drop_total"],
                "events_received": stats["events"], "motion_events": stats["motion_events"],
                "fault_events": stats["faults"],
                "diag_last_dropped_motion_events": stats["diag_dropped_motion_events"],
                "reconnects": stats["reconnects"], "errors": stats["errors"],
            },
            "csi_totals": self.csi_totals() if self.mode == "csi" else None,
            "first_packet_info": self.recorder.first_packet_info if self.recorder else None,
            "csi_npz": npz_summary or None,
            "connections": self.connections,
            "clock": self._clock_summary(),
        }
        tmp = self.dir / "session.json.tmp"
        tmp.write_text(json.dumps(session, indent=2, default=str), encoding="utf-8")
        tmp.replace(self.dir / "session.json")

    # ------------------------------------------------------------------ console
    def status_line(self, records_per_s):
        with self._lock:
            s = dict(self.stats)

        def fmt(value, spec):
            return format(value, spec) if isinstance(value, (int, float)) else "-"

        parts = [f"{self.host:<15}", "up" if s["connected"] else "DOWN"]
        if self.mode == "csi":
            parts.append(f"rec/s {records_per_s:6.1f}  total {s['records']}")
        else:
            parts.append(f"state {s['state'] or '-':<6} score {fmt(s['score'], '.3f')}  ev {s['motion_events']}")
        parts.append(f"occ {fmt(s['occupancy'], '.2f')}")
        parts.append(f"pps {fmt(s['accepted_pps'], '.0f')}")
        parts.append(f"rssi {fmt(s['rssi'], '.0f')}")
        if self.mode == "csi":
            parts.append(f"dev_drops {fmt(s['diag_raw_drop_total'], 'd')}")
        if s["errors"]:
            parts.append(f"errors {s['errors']}")
        return " | ".join(parts)


__all__ = ["BoardSession", "SetupError", "check_board", "DIRECT_PORT"]
