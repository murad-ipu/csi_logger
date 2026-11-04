#!/usr/bin/env python3
"""csi_logger - record raw CSI or motion events from ESPectre boards, and replay CSI offline.

Modes:
  csi     GET /espectre/v1/csi for --duration seconds (board pauses motion events meanwhile)
  events  GET /espectre/v1/events (SSE) for --duration seconds
  replay  run ESPectre's Lightweight and High-Accuracy detectors on a saved recording

Examples:
  python csi_logger.py csi    --target 192.168.1.50 --duration 60 --dry-run
  python csi_logger.py csi    --target 192.168.1.50 --duration 600 --label empty
  python csi_logger.py events --target 192.168.1.50 192.168.1.51 --duration 300 --label walk
  python csi_logger.py replay sessions/20261104_101500_csi_empty
"""

import argparse
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import espectre_path
from http_api import DIRECT_PORT

STOP_JOIN_TIMEOUT_S = 15.0


def build_parser():
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--repo", help="path to the ESPectre clone (default: ../espectre or $ESPECTRE_REPO)")

    record = argparse.ArgumentParser(add_help=False, parents=[common])
    record.add_argument("--target", nargs="+", required=True, metavar="IP",
                        help="board IP address(es); use host:port for a non-default port")
    record.add_argument("--duration", type=float, help="recording length in seconds")
    record.add_argument("--label", default="unlabeled", help="free-text label stored with the session")
    record.add_argument("--out", default="sessions", help="parent folder for session folders")
    record.add_argument("--start-delay", type=float, default=0.0,
                        help="seconds to wait before starting (all boards start together)")
    record.add_argument("--dry-run", action="store_true",
                        help="only check connectivity; print capabilities, sensing and diagnostics")

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="mode", required=True)
    sub.add_parser("csi", parents=[record], help="record raw CSI from GET /csi")
    sub.add_parser("events", parents=[record], help="record motion/fault events from GET /events")
    rp = sub.add_parser("replay", parents=[common], help="run the detectors offline on a CSI recording")
    rp.add_argument("recording", help="session folder, board folder, or csi.npz")
    rp.add_argument("--out", help="output CSV (default: <board folder>/replay.csv)")
    rp.add_argument("--detectors", nargs="+", default=["lightweight", "high_accuracy"],
                    choices=["lightweight", "high_accuracy"])
    rp.add_argument("--lw-threshold", default="calibrate",
                    help="'calibrate' (firmware startup calibration on the first seconds, default), "
                         "'board' (sensing.threshold from session.json), 'base', or a number 0-1")
    rp.add_argument("--ha-threshold", default="default",
                    help="'default' (0.5), 'board' (sensing.threshold from session.json), or a number 0-1")
    return parser


def split_target(text):
    host, sep, port = text.rpartition(":")
    if sep and port.isdigit() and host:
        return host, int(port)
    return text, DIRECT_PORT


def safe_name(text):
    return re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("_") or "x"


# ---------------------------------------------------------------------- dry run
def dry_run(args):
    from board_session import SetupError, check_board
    from http_api import BoardClient

    ok = True
    for target in args.target:
        host, port = split_target(target)
        print(f"\n=== {host}:{port} ({args.mode} mode) ===")
        client = BoardClient(host, port)
        try:
            info = check_board(client, args.mode)
        except SetupError as exc:
            print(f"  FAIL: {exc}")
            ok = False
            continue
        caps, device = info["capabilities"], info.get("device") or {}
        sensing, wifi = info.get("sensing") or {}, info.get("wifi") or {}
        print(f"  device       : {device.get('name')} id={device.get('device_id')} chip={device.get('chip')} "
              f"frontend={device.get('frontend')} firmware={device.get('firmware')}")
        print(f"  protocol     : {caps.get('protocol_version')}  resources={caps.get('resources')}")
        print(f"  events       : {caps.get('events')}")
        print(f"  features     : {caps.get('features')}")
        print(f"  csi          : {caps.get('csi')}")
        print("  sensing      : " + ", ".join(f"{k}={v}" for k, v in sensing.items()))
        print(f"  wifi         : ssid={wifi.get('ssid')} ch={wifi.get('channel')} band={wifi.get('band')} "
              f"rssi={wifi.get('rssi_dbm')} connected={wifi.get('connected')}")
        try:
            diag = client.read_diagnostics()
            keys = ["csi_accepted_pps", "csi_callback_pps", "csi_occupancy", "wifi_rssi_dbm",
                    "generator_pps", "csi_hw_error_total", "csi_pending_frame_drops_total"]
            print("  diagnostics  : " + ", ".join(f"{k}={diag.get(k)}" for k in keys))
            print(f"  raw_csi      : {diag.get('raw_csi')}")
            print(f"  direct_http  : {diag.get('direct_http')}")
        except RuntimeError as exc:
            print(f"  diagnostics  : FAIL {exc}")
            ok = False
        if args.mode == "events":
            ok &= _probe_events(host, port)
        print("  result       : OK" if ok else "  result       : problems found")
    return 0 if ok else 1


def _probe_events(host, port):
    """Open /events briefly and wait for the first event or heartbeat (uses one SSE slot for <=12 s)."""
    from sse import EventStream

    stream = EventStream(host, port, read_timeout=12.0)
    try:
        stream.open()
        name, text = next(stream.events())
        print(f"  /events      : OK, first item '{name}': {text[:100]}")
        return True
    except Exception as exc:
        print(f"  /events      : FAIL {type(exc).__name__}: {exc}")
        return False
    finally:
        stream.close()


# ---------------------------------------------------------------------- record
def record(args):
    from board_session import BoardSession, SetupError, check_board
    from http_api import BoardClient

    if not args.duration or args.duration <= 0:
        raise SystemExit("--duration (seconds, > 0) is required")

    # 1. Check every board before touching anything.
    checked = []
    for target in args.target:
        host, port = split_target(target)
        try:
            checked.append((host, port, check_board(BoardClient(host, port), args.mode)))
        except SetupError as exc:
            print(f"ERROR: {exc}")
            return 2

    # 2. Session folder: <out>/<date>_<mode>_<label>/<board>/
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    session_dir = Path(args.out) / f"{stamp}_{args.mode}_{safe_name(args.label)}"
    sessions = [BoardSession(host, port, args.mode, session_dir / safe_name(host), args.duration,
                             args.label, args.start_delay, info) for host, port, info in checked]
    print(f"Session folder: {session_dir.resolve()}")
    for s in sessions:
        firmware = (s.info.get("device") or {}).get("firmware")
        sensing = s.info.get("sensing") or {}
        print(f"  {s.host}: firmware {firmware}, detector {sensing.get('detector')}, "
              f"traffic {sensing.get('traffic_generator_mode')}, target {sensing.get('csi_target_pps')} pps")
    if args.mode == "csi":
        print("Note: while /csi is open the board pauses motion events (mode = csi_collection).")

    status = "complete"
    try:
        # 3. Optional synchronised start.
        start_at = time.monotonic() + max(0.0, args.start_delay)
        while time.monotonic() < start_at:
            print(f"\rStarting in {start_at - time.monotonic():5.1f} s (Ctrl+C to abort) ", end="", flush=True)
            time.sleep(min(0.2, max(0.0, start_at - time.monotonic())))
        print()
        t0 = time.monotonic()
        for s in sessions:
            s.start(t0)

        # 4. Live console summary once per second until the duration ends.
        last_counts = {s.host: 0 for s in sessions}
        next_tick = t0 + 1.0
        while not all(s.done() for s in sessions):
            time.sleep(max(0.0, next_tick - time.monotonic()))
            next_tick += 1.0
            elapsed = time.monotonic() - t0
            if elapsed >= args.duration:
                for s in sessions:
                    s.request_stop()
            lines = [f"[{_mmss(min(elapsed, args.duration))} / {_mmss(args.duration)}]"]
            for s in sessions:
                count = s.stats["records"]
                lines.append("  " + s.status_line(count - last_counts[s.host]))
                last_counts[s.host] = count
                if s.recorder is not None:
                    s.recorder.flush()
            print("\n".join(lines), flush=True)
    except KeyboardInterrupt:
        status = "interrupted"
        print("\nCtrl+C: stopping and closing connections (press Ctrl+C again to force)...")
    finally:
        for s in sessions:
            s.request_stop()
        try:
            for s in sessions:
                s.join(STOP_JOIN_TIMEOUT_S)
        except KeyboardInterrupt:
            status = "interrupted"
        for s in sessions:
            if s.start_ns is not None:
                s.finalize(status)
            else:
                s.raw_log.close()
    _print_summary(sessions, session_dir)
    return 0


def _mmss(seconds):
    seconds = int(seconds)
    return f"{seconds // 60:02d}:{seconds % 60:02d}"


def _print_summary(sessions, session_dir):
    print(f"\nSaved to {session_dir.resolve()}")
    for s in sessions:
        st = s.stats
        if s.mode == "csi":
            totals = s.csi_totals()
            print(f"  {s.host}: {st['records']} CSI records, device drops {totals['raw_drop_total']}, "
                  f"reconnects {st['reconnects']}, errors {st['errors']}")
        else:
            print(f"  {s.host}: {st['motion_events']} motion events, {st['faults']} faults, "
                  f"reconnects {st['reconnects']}, errors {st['errors']}")


def _check_threshold_option(flag, value, words):
    if value in words:
        return
    try:
        if 0.0 <= float(value) <= 1.0:
            return
    except ValueError:
        pass
    raise SystemExit(f"{flag} must be one of {', '.join(words)} or a number between 0 and 1")


def main(argv=None):
    args = build_parser().parse_args(argv)
    espectre_path.setup(args.repo)
    if args.mode == "replay":
        from replay import run_replay
        _check_threshold_option("--lw-threshold", args.lw_threshold, ("calibrate", "board", "base"))
        _check_threshold_option("--ha-threshold", args.ha_threshold, ("default", "board"))
        return run_replay(args)
    if args.dry_run:
        return dry_run(args)
    return record(args)


if __name__ == "__main__":
    sys.exit(main())
