"""Offline replay of a csi_logger recording through ESPectre's detectors.

Runs the repository's Python Lightweight and High-Accuracy detectors with the
same temporal sampler, evaluation cadence (every EVALUATION_INTERVAL_MS) and
motion hit filter as the firmware, and writes one row per evaluation to
replay.csv, using the PC receive time of the CSI record that triggered it.
"""

import csv
import json
import os
from pathlib import Path

import numpy as np

from csi_store import PARTS_DIR, merge_from_session_json

REPLAY_FIELDS = [
    "detector", "eval_index", "pc_t_ns", "device_ticks_us", "wifi_rx_ts_us", "csi_row",
    "ready", "raw_state", "state", "score", "threshold",
]


def find_recordings(path):
    """Return the board folders / NPZ files under ``path`` (session or board folder)."""
    path = Path(path)
    if path.is_file():
        return [path]
    if (path / "csi.npz").exists() or (path / PARTS_DIR).exists():
        return [path]
    return sorted(p for p in path.iterdir()
                  if p.is_dir() and ((p / "csi.npz").exists() or (p / PARTS_DIR).exists()))


def ensure_npz(recording):
    """Return the csi.npz path, merging leftover part files first if needed."""
    recording = Path(recording)
    if recording.is_file():
        return recording
    npz = recording / "csi.npz"
    if not npz.exists():
        print(f"  {recording.name}: csi.npz missing, merging {PARTS_DIR}/ ...")
        merge_from_session_json(recording)
    if not npz.exists():
        raise SystemExit(f"No CSI data found in {recording}")
    return npz


def load_packets(npz_path):
    """Load packets with ESPectre's loader and attach the PC receive time.

    load_npz_as_packets() keeps only the HT20 sensing view. The same
    ht20_packet_mask() tells which NPZ rows survived, so each packet can be
    matched back to its pc_t_ns.
    """
    from tools.lib.csi_io import ht20_packet_mask, load_npz_as_packets

    packets = load_npz_as_packets(Path(npz_path))
    with np.load(npz_path, allow_pickle=False) as data:
        n_rows, row_len = data["csi_data"].shape
        num_sc = int(data["num_subcarriers"]) if "num_subcarriers" in data else row_len // 2
        pc = data["pc_t_ns"] if "pc_t_ns" in data else None
        mask = ht20_packet_mask(
            data["phy_mode"] if "phy_mode" in data else None,
            data["ltf_type"] if "ltf_type" in data else None,
            data["channel_width"] if "channel_width" in data else None,
            row_len, num_sc, n_rows,
        )
    rows = np.arange(n_rows) if mask is None else np.flatnonzero(mask)
    if len(rows) != len(packets):
        print("  warning: could not match packets to NPZ rows; pc_t_ns left empty")
        rows = None
    for i, packet in enumerate(packets):
        row = int(rows[i]) if rows is not None else None
        packet["csi_row"] = row
        packet["pc_t_ns"] = int(pc[row]) if (pc is not None and row is not None) else None
    return packets, n_rows


def _parse_threshold(text):
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def build_detector(name, packets, sensing, threshold_option):
    """Create a detector configured like the firmware. Returns (detector, threshold_source)."""
    import config
    from tools.lib.dataset_metadata import build_calibrated_lightweight_detector, build_lightweight_detector
    from tools.lib.dataset_metadata import measure_packet_interval_us
    from tools.lib.high_accuracy_detector import HIGH_ACCURACY_DEFAULT_THRESHOLD, HighAccuracyDetector
    from tools.lib.lightweight_detector import LightweightDetector
    from tools.lib.runtime_policy import derive_detector_timing, nominal_packet_interval_us
    from tools.lib.runtime_policy import production_filter_kwargs
    from tools.lib.temporal_csi_sampler import minimum_valid_slots, temporal_window_slots
    from tools.lib.temporal_replay import target_pps_for_packets

    target_pps = target_pps_for_packets(packets, measure_packet_interval_us(packets))
    window = temporal_window_slots(target_pps, config.SEGMENTATION_WINDOW_SIZE_MS)
    board_threshold = sensing.get("threshold") if sensing.get("detector") == name else None
    fixed = _parse_threshold(threshold_option)

    if name == "lightweight":
        detector, source = None, None
        if threshold_option == "calibrate":
            # Same startup calibration the firmware runs on a quiet room
            # (CALIBRATION_DURATION_MS from the start of the recording).
            calibrated = build_calibrated_lightweight_detector(packets)
            if calibrated is not None:
                detector, source = calibrated[0], "startup calibration on the first seconds of the recording"
        if detector is None:
            timing = derive_detector_timing(nominal_packet_interval_us(target_pps),
                                            config.SEGMENTATION_WINDOW_SIZE_MS)
            timing["window_packets"] = window
            detector = build_lightweight_detector(threshold=LightweightDetector.BASE_THRESHOLD,
                                                  timing=timing)
            source = ("calibration failed (motion at start?); using BASE_THRESHOLD"
                      if threshold_option == "calibrate" else "BASE_THRESHOLD")
        if threshold_option == "board":
            if board_threshold is None:
                raise SystemExit("--lw-threshold board: the board was not running the Lightweight "
                                 "detector during the recording (see session.json sensing_start)")
            detector.set_threshold(float(board_threshold))
            source = "board sensing.threshold"
        elif fixed is not None:
            detector.set_threshold(fixed)
            source = "command line"
        return detector, source

    # High-Accuracy detector: fixed probability threshold, no calibration.
    detector = HighAccuracyDetector(window_size=window, threshold=HIGH_ACCURACY_DEFAULT_THRESHOLD,
                                    **production_filter_kwargs())
    detector.set_minimum_valid_samples(minimum_valid_slots(window))
    source = "HIGH_ACCURACY_DEFAULT_THRESHOLD"
    if threshold_option == "board":
        if board_threshold is None:
            raise SystemExit("--ha-threshold board: the board was not running the High-Accuracy "
                             "detector during the recording (see session.json sensing_start)")
        detector.set_threshold(float(board_threshold))
        source = "board sensing.threshold"
    elif fixed is not None:
        detector.set_threshold(fixed)
        source = "command line"
    return detector, source


def replay_detector(name, detector, packets, motion_on_hits, motion_off_hits):
    """Replay all packets; return one dict per detector evaluation.

    Mirrors tools/lib/performance_report.py replay_idle_stream(): temporal
    admission, evaluation cadence, warm-up, and the firmware hit filter.
    """
    import config
    from detector_interface import MotionState
    from tools.lib.dataset_metadata import measure_packet_interval_us
    from tools.lib.performance_report import timing_cadence_for_window
    from tools.lib.runtime_policy import RuntimeMotionPolicy
    from tools.lib.temporal_replay import (apply_temporal_admission, iter_temporal_admissions,
                                           packet_timestamp_us, target_pps_for_packets)

    interval_us = measure_packet_interval_us(packets)
    target_pps = target_pps_for_packets(packets, interval_us)
    window = detector.get_window_size()
    _, cadence = timing_cadence_for_window(window, interval_us)
    hits = RuntimeMotionPolicy(motion_on_hits=motion_on_hits, motion_off_hits=motion_off_hits)

    def label(state):
        return "motion" if state == MotionState.MOTION else "idle"

    rows = []
    for admission in iter_temporal_admissions(packets, target_pps=target_pps,
                                              window_size_ms=config.SEGMENTATION_WINDOW_SIZE_MS,
                                              fallback_interval_us=interval_us):
        packet = admission.packet
        if admission.reset_required:
            cadence.reset()
        apply_temporal_admission(detector, admission)
        cadence.note_packet(elapsed_us=admission.coverage_us)
        due = cadence.should_evaluate()
        if due:
            cadence.after_evaluation()
        # A plain list is much faster for the detector's per-element reads.
        detector.process_packet(
            packet["csi_data"].tolist(), config.DEFAULT_SUBCARRIERS,
            rssi_dbm=packet.get("rssi_dbm"),
            timestamp_us=packet_timestamp_us(packet, admission.packet_index, interval_us),
        )
        if not due:
            continue
        detector.update_state()
        ready = admission.slot_index + 1 >= window and detector.is_ready()
        raw_state = detector.get_state()
        state, _ = hits.apply_state(raw_state, ready=ready)
        rows.append({
            "detector": name, "eval_index": len(rows), "pc_t_ns": packet.get("pc_t_ns"),
            "device_ticks_us": packet.get("device_ticks_us"), "wifi_rx_ts_us": packet.get("wifi_rx_ts_us"),
            "csi_row": packet.get("csi_row"), "ready": int(ready),
            "raw_state": label(raw_state), "state": label(state),
            "score": round(float(detector.get_motion_metric()), 6),
            "threshold": round(float(detector.get_threshold()), 6),
        })
    return rows


def replay_recording(recording, out_path, detectors, lw_threshold, ha_threshold):
    import config

    npz = ensure_npz(recording)
    board_dir = npz.parent
    # Keep ESPectre's NPZ cache next to the recording instead of inside the repo.
    os.environ.setdefault("ESPECTRE_NPZ_CACHE_DIR", str(board_dir / ".npz_cache"))
    session = {}
    if (board_dir / "session.json").exists():
        session = json.loads((board_dir / "session.json").read_text(encoding="utf-8"))
    sensing = session.get("sensing_start") or {}
    on_hits = int(sensing.get("motion_on_hits") or config.MOTION_ON_HITS)
    off_hits = int(sensing.get("motion_off_hits") or config.MOTION_OFF_HITS)

    packets, n_rows = load_packets(npz)
    print(f"  {npz}: {len(packets)} sensing packets of {n_rows} records")
    summary = {"recording": str(npz), "packets": len(packets), "motion_on_hits": on_hits,
               "motion_off_hits": off_hits, "board_detector": sensing.get("detector"),
               "board_threshold": sensing.get("threshold"), "detectors": {}}
    all_rows = []
    for name in detectors:
        option = lw_threshold if name == "lightweight" else ha_threshold
        detector, source = build_detector(name, packets, sensing, option)
        start_threshold = detector.get_threshold()
        rows = replay_detector(name, detector, packets, on_hits, off_hits)
        ready = [r for r in rows if r["ready"]]
        motion = sum(r["state"] == "motion" for r in ready)
        summary["detectors"][name] = {
            "threshold_start": start_threshold, "threshold_source": source,
            "threshold_end": detector.get_threshold(), "window_packets": detector.get_window_size(),
            "evaluations": len(rows), "ready_evaluations": len(ready),
            "motion_fraction": motion / len(ready) if ready else None,
        }
        print(f"  {name:<13} threshold {start_threshold:.3f} ({source}); "
              f"{len(rows)} evaluations, motion in {motion}/{len(ready)} ready ones")
        all_rows.extend(rows)

    out_path = Path(out_path) if out_path else board_dir / "replay.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPLAY_FIELDS)
        writer.writeheader()
        writer.writerows(all_rows)
    out_path.with_name(out_path.stem + "_summary.json").write_text(
        json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(f"  wrote {out_path}")
    return out_path, summary


def run_replay(args):
    recordings = find_recordings(args.recording)
    if not recordings:
        raise SystemExit(f"No csi_logger recording found under {args.recording}")
    if args.out and len(recordings) > 1:
        raise SystemExit("--out can only be used with a single board recording")
    for recording in recordings:
        replay_recording(recording, args.out, args.detectors, args.lw_threshold, args.ha_threshold)
    return 0
