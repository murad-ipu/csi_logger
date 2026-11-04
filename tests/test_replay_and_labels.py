"""Offline replay on the repo's real S3 recordings, and the labels evaluation."""

import csv
import shutil

import numpy as np
import pytest

import evaluate_labels
from replay import replay_recording


def _fixture(repo_root, label):
    files = sorted((repo_root / "data" / label).glob(f"{label}_s3_64sc_*.npz"))
    if not files:
        pytest.skip(f"no S3 {label} fixture in the repo data folder")
    return files[0]


def _as_csi_logger_recording(src, board_dir):
    """Copy an ESPectre dataset NPZ and add the pc_t_ns array csi_logger stores."""
    board_dir.mkdir(parents=True)
    with np.load(src, allow_pickle=False) as data:
        arrays = {key: data[key] for key in data.files}
    n = len(arrays["csi_data"])
    arrays["pc_t_ns"] = 1_700_000_000_000_000_000 + np.arange(n, dtype=np.int64) * 10_000_000
    np.savez_compressed(board_dir / "csi.npz", **arrays)
    return board_dir


def _replay(repo_root, tmp_path, label):
    board = _as_csi_logger_recording(_fixture(repo_root, label), tmp_path / label)
    out, summary = replay_recording(board, None, ["lightweight", "high_accuracy"], "calibrate", "default")
    with open(out, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    shutil.rmtree(board / ".npz_cache", ignore_errors=True)
    return rows, summary


def test_replay_produces_both_detectors_on_pc_timeline(repo_root, tmp_path):
    rows, summary = _replay(repo_root, tmp_path, "empty")
    for name in ("lightweight", "high_accuracy"):
        det = [r for r in rows if r["detector"] == name]
        assert det, name
        assert all(0.0 <= float(r["score"]) <= 1.0 for r in det)
        times = [int(r["pc_t_ns"]) for r in det]
        assert times == sorted(times)
        # Evaluations every 250 ms -> about 4 per second of recording.
        span_s = (times[-1] - times[0]) / 1e9
        assert 3.0 < (len(det) - 1) / span_s < 5.0
        assert summary["detectors"][name]["ready_evaluations"] > 0


def test_motion_recording_scores_more_motion_than_empty(repo_root, tmp_path):
    _, empty = _replay(repo_root, tmp_path, "empty")
    _, motion = _replay(repo_root, tmp_path, "motion")
    for name in ("lightweight", "high_accuracy"):
        assert motion["detectors"][name]["motion_fraction"] > empty["detectors"][name]["motion_fraction"]


def test_evaluate_labels_metrics():
    s = 1_000_000_000
    # empty 0-100 s, walk 100-110 s, empty 110-200 s, walk 200-210 s, end at 220 s
    phases = [(0, 100 * s, "empty"), (100 * s, 110 * s, "walk"), (110 * s, 200 * s, "empty"),
              (200 * s, 210 * s, "walk"), (210 * s, 220 * s, "end")]
    samples = []
    for i in range(0, 220 * 4):                     # 4 samples per second
        t = i * s // 4
        motion = (102 * s <= t < 112 * s)           # detected 2 s after walk #1, carries 2 s into empty
        motion |= (50 * s <= t < 51 * s)            # one false alarm in the first empty phase
        samples.append((t, motion))
    report = evaluate_labels.evaluate(phases, samples)
    assert report["empty"]["false_alarms"] == 1     # carry-over at 110 s is not a new alarm
    assert report["empty"]["false_alarms_per_hour"] == pytest.approx(1 / (190 / 3600))
    assert report["walk"]["detected_pct"] == pytest.approx(100 * 8 / 20)
    assert [p["delay_s"] for p in report["walk"]["phases"]] == [2.0, None]
    assert report["walk"]["missed_phases"] == 1


def test_evaluate_labels_reads_events_csv(tmp_path):
    labels = tmp_path / "labels.csv"
    labels.write_text("t_ns,label\n0,empty\n4000000000,walk\n8000000000,end\n", encoding="utf-8")
    events = tmp_path / "events.csv"
    lines = ["pc_t_ns,device_timestamp_ms,event,state,score,message"]
    for i in range(32):
        t = i * 250_000_000
        lines.append(f"{t},{i * 250},motion,{'motion' if t >= 5_000_000_000 else 'idle'},0.5,")
    lines.append("1000,1,fault,,,boom")
    events.write_text("\n".join(lines) + "\n", encoding="utf-8")
    report = evaluate_labels.evaluate(evaluate_labels.read_labels(labels),
                                      evaluate_labels.read_detections(events))
    assert report["walk"]["phases"][0]["delay_s"] == 1.0
    assert report["empty"]["false_alarms"] == 0
