"""End-to-end runs of csi_logger.py against the fake board in fake_board.py."""

import csv
import json
import threading

import numpy as np
import pytest

import csi_logger
from board_session import SetupError, check_board
from fake_board import FakeBoard
from http_api import BoardClient


def _rows(path):
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _only_board_dir(out):
    (session,) = list(out.iterdir())
    (board,) = list(session.iterdir())
    return board


def test_csi_mode_records_everything_and_releases_the_board(tmp_path):
    with FakeBoard() as board:
        code = csi_logger.main(["csi", "--target", f"127.0.0.1:{board.port}", "--duration", "3",
                                "--label", "empty", "--out", str(tmp_path)])
        assert code == 0
        assert not board.csi_open                     # /csi connection was closed
    folder = _only_board_dir(tmp_path)
    session = json.loads((folder / "session.json").read_text())
    assert session["status"] == "complete" and session["label"] == "empty"
    assert session["firmware"] == "3.1.0"
    assert session["sensing_end"]["mode"] == "sensing"
    assert session["sensing_settings_changed"] == []
    records = session["counts"]["records_received"]
    assert records > 150                              # ~100 records/s for 3 s

    assert len(_rows(folder / "csi_amplitudes.csv")) == records
    with np.load(folder / "csi.npz") as data:
        assert len(data["csi_data"]) == records and "pc_t_ns" in data.files
    diag = _rows(folder / "diagnostics.csv")
    assert len(diag) >= 2 and "raw_csi.raw_drop_total" in diag[0]
    assert _rows(folder / "health.csv") and _rows(folder / "wifi.csv")
    raw = [json.loads(line) for line in (folder / "raw_http.jsonl").read_text().splitlines()]
    assert {"/espectre/v1/diagnostics", "/espectre/v1/csi", "/espectre/v1/health"} <= {r["endpoint"] for r in raw}


def test_events_mode_saves_motion_and_fault_events(tmp_path):
    with FakeBoard() as board:
        code = csi_logger.main(["events", "--target", f"127.0.0.1:{board.port}", "--duration", "3",
                                "--out", str(tmp_path)])
        assert code == 0
    folder = _only_board_dir(tmp_path)
    rows = _rows(folder / "events.csv")
    motion = [r for r in rows if r["event"] == "motion"]
    assert len(motion) >= 8                            # 4 Hz for ~3 s
    assert {r["state"] for r in motion} <= {"idle", "motion"}
    assert any(r["event"] == "fault" and r["message"] == "test fault" for r in rows)
    session = json.loads((folder / "session.json").read_text())
    assert session["counts"]["motion_events"] == len(motion)


@pytest.mark.parametrize("mode", ["csi", "events"])
def test_survives_board_outage_and_reconnects(tmp_path, mode):
    with FakeBoard() as board:
        threading.Timer(1.5, lambda: setattr(board, "offline", True)).start()
        threading.Timer(4.5, lambda: setattr(board, "offline", False)).start()
        code = csi_logger.main([mode, "--target", f"127.0.0.1:{board.port}", "--duration", "9",
                                "--out", str(tmp_path)])
        assert code == 0
    folder = _only_board_dir(tmp_path)
    session = json.loads((folder / "session.json").read_text())
    assert session["status"] == "complete"
    assert session["counts"]["reconnects"] >= 1 and session["counts"]["errors"] >= 1
    assert "failed" in (folder / "errors.log").read_text()
    # Data arrived both before and after the outage.
    if mode == "csi":
        connections = [c for c in session["connections"] if c["records"] > 0]
    else:
        connections = [c for c in session["connections"] if c["events"] > 0]
    assert len(connections) >= 2


def test_capability_check_rejects_board_without_csi():
    with FakeBoard(csi_supported=False) as board:
        client = BoardClient("127.0.0.1", board.port)
        with pytest.raises(SetupError, match="not supported"):
            check_board(client, "csi")
        assert check_board(client, "events")["device"]["firmware"] == "3.1.0"


def test_dry_run_reports_ok(capsys):
    with FakeBoard() as board:
        assert csi_logger.main(["events", "--target", f"127.0.0.1:{board.port}", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "firmware=3.1.0" in out and "/events      : OK" in out
