# csi_logger

Data-collection tool for ESPectre Native boards (Direct HTTP API, port 62587).
It only sends GET requests and never changes board settings.

Needs: Python 3.12+, `requests`, `numpy`, and the ESPectre clone at
`..\espectre` (or pass `--repo <path>` / set `ESPECTRE_REPO`).

| File | Purpose |
| --- | --- |
| `csi_logger.py` | CLI: `csi`, `events`, `replay`, plus `--dry-run` |
| `board_session.py` | Per-board threads: pollers, `/csi` or `/events` worker, `session.json` |
| `csi_store.py` | CSI CSV rows, 10 s part files, final `csi.npz` (ESPectre dataset format) |
| `http_api.py` | GET-only JSON client with retry, logs every reply to `raw_http.jsonl` |
| `sse.py` | SSE parser and `/events` connection |
| `replay.py` | Offline Lightweight + High-Accuracy replay using the repo's detectors |
| `writers.py` | CSV/JSONL writers that flush every second, `errors.log` |
| `evaluate_labels.py` | Score events/replay against `labels.csv` |
| `espectre_path.py` | Finds the ESPectre clone and puts it on `sys.path` |

Output: `sessions\<date>_<mode>_<label>\<board-ip>\` containing `session.json`,
`raw_http.jsonl`, `errors.log`, `diagnostics.csv`, `health.csv`, `wifi.csv`, and
`csi.npz` + `csi_amplitudes.csv` (csi mode) or `events.csv` (events mode).
`replay` adds `replay.csv` and `replay_summary.json`.

Install (once, from this folder):

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Only `numpy` and `requests` are needed at runtime; you do not need to install
the ESPectre repo's own (much larger) `requirements.txt`.

Tests: `python -m pytest tests -q`

Commands (PowerShell)

# 60 s connectivity test
python csi_logger.py events --target <BOARD_IP> --dry-run
python csi_logger.py csi --target <BOARD_IP> --duration 60 --label conntest

# 10-minute empty-room CSI recording (30 s to leave the room)
python csi_logger.py csi --target <BOARD_IP> --duration 600 --label empty --start-delay 30

# 5-minute events recording
python csi_logger.py events --target <BOARD_IP> --duration 300 --label walk

# Offline replay (session folder is printed at start)
python csi_logger.py replay sessions\<YYYYmmdd_HHMMSS>_csi_empty

# Score against labels.csv
python evaluate_labels.py --labels labels.csv --events sessions\<session>\<BOARD_IP>\events.csv
python evaluate_labels.py --labels labels.csv --replay sessions\<session>\<BOARD_IP>\replay.csv --detector lightweight
For several boards, pass --target IP1 IP2 .... The events dry run uses one of the two SSE slots for up to 12 s.