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

Tests: `python -m pytest tests -q`
