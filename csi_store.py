"""Storage for raw CSI records received from GET /espectre/v1/csi.

Every record is written twice:
  * csi_amplitudes.csv - one row per record (PC time, device time, counters,
    RSSI, amplitude of every subcarrier), flushed every second;
  * csi_parts/part_NNNNN.npz - raw int8 I/Q plus metadata, one file every 10 s.
At the end the parts are merged into csi.npz, using the same array names as
ESPectre's own CSICollector.save_sample(), so tools.lib.csi_io.load_npz_as_packets()
and the repo's replay code can read it directly. If the tool crashes, the
parts survive and `replay` merges them on first use.
"""

import json
import os
import shutil
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import numpy as np

from writers import CsvLog

CHUNK_INTERVAL_S = 10.0
PARTS_DIR = "csi_parts"

# Per-record metadata columns (CSV order), followed by amp_0 .. amp_{N-1}.
RECORD_FIELDS = [
    "pc_t_ns", "connection", "seq_num", "raw_stream_sequence", "device_ticks_us",
    "wifi_rx_ts_us", "wifi_rx_start_ts_ns", "channel", "rssi_dbm", "noise_floor_dbm",
    "phy_mode", "ltf_type", "channel_width", "num_subcarriers", "fresh_record_total",
    "raw_drop_total", "raw_send_backpressure_total",
]

# Array name -> dtype for the per-record arrays kept in the chunk files.
_ARRAY_DTYPES = {
    "pc_t_ns": np.int64, "connection": np.uint16, "seq_num": np.uint32,
    "raw_stream_sequence": np.uint64, "device_ticks_us": np.uint64,
    "wifi_rx_ts_us": np.uint32, "wifi_rx_start_ts_ns": np.uint64, "channel": np.uint8,
    "rssi_dbm": np.int16, "noise_floor_dbm": np.int16, "fresh_record_total": np.uint64,
    "raw_drop_total": np.uint64, "raw_send_backpressure_total": np.uint64,
    "iq_len": np.uint16,
}
_STRING_ARRAYS = ("phy_mode", "ltf_type", "channel_width")


def _value(value, missing=0):
    return missing if value is None else value


class CsiRecorder:
    """Receives CSIPacket objects (from csi_io.DirectRawCSIReceiver callbacks)."""

    def __init__(self, board_dir):
        from tools.lib.csi_io import NOISE_FLOOR_INVALID_DBM

        self._noise_invalid = NOISE_FLOOR_INVALID_DBM
        self.board_dir = Path(board_dir)
        self.parts_dir = self.board_dir / PARTS_DIR
        self.parts_dir.mkdir(parents=True, exist_ok=True)
        self.csv = None            # created on the first record (needs subcarrier count)
        self.num_amp_columns = 0
        self.count = 0
        self.length_mismatches = 0
        self.first_packet_info = None
        self._part_index = len(list(self.parts_dir.glob("part_*.npz")))
        self._reset_buffers()
        self._last_chunk = time.monotonic()

    def _reset_buffers(self):
        self._iq = []
        self._cols = {name: [] for name in list(_ARRAY_DTYPES) + list(_STRING_ARRAYS)}

    def add(self, packet, connection):
        """Store one record. Called from the CSI receiver thread."""
        pc_t_ns = time.time_ns()
        iq = np.asarray(packet.iq_raw, dtype=np.int8)
        amplitudes = packet.amplitudes  # csi_io's own I/Q convention: |I + jQ|
        if self.csv is None:
            self.num_amp_columns = len(amplitudes)
            self.csv = CsvLog(
                self.board_dir / "csi_amplitudes.csv",
                RECORD_FIELDS + [f"amp_{i}" for i in range(self.num_amp_columns)],
            )
            self.first_packet_info = {
                "chip": str(packet.chip).lower(), "device_id": int(_value(packet.device_id)),
                "num_subcarriers": int(packet.num_subcarriers),
                "record_version": int(packet.record_version),
                "raw_protocol_version": int(_value(packet.raw_protocol_version)),
                "frontend": packet.frontend or "", "firmware_version": packet.firmware_version or "",
                "firmware_identity": packet.firmware_identity or "",
                "transport_target": packet.transport_target or "",
            }
        if len(amplitudes) != self.num_amp_columns:
            self.length_mismatches += 1

        row = {
            "pc_t_ns": pc_t_ns, "connection": connection, "seq_num": packet.seq_num,
            "raw_stream_sequence": packet.raw_stream_sequence,
            "device_ticks_us": packet.device_ticks_us, "wifi_rx_ts_us": packet.wifi_rx_ts_us,
            "wifi_rx_start_ts_ns": packet.wifi_rx_start_ts_ns, "channel": packet.channel,
            "rssi_dbm": packet.rssi_dbm, "noise_floor_dbm": packet.noise_floor_dbm,
            "phy_mode": packet.phy_mode, "ltf_type": packet.ltf_type,
            "channel_width": packet.channel_width, "num_subcarriers": packet.num_subcarriers,
            "fresh_record_total": packet.fresh_record_total,
            "raw_drop_total": packet.raw_drop_total,
            "raw_send_backpressure_total": packet.raw_send_backpressure_total,
        }
        # Amplitude columns are fixed by the first record; pad/truncate others.
        for i in range(self.num_amp_columns):
            row[f"amp_{i}"] = f"{amplitudes[i]:.2f}" if i < len(amplitudes) else ""
        self.csv.write(row)

        self._iq.append(iq)
        cols = self._cols
        cols["pc_t_ns"].append(pc_t_ns)
        cols["connection"].append(connection)
        cols["seq_num"].append(packet.seq_num)
        cols["raw_stream_sequence"].append(_value(packet.raw_stream_sequence))
        cols["device_ticks_us"].append(_value(packet.device_ticks_us))
        cols["wifi_rx_ts_us"].append(_value(packet.wifi_rx_ts_us))
        cols["wifi_rx_start_ts_ns"].append(_value(packet.wifi_rx_start_ts_ns))
        cols["channel"].append(_value(packet.channel))
        cols["rssi_dbm"].append(_value(packet.rssi_dbm))
        cols["noise_floor_dbm"].append(_value(packet.noise_floor_dbm, self._noise_invalid))
        cols["fresh_record_total"].append(_value(packet.fresh_record_total))
        cols["raw_drop_total"].append(_value(packet.raw_drop_total))
        cols["raw_send_backpressure_total"].append(_value(packet.raw_send_backpressure_total))
        cols["iq_len"].append(len(iq))
        for name in _STRING_ARRAYS:
            cols[name].append(str(getattr(packet, name)))
        self.count += 1

        if time.monotonic() - self._last_chunk >= CHUNK_INTERVAL_S:
            self.dump_chunk()

    def dump_chunk(self):
        """Write buffered records to the next part file (atomic rename)."""
        self._last_chunk = time.monotonic()
        if not self._iq:
            return
        width = max(len(iq) for iq in self._iq)
        csi = np.zeros((len(self._iq), width), dtype=np.int8)
        for row, iq in enumerate(self._iq):
            csi[row, : len(iq)] = iq
        arrays = {"csi_data": csi}
        for name, dtype in _ARRAY_DTYPES.items():
            arrays[name] = np.asarray(self._cols[name], dtype=dtype)
        for name in _STRING_ARRAYS:
            arrays[name] = np.asarray(self._cols[name], dtype=str)
        final = self.parts_dir / f"part_{self._part_index:05d}.npz"
        tmp = final.with_suffix(".tmp.npz")
        np.savez(tmp, **arrays)
        os.replace(tmp, final)
        self._part_index += 1
        self._reset_buffers()

    def flush(self):
        if self.csv is not None:
            self.csv.flush()

    def close(self):
        self.dump_chunk()
        if self.csv is not None:
            self.csv.close()


def merge_parts(board_dir, meta, delete_parts=True):
    """Merge csi_parts/*.npz into csi.npz. Returns (path, summary dict) or (None, {}).

    ``meta`` supplies session-level values: label, csi_target_pps, collected_at,
    totals, and the first-packet info (chip, device_id, firmware, ...).
    Only records whose I/Q length equals the dominant one are kept in csi.npz
    (ESPectre's NPZ format needs a rectangular matrix); csi_amplitudes.csv still
    has every record.
    """
    from tools.lib.atomic_io import atomic_savez_compressed
    from tools.lib.dataset_metadata import DATASET_FORMAT_VERSION

    board_dir = Path(board_dir)
    part_files = sorted((board_dir / PARTS_DIR).glob("part_*.npz"))
    part_files = [p for p in part_files if not p.name.endswith(".tmp.npz")]
    if not part_files:
        return None, {}

    pieces = {}
    for part in part_files:
        with np.load(part, allow_pickle=False) as data:
            for key in data.files:
                pieces.setdefault(key, []).append(data[key])
    lengths = np.concatenate(pieces.pop("iq_len"))
    rows = pieces.pop("csi_data")
    dominant = Counter(lengths.tolist()).most_common(1)[0][0]
    keep = lengths == dominant
    csi = np.concatenate([r[:, :dominant] if r.shape[1] >= dominant
                          else np.pad(r, ((0, 0), (0, dominant - r.shape[1]))) for r in rows])
    columns = {key: np.concatenate(values)[keep] for key, values in pieces.items()}
    csi = csi[keep]

    pc = columns["pc_t_ns"]
    duration_ms = float(pc[-1] - pc[0]) / 1e6 if len(pc) > 1 else 0.0
    observed_pps = (len(pc) - 1) / (duration_ms / 1000.0) if duration_ms > 0 else 0.0
    info = meta.get("first_packet_info") or {}
    totals = meta.get("totals") or {}
    target_pps = int(meta.get("csi_target_pps") or round(observed_pps) or 100)
    endpoint = info.get("transport_target", "")

    sample = {
        # Same keys as tools.lib.csi_io.CSICollector.save_sample()
        "csi_data": csi,
        "num_subcarriers": int(info.get("num_subcarriers", dominant // 2)),
        "label": str(meta.get("label", "unknown")),
        "chip": str(info.get("chip", "unknown")),
        "collected_at": str(meta.get("collected_at") or datetime.now().isoformat()),
        "duration_ms": duration_ms,
        "format_version": DATASET_FORMAT_VERSION,
        "stream_seq_num": columns["seq_num"].astype(np.uint32),
        "raw_stream_sequence": columns["raw_stream_sequence"],
        "phy_mode": columns["phy_mode"],
        "ltf_type": columns["ltf_type"],
        "channel_width": columns["channel_width"],
        "device_id": np.uint64(info.get("device_id", 0)),
        "transport": "http",
        "transport_target": endpoint,
        "endpoint": endpoint,
        "effective_pps": np.float64(observed_pps),
        "observed_pps": np.float64(observed_pps),
        "requested_pps": np.float64(target_pps),
        "csi_target_pps": np.uint64(target_pps),
        "raw_protocol_version": np.uint8(info.get("raw_protocol_version", 0)),
        "record_version": np.uint8(info.get("record_version", 8)),
        "frontend": info.get("frontend", ""),
        "firmware_version": info.get("firmware_version", ""),
        "firmware_identity": info.get("firmware_identity", ""),
        "raw_fresh_record_total": np.uint64(totals.get("fresh_record_total", len(pc))),
        "fresh_record_total": np.uint64(totals.get("fresh_record_total", len(pc))),
        "raw_drop_total": np.uint64(totals.get("raw_drop_total", 0)),
        "raw_send_backpressure_total": np.uint64(totals.get("send_backpressure_total", 0)),
        "send_backpressure_total": np.uint64(totals.get("send_backpressure_total", 0)),
        "raw_final_stream_sequence": np.uint64(columns["raw_stream_sequence"][-1] if len(pc) else 0),
        "device_ticks_us": columns["device_ticks_us"],
        "wifi_rx_ts_us": columns["wifi_rx_ts_us"],
        "wifi_rx_start_ts_ns": columns["wifi_rx_start_ts_ns"],
        "channel": columns["channel"],
        "rssi_dbm": columns["rssi_dbm"],
        "noise_floor_dbm": columns["noise_floor_dbm"],
        # Extra arrays added by csi_logger (ignored by ESPectre loaders)
        "pc_t_ns": columns["pc_t_ns"],
        "connection_index": columns["connection"],
        "record_fresh_total": columns["fresh_record_total"],
        "record_raw_drop_total": columns["raw_drop_total"],
        "record_send_backpressure_total": columns["raw_send_backpressure_total"],
    }
    path = atomic_savez_compressed(board_dir / "csi.npz", sample)

    # Verify before deleting the parts.
    with np.load(path, allow_pickle=False) as check:
        ok = check["csi_data"].shape[0] == int(keep.sum())
    if ok and delete_parts:
        shutil.rmtree(board_dir / PARTS_DIR, ignore_errors=True)
    summary = {
        "npz_records": int(keep.sum()),
        "npz_skipped_length_mismatch": int((~keep).sum()),
        "iq_bytes_per_record": int(dominant),
        "observed_pps": round(observed_pps, 2),
    }
    return path, summary


def merge_from_session_json(board_dir):
    """Rebuild csi.npz from parts using metadata saved in session.json."""
    board_dir = Path(board_dir)
    meta = {}
    session_file = board_dir / "session.json"
    if session_file.exists():
        session = json.loads(session_file.read_text(encoding="utf-8"))
        meta = {
            "label": session.get("label"),
            "csi_target_pps": (session.get("sensing_start") or {}).get("csi_target_pps"),
            "collected_at": session.get("start_iso"),
            "first_packet_info": session.get("first_packet_info"),
            "totals": session.get("csi_totals"),
        }
    return merge_parts(board_dir, meta)
