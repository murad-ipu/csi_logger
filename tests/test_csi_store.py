"""CSI parsing (through the repo's csi_io) and storage: CSV rows, part files, csi.npz."""

import csv

import numpy as np

from csi_store import CsiRecorder, merge_parts
from tools.lib.csi_io import DirectRawCSIReceiver, load_npz_as_packets, parse_csi_record
from v8 import build_frame, build_record, synthetic_payload


def test_repo_parser_reads_v8_record_and_amplitudes():
    # Payload order is Q, I per subcarrier: (Q=4, I=3) -> |3 + 4j| = 5
    packet, _ = parse_csi_record(build_record(seq_num=7, payload=[4, 3, 0, -2], rssi_dbm=-55))
    assert packet.seq_num == 7 and packet.rssi_dbm == -55 and packet.chip == "S3"
    np.testing.assert_allclose(packet.amplitudes, [5.0, 2.0])


def _receiver_fed_with(frames):
    """Feed raw HTTP frames through DirectRawCSIReceiver's own frame parser."""
    receiver = DirectRawCSIReceiver("127.0.0.1", derive_complex=False)
    received = []
    receiver.add_callback(received.append)
    for frame in frames:
        receiver._raw_buffer.extend(frame)
        receiver._consume_raw_frames()
    return receiver, received


def test_recorder_writes_csv_parts_and_espectre_compatible_npz(tmp_path):
    frames = [build_frame(seq, raw_drop_total=seq // 50) for seq in range(1, 121)]
    _, packets = _receiver_fed_with(frames)
    assert len(packets) == 120

    recorder = CsiRecorder(tmp_path)
    for i, packet in enumerate(packets):
        recorder.add(packet, connection=1)
        if i == 59:
            recorder.dump_chunk()          # force two part files
    recorder.close()
    assert len(list((tmp_path / "csi_parts").glob("part_*.npz"))) == 2

    # CSV: one row per record, 64 amplitude columns, values match csi_io.
    with open(tmp_path / "csi_amplitudes.csv", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 120
    assert sum(key.startswith("amp_") for key in rows[0]) == 64
    assert rows[0]["seq_num"] == "1" and rows[0]["rssi_dbm"] == "-50"
    assert float(rows[0]["amp_0"]) == round(float(packets[0].amplitudes[0]), 2)
    pc = [int(r["pc_t_ns"]) for r in rows]
    assert pc == sorted(pc)

    meta = {"label": "empty", "csi_target_pps": 100, "first_packet_info": recorder.first_packet_info,
            "totals": {"raw_drop_total": 2}}
    path, summary = merge_parts(tmp_path, meta)
    assert summary["npz_records"] == 120 and not (tmp_path / "csi_parts").exists()

    # The repo's loader reads it back with the same payloads.
    loaded = load_npz_as_packets(path)
    assert len(loaded) == 120
    np.testing.assert_array_equal(loaded[5]["csi_data"], np.array(synthetic_payload(6), dtype=np.int8))
    assert loaded[0]["csi_target_pps"] == 100 and loaded[0]["chip"] == "s3"
    with np.load(path) as data:
        np.testing.assert_array_equal(data["pc_t_ns"], pc)
        assert int(data["raw_drop_total"]) == 2
