"""Build CSI V8 records and Direct HTTP frames for tests.

Uses the struct definitions from tools/lib/csi_io.py, following the
build_packet() helper in the repo's test/python/host/cli/test_csi_raw_record.py.
"""

import numpy as np

from tools.lib.csi_io import (
    CSI_HEADER_STRUCT,
    RAW_CSI_FLAG_FRESH,
    RAW_CSI_FLAG_WIFI_RX_TS_VALID,
    RAW_CSI_HTTP_FRAME_STRUCT,
    RAW_CSI_PROTOCOL_VERSION,
    RAW_CSI_RECORD_MAGIC,
    RAW_CSI_RECORD_VERSION_V8,
    RAW_CSI_RESPONSE_MAGIC,
)

SESSION_ID = bytes.fromhex("00112233445566778899aabbccddeeff")


def build_record(*, seq_num=1, payload=None, device_ticks_us=123456, wifi_rx_ts_us=0, rssi_dbm=-42,
                 noise_floor_dbm=-96, channel=6, fresh_record_total=0, request_accepted_total=0,
                 transport_backpressure_total=0, chip_code=3, phy_mode=2, ltf_type=2, channel_width=1,
                 device_id=0x10B41DE8EC00, flags=0):
    payload = np.array([1, 2, 3, 4] if payload is None else payload, dtype=np.int8).tobytes()
    header = CSI_HEADER_STRUCT.pack(
        RAW_CSI_RECORD_MAGIC, RAW_CSI_RECORD_VERSION_V8, CSI_HEADER_STRUCT.size, chip_code, flags,
        seq_num, len(payload) // 2, len(payload), device_id, device_ticks_us, wifi_rx_ts_us, 0,
        channel, rssi_dbm, noise_floor_dbm, transport_backpressure_total, fresh_record_total,
        request_accepted_total, phy_mode, ltf_type, channel_width,
    )
    return header + payload


def synthetic_payload(index, num_sc=64):
    """Deterministic int8 I/Q pairs (Q first, then I, as in the V8 payload)."""
    rng = np.random.default_rng(index)
    return rng.integers(-40, 40, size=2 * num_sc, dtype=np.int8).tolist()


def build_frame(stream_sequence, *, raw_drop_total=0, backpressure_total=0, payload=None,
                device_ticks_us=None, rssi_dbm=-50):
    """One Direct HTTP frame (60-byte prefix + V8 record) as sent by GET /csi."""
    ticks = device_ticks_us if device_ticks_us is not None else 1_000_000 + stream_sequence * 10_000
    record = build_record(
        seq_num=stream_sequence, payload=payload if payload is not None else synthetic_payload(stream_sequence),
        device_ticks_us=ticks, wifi_rx_ts_us=ticks & 0xFFFFFFFF, rssi_dbm=rssi_dbm,
        fresh_record_total=stream_sequence, request_accepted_total=stream_sequence,
        transport_backpressure_total=backpressure_total,
        flags=RAW_CSI_FLAG_WIFI_RX_TS_VALID | RAW_CSI_FLAG_FRESH,
    )
    prefix = RAW_CSI_HTTP_FRAME_STRUCT.pack(
        RAW_CSI_RESPONSE_MAGIC, RAW_CSI_PROTOCOL_VERSION, RAW_CSI_RECORD_VERSION_V8,
        RAW_CSI_HTTP_FRAME_STRUCT.size, SESSION_ID, stream_sequence, len(record), 0,
        stream_sequence, raw_drop_total, backpressure_total,
    )
    return prefix + record
