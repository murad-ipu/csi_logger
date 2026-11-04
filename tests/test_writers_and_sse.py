"""Unit tests for JSON flattening, CSV/JSONL writers, and the SSE parser."""

import csv
import json

from sse import SseParser
from writers import CsvLog, JsonlLog, flatten


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_flatten_nests_with_dots_and_serialises_lists():
    flat = flatten({"a": 1, "raw_csi": {"active": True, "x": {"y": None}}, "list": [1, 2]})
    assert flat == {"a": 1, "raw_csi.active": True, "raw_csi.x.y": None, "list": "[1,2]"}


def test_csv_header_grows_without_losing_rows(tmp_path):
    path = tmp_path / "diag.csv"
    log = CsvLog(path)
    log.write({"pc_t_ns": 1, "a": 10})
    log.write({"pc_t_ns": 2, "a": 11, "b": 5})      # new field appears
    log.close()
    rows = read_csv(path)
    assert list(rows[0].keys()) == ["pc_t_ns", "a", "b"]
    assert rows[0] == {"pc_t_ns": "1", "a": "10", "b": ""}
    assert rows[1] == {"pc_t_ns": "2", "a": "11", "b": "5"}


def test_csv_is_readable_before_close(tmp_path):
    path = tmp_path / "events.csv"
    log = CsvLog(path, ["pc_t_ns", "state"])
    log.write({"pc_t_ns": 1, "state": "idle"})
    log.flush()                                     # what the 1 s console tick does
    assert read_csv(path) == [{"pc_t_ns": "1", "state": "idle"}]
    log.close()


def test_jsonl_appends_one_object_per_line(tmp_path):
    path = tmp_path / "raw_http.jsonl"
    log = JsonlLog(path)
    log.write({"endpoint": "/espectre/v1/health", "status": 200, "body": {"ok": True}})
    log.write({"endpoint": "/espectre/v1/wifi", "status": 503, "body": "busy"})
    log.close()
    lines = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert [line["status"] for line in lines] == [200, 503]


def test_sse_parser_events_comments_and_multiline_data():
    parser = SseParser()
    lines = [": heartbeat", "", "event: motion", 'data: {"timestamp_ms":42000,', 'data: "state":"idle"}',
             "", 'data: {"x":1}', "", "event: fault", "data:{\"message\":\"m\"}", ""]
    items = [item for item in (parser.feed_line(line) for line in lines) if item]
    assert items[0] == (":", "heartbeat")
    assert items[1][0] == "motion"
    assert json.loads(items[1][1]) == {"timestamp_ms": 42000, "state": "idle"}
    assert items[2] == ("message", '{"x":1}')        # default event name
    assert items[3] == ("fault", '{"message":"m"}')  # no space after colon
