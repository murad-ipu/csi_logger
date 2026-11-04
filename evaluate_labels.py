#!/usr/bin/env python3
"""Score motion detections against labels.csv (columns t_ns,label; PC time.time_ns()).

Each labels.csv row marks the START of a phase; a phase lasts until the next row.
Add a final row such as "end" to close the last phase (otherwise it ends at the
last detection sample).

Input detections (both use the same PC clock as labels.csv):
  * events.csv from `csi_logger.py events`   (motion rows: pc_t_ns, state, score)
  * replay.csv from `csi_logger.py replay`   (pick one --detector)

Reports:
  * false alarms per hour during "empty"  (idle -> motion transitions)
  * % of "walk" time detected as motion
  * detection delay from the start of each "walk" phase to the first motion

Examples:
  python evaluate_labels.py --labels labels.csv --events sessions/<s>/<board>/events.csv
  python evaluate_labels.py --labels labels.csv --replay sessions/<s>/<board>/replay.csv --detector lightweight
"""

import argparse
import csv
import json
import statistics
import sys


def read_labels(path):
    """Return phases as a list of (start_ns, end_ns_or_None, label), sorted by time."""
    with open(path, newline="", encoding="utf-8") as handle:
        rows = sorted((int(r["t_ns"]), r["label"].strip()) for r in csv.DictReader(handle))
    return [(t, rows[i + 1][0] if i + 1 < len(rows) else None, label)
            for i, (t, label) in enumerate(rows)]


def read_detections(path, detector=None, state_column="state"):
    """Return [(pc_t_ns, is_motion)] sorted by time from events.csv or replay.csv."""
    samples = []
    with open(path, newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if "event" in row and row["event"] != "motion":
                continue                      # skip fault rows in events.csv
            if detector and row.get("detector") != detector:
                continue
            if not row.get("pc_t_ns") or not row.get(state_column):
                continue
            samples.append((int(row["pc_t_ns"]), row[state_column].strip().lower() == "motion"))
    samples.sort()
    return samples


def evaluate(phases, samples, empty_label="empty", walk_label="walk"):
    if not samples:
        raise SystemExit("no detection samples found (wrong --detector or empty file?)")
    last_t = samples[-1][0]
    report = {"empty": {"hours": 0.0, "false_alarms": 0, "motion_samples": 0, "samples": 0},
              "walk": {"seconds": 0.0, "motion_samples": 0, "samples": 0, "phases": []}}

    for start, end, label in phases:
        end = last_t if end is None else end
        if end <= start or label not in (empty_label, walk_label):
            continue
        inside = [m for t, m in samples if start <= t < end]
        if label == empty_label:
            r = report["empty"]
            r["hours"] += (end - start) / 3.6e12
            r["samples"] += len(inside)
            r["motion_samples"] += sum(inside)
            # A false alarm is an idle -> motion transition inside the empty phase.
            # Motion carried over from the previous phase is not counted.
            r["false_alarms"] += sum(1 for a, b in zip(inside, inside[1:]) if b and not a)
        else:
            r = report["walk"]
            r["seconds"] += (end - start) / 1e9
            r["samples"] += len(inside)
            r["motion_samples"] += sum(inside)
            first = next((t for t, m in samples if start <= t < end and m), None)
            r["phases"].append({
                "start_ns": start, "duration_s": round((end - start) / 1e9, 2),
                "delay_s": round((first - start) / 1e9, 3) if first is not None else None,
            })

    e, w = report["empty"], report["walk"]
    e["false_alarms_per_hour"] = e["false_alarms"] / e["hours"] if e["hours"] else None
    e["motion_fraction_pct"] = 100.0 * e["motion_samples"] / e["samples"] if e["samples"] else None
    # Detections arrive at a fixed cadence (~4 per second), so the fraction of
    # samples equals the fraction of time.
    w["detected_pct"] = 100.0 * w["motion_samples"] / w["samples"] if w["samples"] else None
    delays = [p["delay_s"] for p in w["phases"] if p["delay_s"] is not None]
    w["missed_phases"] = sum(p["delay_s"] is None for p in w["phases"])
    w["delay_mean_s"] = statistics.mean(delays) if delays else None
    w["delay_median_s"] = statistics.median(delays) if delays else None
    w["delay_max_s"] = max(delays) if delays else None
    return report


def print_report(report, source):
    def f(value, spec=".2f"):
        return "n/a" if value is None else format(value, spec)

    e, w = report["empty"], report["walk"]
    print(f"Source: {source}")
    print(f"EMPTY : {f(e['hours'] * 60, '.1f')} min, {e['false_alarms']} false alarms "
          f"-> {f(e['false_alarms_per_hour'])} per hour; motion in {f(e['motion_fraction_pct'], '.1f')}% of samples")
    print(f"WALK  : {f(w['seconds'], '.0f')} s, detected as motion {f(w['detected_pct'], '.1f')}% of the time")
    print(f"DELAY : mean {f(w['delay_mean_s'])} s, median {f(w['delay_median_s'])} s, "
          f"max {f(w['delay_max_s'])} s, missed walk phases {w['missed_phases']}/{len(w['phases'])}")
    for i, phase in enumerate(w["phases"], 1):
        delay = "MISSED" if phase["delay_s"] is None else f"{phase['delay_s']:.2f} s"
        print(f"   walk #{i}: {phase['duration_s']:.0f} s, delay {delay}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--labels", required=True, help="labels.csv with columns t_ns,label")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--events", help="events.csv from csi_logger events mode")
    source.add_argument("--replay", help="replay.csv from csi_logger replay mode")
    parser.add_argument("--detector", choices=["lightweight", "high_accuracy"],
                        help="which detector to score from replay.csv (required with --replay)")
    parser.add_argument("--state-column", default="state", choices=["state", "raw_state"],
                        help="replay.csv only: debounced 'state' (like the board) or 'raw_state'")
    parser.add_argument("--empty-label", default="empty")
    parser.add_argument("--walk-label", default="walk")
    parser.add_argument("--json", help="also write the report to this JSON file")
    args = parser.parse_args(argv)
    if args.replay and not args.detector:
        parser.error("--detector is required with --replay")

    path = args.events or args.replay
    samples = read_detections(path, args.detector if args.replay else None,
                              args.state_column if args.replay else "state")
    report = evaluate(read_labels(args.labels), samples, args.empty_label, args.walk_label)
    print_report(report, f"{path}" + (f" [{args.detector}]" if args.detector else ""))
    if args.json:
        with open(args.json, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
