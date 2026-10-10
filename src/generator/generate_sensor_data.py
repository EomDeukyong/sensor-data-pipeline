"""재현 가능한 가상 센서 CSV와 오류 주입 내역을 생성한다 (표준 라이브러리만 사용)."""

import argparse
import csv
import hashlib
import json
import math
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


FIELDS = ["event_id", "device_id", "event_time", "temperature", "vibration", "status"]
LEDGER_FIELDS = ["row_number", "event_id", "kind", "field", "original_value", "injected_value"]
KINDS = ("missing_device_id", "invalid_event_time", "invalid_temperature", "high_temperature")


def parse_start(value):
    """입력 시간은 타임존을 필수로 받고 UTC로 통일한다."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("ISO 8601 시각을 입력하세요.") from exc
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("시각에 Z 또는 +09:00 등 타임존이 필요합니다.")
    return parsed.astimezone(timezone.utc)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100_000, help="중복을 포함한 최종 행 수")
    parser.add_argument("--devices", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-time", type=parse_start, default=parse_start("2026-10-01T00:00:00Z"))
    parser.add_argument("--interval-seconds", type=int, default=1, help="고유 이벤트 간 시간 간격")
    parser.add_argument("--duplicate-rate", type=float, default=0.01)
    parser.add_argument("--missing-rate", type=float, default=0.01)
    parser.add_argument("--invalid-time-rate", type=float, default=0.01)
    parser.add_argument("--invalid-number-rate", type=float, default=0.01)
    parser.add_argument("--anomaly-rate", type=float, default=0.01, help="유효한 고온 측정값 비율")
    parser.add_argument("--output-dir", type=Path, default=Path("data/generated/run-001"))
    args = parser.parse_args(argv)
    if args.rows < 1 or args.devices < 1 or args.interval_seconds < 1:
        parser.error("rows, devices, interval-seconds는 1 이상이어야 합니다.")
    rates = [args.duplicate_rate, args.missing_rate, args.invalid_time_rate,
             args.invalid_number_rate, args.anomaly_rate]
    if any(not math.isfinite(rate) or not 0 <= rate <= 1 for rate in rates):
        parser.error("모든 비율은 0~1 사이 유한한 값이어야 합니다.")
    # 중복은 오류·고온이 없는 정상 이벤트에서만 뽑는다.
    # 따라서 정상 원본 자리와 중복 자리 두 곳이 필요하다.
    counts = [math.floor(args.rows * rate) for rate in rates]
    if 2 * counts[0] + sum(counts[1:]) > args.rows:
        parser.error("정상 원본이 부족합니다. 중복 및 오류/이상값 비율을 낮추세요.")
    unique_rows = args.rows - counts[0]
    try:
        args.start_time + timedelta(seconds=(unique_rows - 1) * args.interval_seconds)
    except OverflowError:
        parser.error("생성할 마지막 이벤트 시각이 datetime 범위를 초과합니다.")
    return args


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def generate(args):
    output = args.output_dir
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise ValueError("출력 경로가 비어 있지 않습니다. 새로운 --output-dir을 지정하세요.")
    output.mkdir(parents=True, exist_ok=True)
    duplicate_count = math.floor(args.rows * args.duplicate_rate)
    kind_counts = dict(zip(KINDS, [math.floor(args.rows * rate) for rate in (
        args.missing_rate, args.invalid_time_rate, args.invalid_number_rate, args.anomaly_rate)]))
    unique_rows = args.rows - duplicate_count
    rng = random.Random(args.seed)
    # 서로 겹치지 않는 행을 선택해 분류별 예상 건수를 정확히 계산한다.
    chosen = rng.sample(range(unique_rows), sum(kind_counts.values()) + duplicate_count)
    injection_by_index = {}
    cursor = 0
    for kind, count in kind_counts.items():
        for index in chosen[cursor:cursor + count]:
            injection_by_index[index] = kind
        cursor += count
    duplicate_sources = set(chosen[cursor:])
    duplicate_rows = []
    data_path = output / "sensor_data.csv"
    ledger_path = output / "injections.csv"
    with data_path.open("w", encoding="utf-8", newline="") as data_file, \
            ledger_path.open("w", encoding="utf-8", newline="") as ledger_file:
        writer = csv.DictWriter(data_file, fieldnames=FIELDS, lineterminator="\n")
        ledger = csv.DictWriter(ledger_file, fieldnames=LEDGER_FIELDS, lineterminator="\n")
        writer.writeheader()
        ledger.writeheader()
        for index in range(unique_rows):
            moment = args.start_time + timedelta(seconds=index * args.interval_seconds)
            row = {
                "event_id": f"evt_{index + 1:010d}",
                "device_id": f"device_{index % args.devices + 1:04d}",
                "event_time": moment.isoformat().replace("+00:00", "Z"),
                "temperature": f"{rng.uniform(20, 70):.2f}",
                "vibration": f"{rng.uniform(0, 5):.3f}",
                "status": rng.choice(["RUNNING", "RUNNING", "IDLE"]),
            }
            kind = injection_by_index.get(index)
            if kind:
                field, value = {
                    "missing_device_id": ("device_id", ""),
                    "invalid_event_time": ("event_time", "not-a-timestamp"),
                    "invalid_temperature": ("temperature", "not-a-number"),
                    "high_temperature": ("temperature", "120.00"),
                }[kind]
                ledger.writerow({"row_number": index + 1, "event_id": row["event_id"],
                                 "kind": kind, "field": field, "original_value": row[field],
                                 "injected_value": value})
                row[field] = value
                if kind == "high_temperature":
                    original_status = row["status"]
                    row["status"] = "ALARM"
                    ledger.writerow({"row_number": index + 1, "event_id": row["event_id"],
                                     "kind": "high_temperature_status", "field": "status",
                                     "original_value": original_status, "injected_value": "ALARM"})
            writer.writerow(row)
            if index in duplicate_sources:
                duplicate_rows.append(row.copy())
        # 학습용으로 완전히 동일한 중복을 마지막에 추가한다. 지연/순서 뒤섞기는 후속 실습 범위.
        for offset, row in enumerate(duplicate_rows, start=1):
            writer.writerow(row)
            ledger.writerow({"row_number": unique_rows + offset, "event_id": row["event_id"],
                             "kind": "duplicate", "field": "*", "original_value": "",
                             "injected_value": "exact copy of an earlier valid row"})
    invalid_rows = sum(kind_counts[kind] for kind in KINDS[:3])
    manifest = {
        "generator_version": 1,
        "python_version": sys.version.split()[0],
        "parameters": {
            "rows": args.rows, "devices": args.devices, "seed": args.seed,
            "start_time_utc": args.start_time.isoformat().replace("+00:00", "Z"),
            "interval_seconds": args.interval_seconds,
            "duplicate_rate": args.duplicate_rate, "missing_rate": args.missing_rate,
            "invalid_time_rate": args.invalid_time_rate,
            "invalid_number_rate": args.invalid_number_rate, "anomaly_rate": args.anomaly_rate,
        },
        "expected": {
            "raw_rows": args.rows, "unique_event_ids": unique_rows,
            "quarantined_rows": invalid_rows, "duplicate_rows_removed": duplicate_count,
            "clean_rows": unique_rows - invalid_rows,
            "valid_high_temperature_rows": kind_counts["high_temperature"],
            "injected_event_counts": {"duplicate": duplicate_count, **kind_counts},
        },
        "files": {
            path.name: {"bytes": path.stat().st_size, "sha256": sha256_file(path)}
            for path in (data_path, ledger_path)
        },
    }
    (output / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest


def main():
    args = parse_args()
    try:
        manifest = generate(args)
    except (OSError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(manifest["expected"], ensure_ascii=False, indent=2))
    print(f"생성 위치: {args.output_dir}")


if __name__ == "__main__":
    main()
