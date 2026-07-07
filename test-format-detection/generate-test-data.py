#!/usr/bin/env python3
"""
Generate test data for format detection load testing.

Usage:
  python3 generate-test-data.py ndjson 5000 100 /tmp/test-data-ndjson
  python3 generate-test-data.py csv 1000 500 /tmp/test-data-csv
  python3 generate-test-data.py json 2000 250 /tmp/test-data-json
  python3 generate-test-data.py mixed 5000 100 /tmp/test-data-mixed

Arguments:
  format:    ndjson | csv | json | mixed
  num_files: number of files to generate
  size_kb:   target size per file in KB
  output_dir: where to write files
"""

import json
import os
import random
import string
import sys
import time
from datetime import datetime, timedelta

LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"]
SERVICES = [f"service-{i}" for i in range(10)]
METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]
PATHS = [f"/api/v1/{r}/{i}" for r in ["users", "orders", "products", "events", "metrics"] for i in range(100)]
CATEGORIES = ["Electronics", "Books", "Clothing", "Food", "Sports", "Home", "Garden", "Toys", "Automotive", "Health"]
REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-northeast-1", "sa-east-1", "eu-central-1"]
PAYMENTS = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]
STATUSES = ["completed", "pending", "shipped", "cancelled", "refunded"]
ACTIONS = ["click", "scroll", "purchase", "view", "search", "add_to_cart", "remove", "checkout"]


def random_timestamp():
    base = datetime(2026, 7, 7, 0, 0, 0)
    offset = timedelta(seconds=random.randint(0, 86400))
    return (base + offset).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def generate_ndjson_file(filepath, target_bytes):
    with open(filepath, 'w') as f:
        written = 0
        while written < target_bytes:
            record = {
                "timestamp": random_timestamp(),
                "level": random.choice(LEVELS),
                "service": random.choice(SERVICES),
                "duration_ms": random.randint(1, 5000),
                "status": random.choice([200, 200, 200, 201, 400, 401, 403, 404, 500]),
                "method": random.choice(METHODS),
                "path": random.choice(PATHS),
                "message": f"Request processed successfully id={random.randint(100000, 999999)}",
                "request_id": f"req-{''.join(random.choices(string.hexdigits[:16], k=16))}",
                "trace_id": f"trace-{''.join(random.choices(string.hexdigits[:16], k=32))}",
                "client_ip": f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
            }
            line = json.dumps(record) + "\n"
            f.write(line)
            written += len(line)


def generate_csv_file(filepath, target_bytes):
    header = "order_id,timestamp,product_name,category,price,quantity,customer_id,region,payment_method,order_status\n"
    with open(filepath, 'w') as f:
        f.write(header)
        written = len(header)
        i = 0
        while written < target_bytes:
            line = (
                f"ORD-{i:07d},"
                f"{random_timestamp()},"
                f"Product-{random.randint(1, 10000)},"
                f"{random.choice(CATEGORIES)},"
                f"{random.uniform(1.99, 999.99):.2f},"
                f"{random.randint(1, 50)},"
                f"CUST-{random.randint(1, 99999):05d},"
                f"{random.choice(REGIONS)},"
                f"{random.choice(PAYMENTS)},"
                f"{random.choice(STATUSES)}\n"
            )
            f.write(line)
            written += len(line)
            i += 1


def generate_json_file(filepath, target_bytes):
    records = []
    written = 2  # for [ and ]
    i = 0
    while written < target_bytes - 10:
        record = {
            "id": i,
            "timestamp": random_timestamp(),
            "user_id": f"U-{random.randint(1, 10000)}",
            "action": random.choice(ACTIONS),
            "page": f"/page/{random.randint(1, 500)}",
            "session": f"sess-{''.join(random.choices(string.hexdigits[:16], k=12))}",
            "duration_ms": random.randint(1, 30000),
            "metadata": {
                "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
                "os": random.choice(["Windows", "macOS", "Linux", "iOS", "Android"]),
                "device": random.choice(["desktop", "mobile", "tablet"])
            }
        }
        records.append(record)
        written += len(json.dumps(record)) + 2  # +2 for comma and newline
        i += 1

    with open(filepath, 'w') as f:
        json.dump(records, f)


def main():
    if len(sys.argv) < 5:
        print(__doc__)
        sys.exit(1)

    fmt = sys.argv[1]
    num_files = int(sys.argv[2])
    size_kb = int(sys.argv[3])
    output_dir = sys.argv[4]
    target_bytes = size_kb * 1024

    os.makedirs(output_dir, exist_ok=True)

    print(f"Generating {num_files} {fmt} files (~{size_kb}KB each)")
    print(f"Output: {output_dir}")
    print(f"Total expected: ~{num_files * size_kb // 1024} MB")
    print()

    start = time.time()
    generators = {
        "ndjson": ("logs-{:05d}.ndjson", generate_ndjson_file),
        "csv": ("orders-{:05d}.csv", generate_csv_file),
        "json": ("events-{:05d}.json", generate_json_file),
    }

    for i in range(1, num_files + 1):
        if fmt == "mixed":
            choice = ["ndjson", "csv", "json"][i % 3]
        else:
            choice = fmt

        pattern, gen_func = generators[choice]
        filepath = os.path.join(output_dir, pattern.format(i))
        gen_func(filepath, target_bytes)

        if i % 100 == 0:
            elapsed = time.time() - start
            rate = i / elapsed
            remaining = (num_files - i) / rate
            print(f"  Progress: {i}/{num_files} files ({rate:.0f}/s, ~{remaining:.0f}s remaining)")

    elapsed = time.time() - start
    total_size_mb = sum(os.path.getsize(os.path.join(output_dir, f)) for f in os.listdir(output_dir)) / (1024 * 1024)
    print()
    print(f"Done!")
    print(f"  Files: {num_files}")
    print(f"  Total size: {total_size_mb:.1f} MB")
    print(f"  Time: {elapsed:.1f}s")
    print(f"  Rate: {num_files/elapsed:.0f} files/s")
    print(f"  Output: {output_dir}")
    print()
    print(f"To upload to S3:")
    print(f"  aws s3 cp {output_dir} s3://siqi-format-detector-bucket/test-data/ --recursive")


if __name__ == "__main__":
    main()
