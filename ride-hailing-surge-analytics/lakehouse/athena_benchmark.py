"""Athena scan-cost benchmark - REAL bytes scanned from Athena (the first version hard-coded 450 MB vs 72 MB).

Runs 5 queries against the raw-CSV table (fact_trips_csv) and the partitioned-Parquet table (fact_trips) and reports
DataScannedInBytes for each. Target: >= 70% reduction.
  python -m lakehouse.athena_benchmark --date 2026-10-05
Requires: s3_sync + Glue crawler done, ATHENA_OUTPUT (s3://bucket/athena-results/) and AWS credentials.
"""
from __future__ import annotations

import argparse
import os
import time

import boto3

QUERIES = {
    "Q1 hourly demand by zone": "SELECT pickup_zone_key, COUNT(*) c FROM {t} WHERE {f} GROUP BY 1",
    "Q2 revenue per driver": "SELECT driver_key, SUM(fare_inr) r FROM {t} WHERE {f} AND trip_status='COMPLETED' GROUP BY 1",
    "Q3 avg wait by zone": "SELECT pickup_zone_key, AVG(wait_time_min) w FROM {t} WHERE {f} GROUP BY 1",
    "Q4 cancel reasons": "SELECT cancel_reason, COUNT(*) c FROM {t} WHERE {f} AND trip_status='CANCELLED' GROUP BY 1",
    "Q5 distance and fare": "SELECT AVG(distance_km), AVG(fare_inr) FROM {t} WHERE {f}",
}


def scanned_bytes(athena, sql: str, db: str, output: str) -> int:
    qid = athena.start_query_execution(QueryString=sql, QueryExecutionContext={"Database": db}, ResultConfiguration={"OutputLocation": output})["QueryExecutionId"]
    while True:
        q = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        if q["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if q["Status"]["State"] != "SUCCEEDED":
        raise RuntimeError(f"Athena query failed: {q['Status'].get('StateChangeReason')}")
    return int(q["Statistics"]["DataScannedInBytes"])


def main() -> None:
    ap = argparse.ArgumentParser(); ap.add_argument("--date", required=True); a = ap.parse_args()
    db, out = os.getenv("GLUE_DATABASE", "ridehail"), os.environ["ATHENA_OUTPUT"]
    athena = boto3.client("athena", region_name=os.getenv("AWS_REGION", "ap-south-1"))
    rows = []
    for name, q in QUERIES.items():
        csv = scanned_bytes(athena, q.format(t="fact_trips_csv", f=f"trip_date = '{a.date}'"), db, out)
        pq = scanned_bytes(athena, q.format(t="fact_trips", f=f"city = 'Hyderabad' AND trip_date = DATE '{a.date}'"), db, out)
        rows.append((name, csv, pq))
    print(f"{'query':28}{'csv_bytes':>14}{'parquet_bytes':>16}{'reduction':>11}")
    for n, c, p in rows:
        print(f"{n:28}{c:>14}{p:>16}{(1 - p / c if c else 0):>10.1%}")
    tc, tp = sum(r[1] for r in rows), sum(r[2] for r in rows)
    red = 1 - tp / tc if tc else 0
    print(f"\nOverall reduction: {red:.1%} -> {'PASS' if red >= 0.70 else 'FAIL'} (target >= 70%)")


if __name__ == "__main__":
    main()
