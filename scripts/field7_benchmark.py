#!/usr/bin/env python3
"""
field7_benchmark.py

Field 7 performance benchmark for the six implemented architectures.

What this script does
---------------------
1. Connects to Trino.
2. Benchmarks one main comparable hourly table per architecture.
3. Runs 5 varied-difficulty queries per architecture.
4. Executes each query 5 times.
5. Discards the 1st run as warm-up.
6. Computes average, median, and p95 latency from runs 2-5.
7. Produces CSV and JSON outputs for later fuzzy scoring.

Install dependency
------------------
pip install trino
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import trino


ARCHITECTURES: Dict[str, Dict[str, str]] = {
    "Data Lake": {
        "table": "lake.enriched.cyprus_hourly_enriched_full_source",
        "source_cte": """
            SELECT
                time AS event_time,
                CAST(time AS DATE) AS event_date,
                point_name,
                temperature_2m,
                precipitation,
                wind_speed_10m
            FROM lake.enriched.cyprus_hourly_enriched_full_source
        """,
    },
    "Data Lakehouse": {
        "table": "iceberg.silver.cyprus_hourly_clean_full",
        "source_cte": """
            SELECT
                event_time,
                event_date,
                point_name,
                temperature_2m,
                precipitation,
                wind_speed_10m
            FROM iceberg.silver.cyprus_hourly_clean_full
        """,
    },
    "Data Mesh": {
        "table": "mesh.station_observations.hourly_observations_full",
        "source_cte": """
            SELECT
                event_time,
                event_date,
                point_name,
                temperature_2m,
                precipitation,
                wind_speed_10m
            FROM mesh.station_observations.hourly_observations_full
        """,
    },
    "Data Fabric": {
        "table": "fabric.integrated.cyprus_weather_unified",
        "source_cte": """
            SELECT
                event_time,
                event_date,
                point_name,
                temperature_2m,
                precipitation,
                wind_speed_10m
            FROM fabric.integrated.cyprus_weather_unified
        """,
    },
    "Semantic Data Fabric": {
        "table": "semanticfabric.source.cyprus_hourly_source",
        "source_cte": """
            SELECT
                event_time,
                event_date,
                point_name,
                temperature_2m,
                precipitation,
                wind_speed_10m
            FROM semanticfabric.source.cyprus_hourly_source
        """,
    },
    "Knowledge Graph": {
        "table": "knowledgegraph.source.cyprus_hourly_source",
        "source_cte": """
            SELECT
                event_time,
                event_date,
                point_name,
                temperature_2m,
                precipitation,
                wind_speed_10m
            FROM knowledgegraph.source.cyprus_hourly_source
        """,
    },
}

QUERY_DEFS: List[Tuple[str, str, str]] = [
    (
        "Q1_full_scan_count",
        "Easy",
        """
        SELECT count(*) AS row_count
        FROM src
        """,
    ),
    (
        "Q2_selective_filter_count",
        "Easy-Medium",
        """
        SELECT count(*) AS january_nicosia_rows
        FROM src
        WHERE point_name = 'Nicosia'
          AND event_date BETWEEN DATE '2024-01-01' AND DATE '2024-01-31'
        """,
    ),
    (
        "Q3_daily_aggregation",
        "Medium",
        """
        SELECT
            event_date,
            avg(temperature_2m) AS avg_temp,
            sum(precipitation) AS total_precip
        FROM src
        GROUP BY event_date
        ORDER BY event_date
        """,
    ),
    (
        "Q4_monthly_grouped_analytics",
        "Medium-Hard",
        """
        SELECT
            year(event_time) AS yr,
            month(event_time) AS mon,
            point_name,
            avg(temperature_2m) AS avg_temp,
            max(wind_speed_10m) AS max_wind,
            sum(precipitation) AS total_precip
        FROM src
        GROUP BY year(event_time), month(event_time), point_name
        ORDER BY yr, mon, point_name
        """,
    ),
    (
        "Q5_grouped_ranking",
        "Hard",
        """
        SELECT
            point_name,
            yr,
            mon,
            avg_temp,
            rank() OVER (PARTITION BY point_name ORDER BY avg_temp DESC) AS temp_rank
        FROM (
            SELECT
                point_name,
                year(event_time) AS yr,
                month(event_time) AS mon,
                avg(temperature_2m) AS avg_temp
            FROM src
            GROUP BY point_name, year(event_time), month(event_time)
        ) monthly
        ORDER BY point_name, temp_rank, yr, mon
        """,
    ),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run Field 7 performance benchmarks against Trino.")
    p.add_argument("--host", default="localhost", help="Trino host")
    p.add_argument("--port", type=int, default=8080, help="Trino port")
    p.add_argument("--user", default="trino", help="Trino user")
    p.add_argument("--http-scheme", default="http", choices=["http", "https"])
    p.add_argument("--runs", type=int, default=5, help="Runs per query (first run discarded as warm-up)")
    p.add_argument("--output-dir", default="field7_benchmark_output", help="Folder for benchmark results")
    p.add_argument("--stop-on-error", action="store_true", help="Stop immediately if a query fails")
    return p.parse_args()


def connect(args: argparse.Namespace):
    return trino.dbapi.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        http_scheme=args.http_scheme,
    )


def percentile(values: List[float], q: float) -> float:
    if not values:
        raise ValueError("Cannot compute percentile of empty list.")
    if len(values) == 1:
        return values[0]
    sorted_vals = sorted(values)
    pos = (len(sorted_vals) - 1) * q
    low = math.floor(pos)
    high = math.ceil(pos)
    if low == high:
        return sorted_vals[low]
    fraction = pos - low
    return sorted_vals[low] + (sorted_vals[high] - sorted_vals[low]) * fraction


def field7_band_from_p95_ms(p95_ms: float) -> str:
    if p95_ms > 2000:
        return "Low"
    if p95_ms <= 500:
        return "High"
    return "Medium"


def build_sql(source_cte: str, query_body: str) -> str:
    return f"""
    WITH src AS (
        {source_cte}
    )
    {query_body}
    """


def execute_and_time(conn, sql: str) -> Tuple[float, int]:
    cur = conn.cursor()
    start = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    end = time.perf_counter()
    elapsed_ms = (end - start) * 1000.0
    return elapsed_ms, len(rows)


def benchmark_query(
    conn,
    architecture: str,
    table_name: str,
    source_cte: str,
    query_name: str,
    difficulty: str,
    query_body: str,
    runs: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    sql = build_sql(source_cte, query_body)
    run_rows: List[Dict[str, Any]] = []
    kept_latencies: List[float] = []
    result_row_count = None

    for run_no in range(1, runs + 1):
        elapsed_ms, fetched_rows = execute_and_time(conn, sql)
        if result_row_count is None:
            result_row_count = fetched_rows

        is_warmup = run_no == 1
        run_rows.append(
            {
                "architecture": architecture,
                "table_name": table_name,
                "query_name": query_name,
                "difficulty": difficulty,
                "run_number": run_no,
                "is_warmup": int(is_warmup),
                "latency_ms": round(elapsed_ms, 3),
                "result_row_count": fetched_rows,
            }
        )

        if not is_warmup:
            kept_latencies.append(elapsed_ms)

    summary = {
        "architecture": architecture,
        "table_name": table_name,
        "query_name": query_name,
        "difficulty": difficulty,
        "runs_total": runs,
        "runs_used_for_stats": len(kept_latencies),
        "avg_ms": round(statistics.mean(kept_latencies), 3),
        "median_ms": round(statistics.median(kept_latencies), 3),
        "p95_ms": round(percentile(kept_latencies, 0.95), 3),
        "min_ms": round(min(kept_latencies), 3),
        "max_ms": round(max(kept_latencies), 3),
        "result_row_count": result_row_count,
    }
    return run_rows, summary


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = connect(args)

    run_rows_all: List[Dict[str, Any]] = []
    query_summary_rows: List[Dict[str, Any]] = []
    architecture_summary_rows: List[Dict[str, Any]] = []
    error_rows: List[Dict[str, Any]] = []

    for architecture, cfg in ARCHITECTURES.items():
        arch_query_summaries: List[Dict[str, Any]] = []
        print(f"Benchmarking {architecture} ...")

        for query_name, difficulty, query_body in QUERY_DEFS:
            try:
                run_rows, summary = benchmark_query(
                    conn=conn,
                    architecture=architecture,
                    table_name=cfg["table"],
                    source_cte=cfg["source_cte"],
                    query_name=query_name,
                    difficulty=difficulty,
                    query_body=query_body,
                    runs=args.runs,
                )
                run_rows_all.extend(run_rows)
                query_summary_rows.append(summary)
                arch_query_summaries.append(summary)
                print(f"  {query_name}: p95={summary['p95_ms']} ms")
            except Exception as exc:
                err = {
                    "architecture": architecture,
                    "table_name": cfg["table"],
                    "query_name": query_name,
                    "error": str(exc),
                }
                error_rows.append(err)
                print(f"  {query_name}: ERROR -> {exc}")
                if args.stop_on_error:
                    raise

        if arch_query_summaries:
            overall_p95_ms = sum(r["p95_ms"] for r in arch_query_summaries) / len(arch_query_summaries)
            architecture_summary_rows.append(
                {
                    "architecture": architecture,
                    "table_name": cfg["table"],
                    "queries_benchmarked": len(arch_query_summaries),
                    "overall_avg_of_query_averages_ms": round(
                        sum(r["avg_ms"] for r in arch_query_summaries) / len(arch_query_summaries), 3
                    ),
                    "overall_avg_of_query_medians_ms": round(
                        sum(r["median_ms"] for r in arch_query_summaries) / len(arch_query_summaries), 3
                    ),
                    "overall_avg_of_query_p95_ms": round(overall_p95_ms, 3),
                    "field7_band_from_p95": field7_band_from_p95_ms(overall_p95_ms),
                }
            )

    write_csv(output_dir / "field7_query_run_details.csv", run_rows_all)
    write_csv(output_dir / "field7_query_summary.csv", query_summary_rows)
    write_csv(output_dir / "field7_architecture_summary.csv", architecture_summary_rows)
    write_csv(output_dir / "field7_errors.csv", error_rows)

    json_payload = {
        "architectures": architecture_summary_rows,
        "query_summaries": query_summary_rows,
        "errors": error_rows,
    }
    (output_dir / "field7_benchmark_results.json").write_text(
        json.dumps(json_payload, indent=2),
        encoding="utf-8",
    )

    notes = """
Field 7 benchmark notes

Official benchmark logic
- One main comparable hourly table per architecture
- 5 query types with varied difficulty
- 5 runs per query
- 1st run discarded as warm-up
- stats computed from runs 2-5

Main result to use for Field 7
- overall_avg_of_query_p95_ms from field7_architecture_summary.csv

Interpretation against Field 7 scale
- Low: p95 > 2000 ms
- Medium: 500 ms < p95 <= 2000 ms
- High: p95 <= 500 ms
"""
    (output_dir / "field7_notes.txt").write_text(notes.strip() + "\n", encoding="utf-8")

    print("")
    print(f"Done. Output folder: {output_dir.resolve()}")
    print("Created:")
    print("- field7_query_run_details.csv")
    print("- field7_query_summary.csv")
    print("- field7_architecture_summary.csv")
    print("- field7_errors.csv")
    print("- field7_benchmark_results.json")
    print("- field7_notes.txt")


if __name__ == "__main__":
    main()
