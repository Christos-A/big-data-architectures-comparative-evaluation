#!/usr/bin/env python3
"""
ingest_cyprus_multiyear.py

Cyprus-wide multi-year ingestion script for thesis prototype.

What it does:
1. Fetches historical Open-Meteo hourly weather data month-by-month
   for multiple representative points across Cyprus.
2. Creates:
   - structured artifacts      -> CSV, Parquet
   - semi-structured artifacts -> JSON manifests / raw API payloads / JSON summaries
   - unstructured artifacts    -> TXT reports, PNG charts
3. Uploads them to MinIO buckets for:
   - Data Lake   -> bucket: lake
   - Lakehouse   -> bucket: lakehouse
   - Data Mesh   -> bucket: mesh
4. Keeps the upload logic configuration-based so more architectures
   can be added tomorrow with minimal changes.

Important ports:
- MinIO API/S3 endpoint: http://localhost:9000
- MinIO web console:     http://localhost:9001
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import boto3
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import requests
from botocore.client import Config
from botocore.exceptions import ClientError


# ---------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

DEFAULT_HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
]

# Representative Cyprus points
CYPRUS_POINTS = [
    {"point_name": "Nicosia", "latitude": 35.1856, "longitude": 33.3823},
    {"point_name": "Limassol", "latitude": 34.7071, "longitude": 33.0226},
    {"point_name": "Larnaca", "latitude": 34.9167, "longitude": 33.6292},
    {"point_name": "Paphos", "latitude": 34.7754, "longitude": 32.4218},
    {"point_name": "AyiaNapa", "latitude": 34.9821, "longitude": 34.0018},
    {"point_name": "Troodos", "latitude": 34.9230, "longitude": 32.8597},
]

CONTENT_TYPES = {
    ".csv": "text/csv",
    ".json": "application/json",
    ".txt": "text/plain",
    ".png": "image/png",
    ".parquet": "application/octet-stream",
}

# ---------------------------------------------------------------------
# Upload configuration
# Keep this config-driven so tomorrow more architectures can be added
# without rewriting the whole script.
# ---------------------------------------------------------------------

ARCHITECTURE_UPLOADS_MONTHLY = {
    "lake": {
        "archive_csv": "raw/structured/year={year}/month={month}/archive_hourly_observations_cyprus.csv",
        "archive_json": "raw/semi_structured/year={year}/month={month}/archive_response_cyprus.json",
        "report_txt": "raw/unstructured/year={year}/month={month}/climate_report_cyprus.txt",
        "plot_png": "raw/unstructured/year={year}/month={month}/temperature_trend_cyprus.png",
        "enriched_parquet": "enriched/structured/year={year}/month={month}/hourly_observations_enriched_cyprus.parquet",
        "daily_by_point_json": "enriched/semi_structured/year={year}/month={month}/daily_weather_summary_by_point.json",
        "daily_cyprus_parquet": "curated/structured/year={year}/month={month}/daily_weather_summary_cyprus.parquet",
        "manifest_json": "curated/semi_structured/year={year}/month={month}/ingestion_manifest_cyprus.json",
    },
    "lakehouse": {
        "archive_parquet": "bronze/year={year}/month={month}/archive_hourly_observations_cyprus.parquet",
        "enriched_parquet": "silver/year={year}/month={month}/hourly_observations_clean_cyprus.parquet",
        "daily_cyprus_parquet": "gold/year={year}/month={month}/daily_weather_summary_cyprus.parquet",
        "manifest_json": "gold/year={year}/month={month}/ingestion_manifest_cyprus.json",
    },
    "mesh": {
        "archive_parquet": "station_observations/year={year}/month={month}/station_observations_cyprus.parquet",
        "report_txt": "climate_reports/year={year}/month={month}/climate_report_cyprus.txt",
        "plot_png": "climate_reports/year={year}/month={month}/temperature_trend_cyprus.png",
        "daily_cyprus_parquet": "climate_reports/year={year}/month={month}/daily_weather_summary_cyprus.parquet",
        "manifest_json": "climate_reports/year={year}/month={month}/ingestion_manifest_cyprus.json",
    },
}

ARCHITECTURE_UPLOADS_FULL_RANGE = {
    "lake": {
        "archive_full_parquet": "raw/structured/full_range/archive_hourly_observations_cyprus_full.parquet",
        "historical_manifest_json": "curated/semi_structured/full_range/historical_manifest_cyprus_full.json",
        "historical_report_txt": "raw/unstructured/full_range/historical_report_cyprus_full.txt",
        "historical_plot_png": "raw/unstructured/full_range/temperature_trend_cyprus_full.png",
        "enriched_full_parquet": "enriched/structured/full_range/hourly_observations_enriched_cyprus_full.parquet",
        "daily_by_point_full_parquet": "curated/structured/full_range/daily_weather_summary_by_point_full.parquet",
        "daily_cyprus_full_parquet": "curated/structured/full_range/daily_weather_summary_cyprus_full.parquet",
    },
    "lakehouse": {
        "archive_full_parquet": "bronze/full_range/archive_hourly_observations_cyprus_full.parquet",
        "enriched_full_parquet": "silver/full_range/hourly_observations_clean_cyprus_full.parquet",
        "daily_cyprus_full_parquet": "gold/full_range/daily_weather_summary_cyprus_full.parquet",
        "historical_manifest_json": "gold/full_range/historical_manifest_cyprus_full.json",
    },
    "mesh": {
        "archive_full_parquet": "station_observations/full_range/station_observations_cyprus_full.parquet",
        "daily_cyprus_full_parquet": "climate_reports/full_range/daily_weather_summary_cyprus_full.parquet",
        "historical_report_txt": "climate_reports/full_range/historical_report_cyprus_full.txt",
        "historical_plot_png": "climate_reports/full_range/temperature_trend_cyprus_full.png",
        "historical_manifest_json": "climate_reports/full_range/historical_manifest_cyprus_full.json",
    },
}

ARCHITECTURE_UPLOADS_FORECAST = {
    "lake": {
        "forecast_json": "raw/semi_structured/latest/forecast_response_cyprus.json",
        "forecast_parquet": "enriched/structured/latest/forecast_hourly_cyprus.parquet",
        "forecast_report_txt": "raw/unstructured/latest/forecast_report_cyprus.txt",
        "forecast_plot_png": "raw/unstructured/latest/forecast_temperature_trend_cyprus.png",
        "forecast_manifest_json": "curated/semi_structured/latest/forecast_manifest_cyprus.json",
    },
    "lakehouse": {
        "forecast_json": "bronze/latest/forecast_response_cyprus.json",
        "forecast_parquet": "silver/latest/forecast_hourly_cyprus.parquet",
        "forecast_manifest_json": "gold/latest/forecast_manifest_cyprus.json",
    },
    "mesh": {
        "forecast_json": "forecast_services/latest/forecast_response_cyprus.json",
        "forecast_parquet": "forecast_services/latest/forecast_hourly_cyprus.parquet",
        "forecast_report_txt": "climate_reports/latest/forecast_report_cyprus.txt",
        "forecast_plot_png": "climate_reports/latest/forecast_temperature_trend_cyprus.png",
        "forecast_manifest_json": "climate_reports/latest/forecast_manifest_cyprus.json",
    },
}

# Tomorrow, more architectures can be added here, for example:
# ARCHITECTURE_UPLOADS_MONTHLY["datafabric"] = {...}
# ARCHITECTURE_UPLOADS_MONTHLY["semanticfabric"] = {...}
# ARCHITECTURE_UPLOADS_MONTHLY["knowledgegraph"] = {...}


# ---------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Cyprus-wide multi-year weather data into MinIO architectures."
    )
    parser.add_argument("--region-name", default="cyprus")
    parser.add_argument("--timezone", default="Europe/Nicosia")

    # Multi-year defaults
    parser.add_argument("--start", default="2020-01-01", help="Historical start date YYYY-MM-DD")
    parser.add_argument("--end", default="2024-12-31", help="Historical end date YYYY-MM-DD")

    # Forecast is current / latest only
    parser.add_argument("--forecast-days", type=int, default=7)

    parser.add_argument(
        "--minio-endpoint",
        default=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        help="MinIO S3/API endpoint. Use 9000, not 9001.",
    )
    parser.add_argument("--minio-access-key", default=os.getenv("MINIO_ACCESS_KEY", "minio"))
    parser.add_argument("--minio-secret-key", default=os.getenv("MINIO_SECRET_KEY", "minio123"))
    parser.add_argument("--minio-region", default=os.getenv("MINIO_REGION", "us-east-1"))

    parser.add_argument("--output-root", default="data")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--skip-forecast", action="store_true")
    parser.add_argument("--request-delay-seconds", type=float, default=0.15)

    return parser.parse_args()


# ---------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------

def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def content_type_for(path: Path) -> str:
    return CONTENT_TYPES.get(path.suffix.lower(), "application/octet-stream")


def save_text(path: Path, content: str) -> None:
    ensure_dir(path.parent)
    path.write_text(content, encoding="utf-8")


def save_json(path: Path, payload) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def save_dataframe_csv(path: Path, df: pd.DataFrame) -> None:
    ensure_dir(path.parent)
    df.to_csv(path, index=False)


def save_dataframe_parquet(path: Path, df: pd.DataFrame) -> None:
    ensure_dir(path.parent)
    df.to_parquet(path, index=False)


def dataframe_to_json_records(df: pd.DataFrame) -> str:
    safe_df = df.copy()
    for col in safe_df.columns:
        if pd.api.types.is_datetime64_any_dtype(safe_df[col]):
            safe_df[col] = safe_df[col].astype(str)
    return safe_df.to_json(orient="records", indent=2)


def save_dataframe_json(path: Path, df: pd.DataFrame) -> None:
    ensure_dir(path.parent)
    path.write_text(dataframe_to_json_records(df), encoding="utf-8")


def round2(value: float) -> float:
    try:
        return round(float(value), 2)
    except Exception:
        return value


def month_ranges(start_date: str, end_date: str) -> Iterable[Tuple[str, str]]:
    start_ts = pd.Timestamp(start_date).normalize().replace(day=1)
    end_ts = pd.Timestamp(end_date).normalize()

    current = start_ts
    while current <= end_ts:
        next_month = current + pd.offsets.MonthBegin(1)
        month_end = next_month - pd.Timedelta(days=1)
        if month_end > end_ts:
            month_end = end_ts

        yield current.date().isoformat(), month_end.date().isoformat()
        current = next_month


# ---------------------------------------------------------------------
# API / Data building
# ---------------------------------------------------------------------

def fetch_json(session: requests.Session, url: str, params: Dict, retries: int = 3) -> Dict:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, params=params, timeout=60)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(1.0 * attempt)
            else:
                raise RuntimeError(f"Failed request after {retries} attempts: {exc}") from exc
    raise RuntimeError(str(last_error))


def build_hourly_dataframe(payload: Dict, point_name: str, latitude: float, longitude: float) -> pd.DataFrame:
    hourly = payload.get("hourly")
    if not hourly or "time" not in hourly:
        raise ValueError("Payload does not contain hourly data with time values.")

    df = pd.DataFrame(hourly)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    for col in df.columns:
        if col != "time":
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["point_name"] = point_name
    df["latitude"] = latitude
    df["longitude"] = longitude
    return df


def build_enriched_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    enriched = df.copy()
    enriched = enriched.sort_values(["point_name", "time"]).reset_index(drop=True)

    if "temperature_2m" in enriched.columns:
        enriched["temperature_rolling_24h"] = (
            enriched.groupby("point_name")["temperature_2m"]
            .transform(lambda s: s.rolling(window=24, min_periods=1).mean())
        )
        enriched["temperature_band"] = pd.cut(
            enriched["temperature_2m"],
            bins=[-999, 10, 25, 999],
            labels=["low", "medium", "high"],
        ).astype(str)

    if "precipitation" in enriched.columns:
        enriched["is_rainy"] = (enriched["precipitation"].fillna(0) > 0).astype(int)

    if "wind_speed_10m" in enriched.columns:
        enriched["is_windy"] = (enriched["wind_speed_10m"].fillna(0) >= 25).astype(int)

    enriched["ingested_at_utc"] = datetime.now(timezone.utc).isoformat()
    return enriched


def build_daily_summary_by_point(df: pd.DataFrame) -> pd.DataFrame:
    summary = df.copy()
    summary["date"] = pd.to_datetime(summary["time"]).dt.date

    daily = (
        summary.groupby(["point_name", "latitude", "longitude", "date"], as_index=False)
        .agg(
            temperature_2m=("temperature_2m", "mean"),
            relative_humidity_2m=("relative_humidity_2m", "mean"),
            precipitation=("precipitation", "sum"),
            wind_speed_10m=("wind_speed_10m", "max"),
        )
        .rename(
            columns={
                "temperature_2m": "avg_temperature_2m",
                "relative_humidity_2m": "avg_relative_humidity_2m",
                "precipitation": "total_precipitation",
                "wind_speed_10m": "max_wind_speed_10m",
            }
        )
    )
    daily["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
    return daily


def build_cyprus_daily_summary(df: pd.DataFrame, region_name: str) -> pd.DataFrame:
    summary = df.copy()
    summary["date"] = pd.to_datetime(summary["time"]).dt.date

    daily = (
        summary.groupby("date", as_index=False)
        .agg(
            temperature_2m=("temperature_2m", "mean"),
            relative_humidity_2m=("relative_humidity_2m", "mean"),
            precipitation=("precipitation", "sum"),
            wind_speed_10m=("wind_speed_10m", "max"),
        )
        .rename(
            columns={
                "temperature_2m": "cyprus_avg_temperature_2m",
                "relative_humidity_2m": "cyprus_avg_relative_humidity_2m",
                "precipitation": "cyprus_total_precipitation",
                "wind_speed_10m": "cyprus_max_wind_speed_10m",
            }
        )
    )
    daily["region_name"] = region_name
    daily["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
    return daily


def build_forecast_dataframe(session: requests.Session, args: argparse.Namespace) -> Tuple[List[Dict], pd.DataFrame]:
    all_forecast_payloads = []
    forecast_frames = []

    for point in CYPRUS_POINTS:
        params = {
            "latitude": point["latitude"],
            "longitude": point["longitude"],
            "hourly": ",".join(DEFAULT_HOURLY_VARS),
            "forecast_days": args.forecast_days,
            "timezone": args.timezone,
        }
        payload = fetch_json(session, FORECAST_URL, params)
        df = build_hourly_dataframe(
            payload,
            point_name=point["point_name"],
            latitude=point["latitude"],
            longitude=point["longitude"],
        )

        all_forecast_payloads.append(
            {
                "point_name": point["point_name"],
                "latitude": point["latitude"],
                "longitude": point["longitude"],
                "payload": payload,
            }
        )
        forecast_frames.append(df)
        time.sleep(args.request_delay_seconds)

    forecast_df = pd.concat(forecast_frames, ignore_index=True)
    return all_forecast_payloads, forecast_df


# ---------------------------------------------------------------------
# Reporting / plotting
# ---------------------------------------------------------------------

def make_monthly_report(
    region_name: str,
    year: str,
    month: str,
    archive_df: pd.DataFrame,
    start_date: str,
    end_date: str,
) -> str:
    lines = []
    lines.append(f"Climate Report for {region_name.title()} - {year}-{month}")
    lines.append("=" * len(lines[-1]))
    lines.append("")
    lines.append(f"Coverage period: {start_date} to {end_date}")
    lines.append(f"Generated at (UTC): {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"Observation points used: {len(CYPRUS_POINTS)}")
    lines.append(f"Total hourly rows: {len(archive_df)}")
    lines.append("")

    if not archive_df.empty:
        lines.append("Monthly summary")
        lines.append(f"- Mean temperature: {round2(archive_df['temperature_2m'].mean())} °C")
        lines.append(f"- Min temperature: {round2(archive_df['temperature_2m'].min())} °C")
        lines.append(f"- Max temperature: {round2(archive_df['temperature_2m'].max())} °C")
        lines.append(f"- Mean humidity: {round2(archive_df['relative_humidity_2m'].mean())} %")
        lines.append(f"- Total precipitation: {round2(archive_df['precipitation'].sum())} mm")
        lines.append(f"- Max wind speed: {round2(archive_df['wind_speed_10m'].max())} km/h")
        lines.append("")

        point_counts = archive_df.groupby("point_name").size().to_dict()
        lines.append("Rows per observation point")
        for point_name, count in sorted(point_counts.items()):
            lines.append(f"- {point_name}: {count}")

    lines.append("")
    lines.append("Data types generated")
    lines.append("- Structured: CSV and Parquet")
    lines.append("- Semi-structured: JSON payloads and JSON summaries")
    lines.append("- Unstructured: text report and image chart")

    return "\n".join(lines)


def make_full_range_report(
    region_name: str,
    start_date: str,
    end_date: str,
    archive_df: pd.DataFrame,
    forecast_df: pd.DataFrame | None,
) -> str:
    lines = []
    lines.append(f"Historical Climate Report for {region_name.title()} ({start_date} to {end_date})")
    lines.append("=" * len(lines[-1]))
    lines.append("")
    lines.append(f"Generated at (UTC): {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"Observation points used: {len(CYPRUS_POINTS)}")
    lines.append(f"Historical hourly rows: {len(archive_df)}")
    lines.append("")

    if not archive_df.empty:
        lines.append("Historical full-range summary")
        lines.append(f"- Mean temperature: {round2(archive_df['temperature_2m'].mean())} °C")
        lines.append(f"- Min temperature: {round2(archive_df['temperature_2m'].min())} °C")
        lines.append(f"- Max temperature: {round2(archive_df['temperature_2m'].max())} °C")
        lines.append(f"- Mean humidity: {round2(archive_df['relative_humidity_2m'].mean())} %")
        lines.append(f"- Total precipitation: {round2(archive_df['precipitation'].sum())} mm")
        lines.append(f"- Max wind speed: {round2(archive_df['wind_speed_10m'].max())} km/h")

    if forecast_df is not None and not forecast_df.empty:
        lines.append("")
        lines.append("Latest forecast overview")
        lines.append(f"- Forecast hourly rows: {len(forecast_df)}")
        lines.append(f"- Forecast mean temperature: {round2(forecast_df['temperature_2m'].mean())} °C")
        lines.append(f"- Forecast total precipitation: {round2(forecast_df['precipitation'].sum())} mm")

    lines.append("")
    lines.append("Architecture note")
    lines.append("- The same meteorological domain is stored across Data Lake, Lakehouse, and Data Mesh layouts.")
    lines.append("- Monthly partitioned uploads are used for scale and organization.")
    lines.append("- Full-range outputs are also generated for easier validation and querying.")

    return "\n".join(lines)


def make_forecast_report(region_name: str, forecast_df: pd.DataFrame, forecast_days: int) -> str:
    lines = []
    lines.append(f"Forecast Report for {region_name.title()}")
    lines.append("=" * len(lines[-1]))
    lines.append("")
    lines.append(f"Forecast days: {forecast_days}")
    lines.append(f"Generated at (UTC): {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"Forecast rows: {len(forecast_df)}")
    lines.append("")

    if not forecast_df.empty:
        lines.append(f"- Mean forecast temperature: {round2(forecast_df['temperature_2m'].mean())} °C")
        lines.append(f"- Total forecast precipitation: {round2(forecast_df['precipitation'].sum())} mm")
        lines.append(f"- Max forecast wind speed: {round2(forecast_df['wind_speed_10m'].max())} km/h")

    return "\n".join(lines)


def make_temperature_plot(path: Path, df: pd.DataFrame, title: str) -> None:
    if df.empty or "temperature_2m" not in df.columns:
        return

    plot_df = (
        df.groupby("time", as_index=False)["temperature_2m"]
        .mean()
        .sort_values("time")
    )

    ensure_dir(path.parent)
    plt.figure(figsize=(11, 4))
    plt.plot(plot_df["time"], plot_df["temperature_2m"])
    plt.title(title)
    plt.xlabel("Time")
    plt.ylabel("Temperature (°C)")
    plt.xticks(rotation=30)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


# ---------------------------------------------------------------------
# MinIO helpers
# ---------------------------------------------------------------------

def minio_client(endpoint: str, access_key: str, secret_key: str, region: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4"),
    )


def ensure_bucket_exists(s3, bucket_name: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError:
        s3.create_bucket(Bucket=bucket_name)


def upload_file(s3, local_path: Path, bucket: str, key: str) -> None:
    s3.upload_file(
        str(local_path),
        bucket,
        key,
        ExtraArgs={"ContentType": content_type_for(local_path)},
    )


def upload_artifact_group(
    s3,
    local_files: Dict[str, Path],
    upload_config: Dict[str, Dict[str, str]],
    format_kwargs: Dict[str, str],
) -> None:
    for bucket, artifact_map in upload_config.items():
        ensure_bucket_exists(s3, bucket)
        for logical_name, object_key_template in artifact_map.items():
            if logical_name not in local_files:
                continue
            object_key = object_key_template.format(**format_kwargs)
            upload_file(s3, local_files[logical_name], bucket, object_key)
            print(f"Uploaded -> s3://{bucket}/{object_key}")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    try:
        start_ts = pd.Timestamp(args.start)
        end_ts = pd.Timestamp(args.end)
    except Exception as exc:
        raise ValueError("Start/end date must be in YYYY-MM-DD format.") from exc

    if end_ts < start_ts:
        raise ValueError("End date must be greater than or equal to start date.")

    region_slug = slugify(args.region_name)
    run_root = Path(args.output_root) / f"{region_slug}_{args.start}_{args.end}"
    ensure_dir(run_root)

    session = requests.Session()

    all_archive_frames: List[pd.DataFrame] = []
    all_enriched_frames: List[pd.DataFrame] = []
    all_daily_by_point_frames: List[pd.DataFrame] = []
    all_daily_cyprus_frames: List[pd.DataFrame] = []

    if not args.skip_upload:
        print(f"Connecting to MinIO at {args.minio_endpoint} ...")
        s3 = minio_client(
            endpoint=args.minio_endpoint,
            access_key=args.minio_access_key,
            secret_key=args.minio_secret_key,
            region=args.minio_region,
        )
        for bucket in ("lake", "lakehouse", "mesh"):
            ensure_bucket_exists(s3, bucket)
    else:
        s3 = None

    print("Fetching historical Cyprus-wide data month by month...")

    for chunk_start, chunk_end in month_ranges(args.start, args.end):
        month_archive_payloads = []
        month_frames = []

        year = chunk_start[:4]
        month = chunk_start[5:7]

        print(f"  -> {year}-{month} ({chunk_start} to {chunk_end})")

        for point in CYPRUS_POINTS:
            params = {
                "latitude": point["latitude"],
                "longitude": point["longitude"],
                "start_date": chunk_start,
                "end_date": chunk_end,
                "hourly": ",".join(DEFAULT_HOURLY_VARS),
                "timezone": args.timezone,
            }

            payload = fetch_json(session, ARCHIVE_URL, params)
            df = build_hourly_dataframe(
                payload,
                point_name=point["point_name"],
                latitude=point["latitude"],
                longitude=point["longitude"],
            )

            month_archive_payloads.append(
                {
                    "point_name": point["point_name"],
                    "latitude": point["latitude"],
                    "longitude": point["longitude"],
                    "start_date": chunk_start,
                    "end_date": chunk_end,
                    "payload": payload,
                }
            )
            month_frames.append(df)
            time.sleep(args.request_delay_seconds)

        month_archive_df = pd.concat(month_frames, ignore_index=True)
        month_enriched_df = build_enriched_dataframe(month_archive_df)
        month_daily_by_point_df = build_daily_summary_by_point(month_archive_df)
        month_daily_cyprus_df = build_cyprus_daily_summary(month_archive_df, args.region_name)

        all_archive_frames.append(month_archive_df)
        all_enriched_frames.append(month_enriched_df)
        all_daily_by_point_frames.append(month_daily_by_point_df)
        all_daily_cyprus_frames.append(month_daily_cyprus_df)

        month_report = make_monthly_report(
            region_name=args.region_name,
            year=year,
            month=month,
            archive_df=month_archive_df,
            start_date=chunk_start,
            end_date=chunk_end,
        )

        month_manifest = {
            "region_name": args.region_name,
            "year": year,
            "month": month,
            "start_date": chunk_start,
            "end_date": chunk_end,
            "timezone": args.timezone,
            "points": CYPRUS_POINTS,
            "rows": {
                "archive_hourly": int(len(month_archive_df)),
                "enriched_hourly": int(len(month_enriched_df)),
                "daily_by_point": int(len(month_daily_by_point_df)),
                "daily_cyprus": int(len(month_daily_cyprus_df)),
            },
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "architectures": ["lake", "lakehouse", "mesh"],
        }

        month_dir = run_root / "monthly" / f"year={year}" / f"month={month}"
        ensure_dir(month_dir)

        month_files: Dict[str, Path] = {}

        month_files["archive_csv"] = month_dir / "archive_hourly_observations_cyprus.csv"
        month_files["archive_parquet"] = month_dir / "archive_hourly_observations_cyprus.parquet"
        month_files["archive_json"] = month_dir / "archive_response_cyprus.json"
        month_files["enriched_parquet"] = month_dir / "hourly_observations_enriched_cyprus.parquet"
        month_files["daily_by_point_parquet"] = month_dir / "daily_weather_summary_by_point.parquet"
        month_files["daily_by_point_json"] = month_dir / "daily_weather_summary_by_point.json"
        month_files["daily_cyprus_parquet"] = month_dir / "daily_weather_summary_cyprus.parquet"
        month_files["report_txt"] = month_dir / "climate_report_cyprus.txt"
        month_files["plot_png"] = month_dir / "temperature_trend_cyprus.png"
        month_files["manifest_json"] = month_dir / "ingestion_manifest_cyprus.json"

        save_dataframe_csv(month_files["archive_csv"], month_archive_df)
        save_dataframe_parquet(month_files["archive_parquet"], month_archive_df)
        save_json(month_files["archive_json"], month_archive_payloads)
        save_dataframe_parquet(month_files["enriched_parquet"], month_enriched_df)
        save_dataframe_parquet(month_files["daily_by_point_parquet"], month_daily_by_point_df)
        save_dataframe_json(month_files["daily_by_point_json"], month_daily_by_point_df)
        save_dataframe_parquet(month_files["daily_cyprus_parquet"], month_daily_cyprus_df)
        save_text(month_files["report_txt"], month_report)
        make_temperature_plot(
            month_files["plot_png"],
            month_archive_df,
            f"Average Temperature Trend - {args.region_name.title()} - {year}-{month}",
        )
        save_json(month_files["manifest_json"], month_manifest)

        if not args.skip_upload:
            upload_artifact_group(
                s3,
                month_files,
                ARCHITECTURE_UPLOADS_MONTHLY,
                {"year": year, "month": month},
            )

    # -----------------------------------------------------------------
    # Full-range outputs
    # -----------------------------------------------------------------

    archive_full_df = pd.concat(all_archive_frames, ignore_index=True)
    enriched_full_df = pd.concat(all_enriched_frames, ignore_index=True)
    daily_by_point_full_df = pd.concat(all_daily_by_point_frames, ignore_index=True)
    daily_cyprus_full_df = pd.concat(all_daily_cyprus_frames, ignore_index=True)

    forecast_full_df = None
    forecast_payloads = None

    if not args.skip_forecast:
        print("Fetching latest Cyprus-wide forecast...")
        forecast_payloads, forecast_full_df = build_forecast_dataframe(session, args)

    full_range_report = make_full_range_report(
        region_name=args.region_name,
        start_date=args.start,
        end_date=args.end,
        archive_df=archive_full_df,
        forecast_df=forecast_full_df,
    )

    historical_manifest = {
        "region_name": args.region_name,
        "historical_start": args.start,
        "historical_end": args.end,
        "timezone": args.timezone,
        "points": CYPRUS_POINTS,
        "rows": {
            "archive_hourly_full": int(len(archive_full_df)),
            "enriched_hourly_full": int(len(enriched_full_df)),
            "daily_by_point_full": int(len(daily_by_point_full_df)),
            "daily_cyprus_full": int(len(daily_cyprus_full_df)),
        },
        "forecast_included": not args.skip_forecast,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "architectures": ["lake", "lakehouse", "mesh"],
    }

    full_dir = run_root / "full_range"
    ensure_dir(full_dir)

    full_files: Dict[str, Path] = {}

    full_files["archive_full_parquet"] = full_dir / "archive_hourly_observations_cyprus_full.parquet"
    full_files["enriched_full_parquet"] = full_dir / "hourly_observations_enriched_cyprus_full.parquet"
    full_files["daily_by_point_full_parquet"] = full_dir / "daily_weather_summary_by_point_full.parquet"
    full_files["daily_cyprus_full_parquet"] = full_dir / "daily_weather_summary_cyprus_full.parquet"
    full_files["historical_report_txt"] = full_dir / "historical_report_cyprus_full.txt"
    full_files["historical_plot_png"] = full_dir / "temperature_trend_cyprus_full.png"
    full_files["historical_manifest_json"] = full_dir / "historical_manifest_cyprus_full.json"

    save_dataframe_parquet(full_files["archive_full_parquet"], archive_full_df)
    save_dataframe_parquet(full_files["enriched_full_parquet"], enriched_full_df)
    save_dataframe_parquet(full_files["daily_by_point_full_parquet"], daily_by_point_full_df)
    save_dataframe_parquet(full_files["daily_cyprus_full_parquet"], daily_cyprus_full_df)
    save_text(full_files["historical_report_txt"], full_range_report)
    make_temperature_plot(
        full_files["historical_plot_png"],
        archive_full_df,
        f"Average Temperature Trend - {args.region_name.title()} ({args.start} to {args.end})",
    )
    save_json(full_files["historical_manifest_json"], historical_manifest)

    if not args.skip_upload:
        upload_artifact_group(
            s3,
            full_files,
            ARCHITECTURE_UPLOADS_FULL_RANGE,
            {},
        )

    # -----------------------------------------------------------------
    # Latest forecast outputs
    # -----------------------------------------------------------------

    if not args.skip_forecast and forecast_full_df is not None and forecast_payloads is not None:
        forecast_manifest = {
            "region_name": args.region_name,
            "forecast_days": args.forecast_days,
            "timezone": args.timezone,
            "points": CYPRUS_POINTS,
            "rows": {
                "forecast_hourly": int(len(forecast_full_df)),
            },
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "architectures": ["lake", "lakehouse", "mesh"],
        }

        forecast_report = make_forecast_report(
            region_name=args.region_name,
            forecast_df=forecast_full_df,
            forecast_days=args.forecast_days,
        )

        forecast_dir = run_root / "latest_forecast"
        ensure_dir(forecast_dir)

        forecast_files: Dict[str, Path] = {}
        forecast_files["forecast_json"] = forecast_dir / "forecast_response_cyprus.json"
        forecast_files["forecast_parquet"] = forecast_dir / "forecast_hourly_cyprus.parquet"
        forecast_files["forecast_report_txt"] = forecast_dir / "forecast_report_cyprus.txt"
        forecast_files["forecast_plot_png"] = forecast_dir / "forecast_temperature_trend_cyprus.png"
        forecast_files["forecast_manifest_json"] = forecast_dir / "forecast_manifest_cyprus.json"

        save_json(forecast_files["forecast_json"], forecast_payloads)
        save_dataframe_parquet(forecast_files["forecast_parquet"], forecast_full_df)
        save_text(forecast_files["forecast_report_txt"], forecast_report)
        make_temperature_plot(
            forecast_files["forecast_plot_png"],
            forecast_full_df,
            f"Forecast Temperature Trend - {args.region_name.title()}",
        )
        save_json(forecast_files["forecast_manifest_json"], forecast_manifest)

        if not args.skip_upload:
            upload_artifact_group(
                s3,
                forecast_files,
                ARCHITECTURE_UPLOADS_FORECAST,
                {},
            )

    print("")
    print("Done.")
    print(f"Local staging folder: {run_root}")
    print("Architectures updated: lake, lakehouse, mesh")
    print("Script is ready to be extended for more architectures tomorrow.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)