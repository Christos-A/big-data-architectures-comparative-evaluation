#!/usr/bin/env python3
"""
test_field6.py

Field 6 automated evidence collector for the thesis architectures.

Purpose
-------
This script does NOT replace Step 1 reference weighting.
It helps produce Step 2 implementation/testing evidence for Field 6.

Outputs
-------
1. field6_architecture_summary.csv
2. field6_automated_checks.csv
3. field6_manual_checks_template.csv
4. field6_notes.txt

How to use
----------
pip install trino
python scripts/test_field6.py

Default Trino connection:
- host: localhost
- port: 8080
- user: trino

The script is conservative:
- It auto-scores only checks that can be observed safely from Trino.
- It leaves non-scriptable checks for the manual evidence template.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import trino


EXPECTED = {
    "historical_hourly": 263088,
    "daily_summary": 1827,
    "latest_forecast": 1008,
    "fabric_inventory": 6,
    "kg_ontology_terms": 9,
    "kg_triples": 1315440,
}

ARCHITECTURES: Dict[str, Dict[str, Any]] = {
    "lake": {
        "source": ("lake.raw.cyprus_hourly_full_source", EXPECTED["historical_hourly"]),
        "transform": ("lake.enriched.cyprus_hourly_enriched_full_source", EXPECTED["historical_hourly"]),
        "final": ("lake.curated.cyprus_daily_summary_full", EXPECTED["daily_summary"]),
        "aux": [("lake.enriched.cyprus_forecast_hourly_latest_source", EXPECTED["latest_forecast"])],
        "expected_prefixes": ["lake.raw.", "lake.enriched.", "lake.curated."],
    },
    "lakehouse": {
        "source": ("iceberg.bronze.cyprus_hourly_full", EXPECTED["historical_hourly"]),
        "transform": ("iceberg.silver.cyprus_hourly_clean_full", EXPECTED["historical_hourly"]),
        "final": ("iceberg.gold.cyprus_daily_summary_full", EXPECTED["daily_summary"]),
        "aux": [],
        "expected_prefixes": ["iceberg.bronze.", "iceberg.silver.", "iceberg.gold."],
    },
    "mesh": {
        "source": ("mesh.station_observations.hourly_observations_full", EXPECTED["historical_hourly"]),
        "transform": ("mesh.forecast_services.hourly_forecast_latest", EXPECTED["latest_forecast"]),
        "final": ("mesh.climate_reports.daily_summary_full", EXPECTED["daily_summary"]),
        "aux": [],
        "expected_prefixes": [
            "mesh.station_observations.",
            "mesh.forecast_services.",
            "mesh.climate_reports.",
        ],
    },
    "fabric": {
        "source": ("fabric.ingest.cyprus_hourly_ingest", EXPECTED["historical_hourly"]),
        "transform": ("fabric.integrated.cyprus_weather_unified", EXPECTED["historical_hourly"]),
        "final": ("fabric.metadata.cyprus_dataset_inventory", EXPECTED["fabric_inventory"]),
        "aux": [],
        "expected_prefixes": ["fabric.ingest.", "fabric.integrated.", "fabric.metadata."],
    },
    "semanticfabric": {
        "source": ("semanticfabric.source.cyprus_hourly_source", EXPECTED["historical_hourly"]),
        "transform": ("semanticfabric.mappings.weather_semantic_mappings", EXPECTED["historical_hourly"]),
        "final": ("semanticfabric.products.cyprus_climate_product", EXPECTED["daily_summary"]),
        "aux": [],
        "expected_prefixes": [
            "semanticfabric.source.",
            "semanticfabric.mappings.",
            "semanticfabric.products.",
        ],
    },
    "knowledgegraph": {
        "source": ("knowledgegraph.source.cyprus_hourly_source", EXPECTED["historical_hourly"]),
        "transform": ("knowledgegraph.ontology.weather_ontology_terms", EXPECTED["kg_ontology_terms"]),
        "final": ("knowledgegraph.triples.weather_observation_triples", EXPECTED["kg_triples"]),
        "aux": [],
        "expected_prefixes": [
            "knowledgegraph.source.",
            "knowledgegraph.ontology.",
            "knowledgegraph.triples.",
        ],
    },
}

# Manual-only or hybrid checks that should be reviewed by the user.
MANUAL_TEMPLATE_ROWS = [
    # Low
    ("L1", "C1", "Processing mainly uses manual SQL/Python/scripts", "manual", ""),
    ("L1", "C2", "No reusable orchestrated workflow is present", "manual", ""),
    ("L1", "C3", "Jobs are run independently", "manual", ""),
    ("L1", "C4", "Outputs depend on manual sequencing", "manual", ""),
    ("L2", "C1", "No stable layer sequence exists", "manual", ""),
    ("L2", "C2", "Flow is not documented or inferable", "manual", ""),
    ("L2", "C3", "Source-to-output path changes between runs", "manual", ""),
    ("L3", "C1", "No central dependency management", "manual", ""),
    ("L3", "C2", "No automatic retry mechanism", "manual", ""),
    ("L3", "C3", "No central monitoring", "manual", ""),
    ("L3", "C4", "Failure handling is embedded locally in jobs/scripts", "manual", ""),
    # Medium / hybrid
    ("M2", "C1", "Processing mainly uses SQL", "manual_or_script_note", ""),
    ("M2", "C2", "Processing mainly uses Python/custom code", "manual_or_script_note", ""),
    ("M2", "C3", "Processing mainly uses engine-native logic", "manual_or_script_note", ""),
    ("M2", "C4", "No dedicated orchestration tool is required", "manual", ""),
    ("M4", "C4", "Monitoring is not provided by a dedicated orchestration layer", "manual", ""),
    # High
    ("H1", "C1", "Dedicated integration/orchestration tool exists", "manual", ""),
    ("H1", "C2", "It is part of the architecture, not external ad-hoc help", "manual", ""),
    ("H1", "C3", "It is actually used in the implemented process", "manual", ""),
    ("H2", "C1", "Pipeline/DAG abstraction exists", "manual", ""),
    ("H2", "C2", "Dependency handling exists", "manual", ""),
    ("H2", "C3", "Retry logic exists", "manual", ""),
    ("H2", "C4", "Tool-backed monitoring exists", "manual", ""),
    ("H3", "C1", "New datasets can be added mostly through configuration", "manual", ""),
    ("H3", "C2", "Metadata/mappings drive part of the process", "manual", ""),
    ("H3", "C3", "Logic is reusable across sources", "manual_or_script_note", ""),
    ("H3", "C4", "Process is not separately hardcoded each time", "manual", ""),
    ("H4", "C1", "Batch is supported in a tool-backed way", "manual", ""),
    ("H4", "C2", "Streaming/event-driven is supported in a tool-backed way", "manual", ""),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Collect automated Field 6 evidence from Trino.")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", default=8080, type=int)
    p.add_argument("--user", default="trino")
    p.add_argument("--http-scheme", default="http", choices=["http", "https"])
    p.add_argument("--output-dir", default="field6_test_output")
    return p.parse_args()


def connect(args: argparse.Namespace):
    return trino.dbapi.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        http_scheme=args.http_scheme,
    )


def run_scalar(conn, query: str) -> Optional[Any]:
    cur = conn.cursor()
    cur.execute(query)
    row = cur.fetchone()
    return None if row is None else row[0]


def safe_scalar(conn, query: str) -> Tuple[bool, Optional[Any], str]:
    try:
        value = run_scalar(conn, query)
        return True, value, ""
    except Exception as exc:
        return False, None, str(exc)


def fq_to_parts(fq_table: str) -> Tuple[str, str, str]:
    catalog, schema, table = fq_table.split(".", 2)
    return catalog, schema, table


def table_exists(conn, fq_table: str) -> Tuple[bool, str]:
    catalog, schema, table = fq_to_parts(fq_table)
    query = f"""
    SELECT count(*)
    FROM {catalog}.information_schema.tables
    WHERE table_schema = '{schema}'
      AND table_name = '{table}'
    """
    ok, value, err = safe_scalar(conn, query)
    if not ok:
        return False, err
    return (value or 0) > 0, ""


def row_count(conn, fq_table: str) -> Tuple[bool, Optional[int], str]:
    return safe_scalar(conn, f"SELECT count(*) FROM {fq_table}")


def sample_query_ok(conn, fq_table: str) -> Tuple[bool, str]:
    ok, _, err = safe_scalar(conn, f"SELECT 1 FROM {fq_table} LIMIT 1")
    return ok, err


def expected_match_score(actual: Optional[int], expected: int) -> float:
    if actual is None:
        return 0.0
    if actual == expected:
        return 1.0
    if actual > 0:
        # partial if the table exists and has data but not the exact expected value
        return 0.5
    return 0.0


def prefix_ok(fq_table: str, expected_prefixes: List[str]) -> float:
    return 1.0 if any(fq_table.startswith(p) for p in expected_prefixes) else 0.0


def evaluate_architecture(conn, arch_name: str, config: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    stage_defs = {
        "source": config["source"],
        "transform": config["transform"],
        "final": config["final"],
    }

    summary: Dict[str, Any] = {"architecture": arch_name}
    auto_rows: List[Dict[str, Any]] = []

    stage_results = {}

    for stage_name, (fq_table, expected_rows) in stage_defs.items():
        exists, exists_err = table_exists(conn, fq_table)
        count_ok, count_value, count_err = (False, None, "")
        sample_ok, sample_err = (False, "")

        if exists:
            count_ok, count_value, count_err = row_count(conn, fq_table)
            sample_ok, sample_err = sample_query_ok(conn, fq_table)

        stage_results[stage_name] = {
            "table": fq_table,
            "exists": 1.0 if exists else 0.0,
            "exists_error": exists_err,
            "count_ok": count_ok,
            "count_value": count_value,
            "count_error": count_err,
            "sample_ok": 1.0 if sample_ok else 0.0,
            "sample_error": sample_err,
            "expected_rows": expected_rows,
            "count_match": expected_match_score(count_value if count_ok else None, expected_rows),
            "prefix_ok": prefix_ok(fq_table, config["expected_prefixes"]),
        }

        summary[f"{stage_name}_table"] = fq_table
        summary[f"{stage_name}_exists"] = exists
        summary[f"{stage_name}_row_count"] = count_value if count_ok else ""
        summary[f"{stage_name}_expected_rows"] = expected_rows
        summary[f"{stage_name}_count_match_score"] = stage_results[stage_name]["count_match"]

    # Optional aux tables
    aux_results = []
    for fq_table, expected_rows in config.get("aux", []):
        exists, exists_err = table_exists(conn, fq_table)
        count_ok, count_value, count_err = (False, None, "")
        if exists:
            count_ok, count_value, count_err = row_count(conn, fq_table)
        aux_results.append({
            "table": fq_table,
            "exists": 1.0 if exists else 0.0,
            "row_count": count_value if count_ok else "",
            "expected_rows": expected_rows,
            "count_match": expected_match_score(count_value if count_ok else None, expected_rows),
            "error": exists_err or count_err,
        })
    summary["aux_tables_json"] = json.dumps(aux_results)

    # ---------------------------
    # Automated evidence checks
    # ---------------------------

    # M1
    auto_rows.extend([
        make_check_row(arch_name, "M1", "C1", "Ingestion/source stage exists", stage_results["source"]["exists"], "script"),
        make_check_row(arch_name, "M1", "C2", "Transformation stage exists", stage_results["transform"]["exists"], "script"),
        make_check_row(arch_name, "M1", "C3", "Final/product stage exists", stage_results["final"]["exists"], "script"),
        make_check_row(
            arch_name, "M1", "C4",
            "Same stage sequence is queryable with non-zero data",
            1.0 if all(
                stage_results[s]["exists"] == 1.0 and
                (stage_results[s]["count_value"] or 0) > 0
                for s in ("source", "transform", "final")
            ) else 0.0,
            "script"
        ),
    ])

    # M3
    auto_rows.extend([
        make_check_row(
            arch_name, "M3", "C1",
            "Expected stage tables exist",
            1.0 if all(stage_results[s]["exists"] == 1.0 for s in ("source", "transform", "final")) else 0.0,
            "script"
        ),
        make_check_row(
            arch_name, "M3", "C2",
            "Outputs contain non-zero rows in all expected stages",
            1.0 if all((stage_results[s]["count_value"] or 0) > 0 for s in ("source", "transform", "final")) else 0.0,
            "script"
        ),
        make_check_row(
            arch_name, "M3", "C3",
            "Naming/location logic matches expected schemas",
            sum(stage_results[s]["prefix_ok"] for s in ("source", "transform", "final")) / 3.0,
            "script"
        ),
        make_check_row(
            arch_name, "M3", "C4",
            "Basic queryability succeeds for all stages",
            sum(stage_results[s]["sample_ok"] for s in ("source", "transform", "final")) / 3.0,
            "script"
        ),
    ])

    # M4
    auto_rows.extend([
        make_check_row(
            arch_name, "M4", "C1",
            "Validation queries/checks can be executed",
            1.0 if all(stage_results[s]["sample_ok"] == 1.0 for s in ("source", "transform", "final")) else 0.0,
            "script"
        ),
        make_check_row(
            arch_name, "M4", "C2",
            "Row-count checks can be executed and compared",
            sum(stage_results[s]["count_match"] for s in ("source", "transform", "final")) / 3.0,
            "script"
        ),
        make_check_row(
            arch_name, "M4", "C3",
            "Rebuilt tables appear populated and stable enough to validate",
            1.0 if all((stage_results[s]["count_value"] or 0) > 0 for s in ("source", "transform", "final")) else 0.0,
            "script"
        ),
    ])

    # Partial L2 evidence (inverse of clear repeated flow)
    auto_rows.append(
        make_check_row(
            arch_name, "L2", "C4",
            "No consistent output stage exists (inverse evidence; 1 means Low trait present)",
            0.0 if stage_results["final"]["exists"] == 1.0 and (stage_results["final"]["count_value"] or 0) > 0 else 1.0,
            "script"
        )
    )

    return summary, auto_rows


def make_check_row(
    architecture: str,
    bullet: str,
    check_id: str,
    description: str,
    automated_value: float,
    evidence_source: str,
) -> Dict[str, Any]:
    return {
        "architecture": architecture,
        "bullet": bullet,
        "check_id": check_id,
        "description": description,
        "automated_value": automated_value,
        "evidence_source": evidence_source,
        "manual_value": "",
        "final_value_for_step2": "",
        "notes": "",
    }


def build_manual_template(output_path: Path) -> None:
    rows = []
    for arch in ARCHITECTURES.keys():
        for bullet, check_id, description, source_kind, notes in MANUAL_TEMPLATE_ROWS:
            rows.append({
                "architecture": arch,
                "bullet": bullet,
                "check_id": check_id,
                "description": description,
                "evidence_source": source_kind,
                "manual_value_0_0.5_1": "",
                "notes": notes,
            })

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "architecture",
                "bullet",
                "check_id",
                "description",
                "evidence_source",
                "manual_value_0_0.5_1",
                "notes",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def write_notes(path: Path) -> None:
    notes = """Field 6 testing notes

What the script covers automatically
- M1: existence of source / transform / final stages
- M3: stage existence, naming/prefix consistency, queryability
- M4: validation-query feasibility and row-count comparison
- one partial inverse check for L2

What still needs manual scoring
- L1, most of L2, L3
- M2
- M4.C4
- H1, H2, H4
- most of H3

How to use the outputs
1. field6_architecture_summary.csv:
   quick view of expected tables and row counts per architecture
2. field6_automated_checks.csv:
   pre-filled script-based evidence checks
3. field6_manual_checks_template.csv:
   fill remaining non-scriptable checks using 0 / 0.5 / 1

Then combine automated + manual checks to compute the final Step 2 bullet percentages.
"""
    path.write_text(notes, encoding="utf-8")


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = connect(args)

    summaries: List[Dict[str, Any]] = []
    automated_rows: List[Dict[str, Any]] = []

    for arch_name, config in ARCHITECTURES.items():
        summary, rows = evaluate_architecture(conn, arch_name, config)
        summaries.append(summary)
        automated_rows.extend(rows)

    with (output_dir / "field6_architecture_summary.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(summaries[0].keys()))
        writer.writeheader()
        writer.writerows(summaries)

    with (output_dir / "field6_automated_checks.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "architecture",
                "bullet",
                "check_id",
                "description",
                "automated_value",
                "evidence_source",
                "manual_value",
                "final_value_for_step2",
                "notes",
            ],
        )
        writer.writeheader()
        writer.writerows(automated_rows)

    build_manual_template(output_dir / "field6_manual_checks_template.csv")
    write_notes(output_dir / "field6_notes.txt")

    print(f"Done. Output folder: {output_dir.resolve()}")
    print("Created:")
    print("- field6_architecture_summary.csv")
    print("- field6_automated_checks.csv")
    print("- field6_manual_checks_template.csv")
    print("- field6_notes.txt")


if __name__ == "__main__":
    main()
