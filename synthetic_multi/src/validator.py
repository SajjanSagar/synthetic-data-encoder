import json
import sqlite3
from pathlib import Path
from statistics import mean, median
from typing import Dict, List

import yaml

from .csv_to_sqlite import load_csvs_to_sqlite
from .logging_utils import get_logger


def _get_table_schema(conn: sqlite3.Connection, table: str) -> Dict[str, str]:
    cursor = conn.execute(f"PRAGMA table_info('{table}')")
    return {row[1]: row[2] for row in cursor.fetchall()}


def _get_tables(conn: sqlite3.Connection) -> List[str]:
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    return [row[0] for row in cursor.fetchall()]


def _fk_orphan_count(conn: sqlite3.Connection, rel: Dict[str, str]) -> int:
    query = f"""
        SELECT COUNT(*)
        FROM "{rel['child_table']}" c
        LEFT JOIN "{rel['parent_table']}" p
            ON c."{rel['child_key']}" = p."{rel['parent_key']}"
        WHERE c."{rel['child_key']}" IS NOT NULL
          AND p."{rel['parent_key']}" IS NULL
    """
    return conn.execute(query).fetchone()[0]


def _child_counts(conn: sqlite3.Connection, rel: Dict[str, str]) -> List[int]:
    query = f"""
        SELECT COUNT(*) AS child_count
        FROM "{rel['child_table']}"
        WHERE "{rel['child_key']}" IS NOT NULL
        GROUP BY "{rel['child_key']}"
    """
    return [row[0] for row in conn.execute(query).fetchall()]


def validate_synthetic(
    staging_db: str,
    synthetic_csv_dir: str,
    synthetic_db: str,
    relationships_path: str,
    reports_dir: str,
) -> Dict[str, object]:
    logger = get_logger(__name__)
    Path(reports_dir).mkdir(parents=True, exist_ok=True)
    load_csvs_to_sqlite(synthetic_csv_dir, synthetic_db, empty_string_as_null=True)

    report: Dict[str, object] = {
        "schema_parity": [],
        "fk_integrity": [],
        "cardinality_similarity": [],
    }

    real_conn = sqlite3.connect(staging_db)
    synth_conn = sqlite3.connect(synthetic_db)
    try:
        real_tables = _get_tables(real_conn)
        synth_tables = _get_tables(synth_conn)

        for table in real_tables:
            real_schema = _get_table_schema(real_conn, table)
            synth_schema = _get_table_schema(synth_conn, table)
            report["schema_parity"].append(
                {
                    "table": table,
                    "missing_in_synthetic": [
                        col for col in real_schema if col not in synth_schema
                    ],
                    "extra_in_synthetic": [
                        col for col in synth_schema if col not in real_schema
                    ],
                }
            )

        with open(relationships_path, "r", encoding="utf-8") as handle:
            relationships = yaml.safe_load(handle) or []

        for rel in relationships:
            orphans = _fk_orphan_count(synth_conn, rel)
            report["fk_integrity"].append(
                {
                    "relationship": rel,
                    "orphan_rows": orphans,
                }
            )

            real_counts = _child_counts(real_conn, rel)
            synth_counts = _child_counts(synth_conn, rel)
            report["cardinality_similarity"].append(
                {
                    "relationship": rel,
                    "real_mean": mean(real_counts) if real_counts else 0,
                    "synthetic_mean": mean(synth_counts) if synth_counts else 0,
                    "real_median": median(real_counts) if real_counts else 0,
                    "synthetic_median": median(synth_counts) if synth_counts else 0,
                }
            )
    finally:
        real_conn.close()
        synth_conn.close()

    report_path = Path(reports_dir) / "validation.json"
    with open(report_path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)

    md_path = Path(reports_dir) / "validation.md"
    with open(md_path, "w", encoding="utf-8") as handle:
        handle.write("# Validation Report\n\n")
        handle.write("## Schema Parity\n")
        for item in report["schema_parity"]:
            handle.write(
                f"- {item['table']}: missing={item['missing_in_synthetic']}, "
                f"extra={item['extra_in_synthetic']}\n"
            )
        handle.write("\n## FK Integrity\n")
        for item in report["fk_integrity"]:
            rel = item["relationship"]
            handle.write(
                f"- {rel['child_table']}.{rel['child_key']} -> "
                f"{rel['parent_table']}.{rel['parent_key']}: "
                f"orphans={item['orphan_rows']}\n"
            )
        handle.write("\n## Cardinality Similarity\n")
        for item in report["cardinality_similarity"]:
            rel = item["relationship"]
            handle.write(
                f"- {rel['child_table']} to {rel['parent_table']}: "
                f"real_mean={item['real_mean']:.2f}, "
                f"synthetic_mean={item['synthetic_mean']:.2f}, "
                f"real_median={item['real_median']:.2f}, "
                f"synthetic_median={item['synthetic_median']:.2f}\n"
            )

    logger.info("Validation reports written to %s", reports_dir)
    return report


def summarize_report(report: Dict[str, object]) -> Dict[str, object]:
    schema_ok = all(
        not item["missing_in_synthetic"] and not item["extra_in_synthetic"]
        for item in report.get("schema_parity", [])
    )
    fk_issues = sum(item["orphan_rows"] for item in report.get("fk_integrity", []))
    cardinality = []
    for item in report.get("cardinality_similarity", []):
        rel = item["relationship"]
        cardinality.append(
            {
                "relationship": (
                    f"{rel['child_table']}.{rel['child_key']} -> "
                    f"{rel['parent_table']}.{rel['parent_key']}"
                ),
                "real_mean": item["real_mean"],
                "synthetic_mean": item["synthetic_mean"],
                "real_median": item["real_median"],
                "synthetic_median": item["synthetic_median"],
            }
        )
    return {
        "schema_parity_ok": schema_ok,
        "fk_orphan_rows": fk_issues,
        "cardinality": cardinality,
    }
