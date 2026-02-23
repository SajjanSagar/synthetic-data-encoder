import json
import sqlite3
from typing import Dict, List

from .logging_utils import get_logger

def _get_columns(conn: sqlite3.Connection, table: str) -> List[Dict[str, str]]:
    cursor = conn.execute(f"PRAGMA table_info('{table}')")
    return [{"name": row[1], "type": row[2]} for row in cursor.fetchall()]


def _sqlite_type_to_sdv(sqlite_type: str) -> str:
    normalized = sqlite_type.upper()
    if "INT" in normalized:
        return "numerical"
    if "REAL" in normalized or "FLOAT" in normalized or "DOUBLE" in normalized:
        return "numerical"
    if "BOOL" in normalized:
        return "boolean"
    if "DATE" in normalized or "TIME" in normalized:
        return "datetime"
    return "categorical"


def build_metadata(
    db_path: str,
    confirmed_schema: Dict[str, object],
    output_path: str,
) -> Dict[str, object]:
    logger = get_logger(__name__)
    conn = sqlite3.connect(db_path)
    relationships = confirmed_schema.get("relationships", [])
    id_columns_by_table: Dict[str, set] = {}
    for rel in relationships:
        id_columns_by_table.setdefault(rel["parent_table"], set()).add(
            rel["parent_key"]
        )
        id_columns_by_table.setdefault(rel["child_table"], set()).add(rel["child_key"])

    metadata = {"tables": {}, "relationships": relationships}
    try:
        for table, table_info in confirmed_schema.get("tables", {}).items():
            columns = _get_columns(conn, table)
            primary_keys = table_info.get("primary_key", [])
            id_columns = set(primary_keys)
            id_columns.update(id_columns_by_table.get(table, set()))
            metadata["tables"][table] = {
                "primary_key": table_info.get("primary_key", []),
                "columns": {
                    col["name"]: {
                        "sdtype": "id"
                        if col["name"] in id_columns
                        else _sqlite_type_to_sdv(col["type"])
                    }
                    for col in columns
                },
            }
    finally:
        conn.close()

    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(metadata, handle, indent=2)
    logger.info("Saved metadata to %s", output_path)
    return metadata
