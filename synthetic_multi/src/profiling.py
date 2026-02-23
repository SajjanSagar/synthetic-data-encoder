import sqlite3
from itertools import combinations
from typing import Dict, List, Tuple

from .logging_utils import get_logger


def _get_tables(conn: sqlite3.Connection) -> List[str]:
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    return [row[0] for row in cursor.fetchall()]


def _get_columns(conn: sqlite3.Connection, table: str) -> List[Tuple[str, str]]:
    cursor = conn.execute(f"PRAGMA table_info('{table}')")
    return [(row[1], row[2]) for row in cursor.fetchall()]


def _column_stats(conn: sqlite3.Connection, table: str, column: str) -> Dict[str, int]:
    cursor = conn.execute(
        f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT "{column}") AS distinct_count,
            SUM(CASE WHEN "{column}" IS NULL THEN 1 ELSE 0 END) AS null_count
        FROM "{table}"
        """
    )
    total, distinct_count, null_count = cursor.fetchone()
    return {
        "total": total or 0,
        "distinct": distinct_count or 0,
        "nulls": null_count or 0,
    }


def _composite_stats(
    conn: sqlite3.Connection, table: str, columns: Tuple[str, ...]
) -> Dict[str, int]:
    col_expr = " || '|' || ".join([f'COALESCE("{col}", "")' for col in columns])
    cursor = conn.execute(
        f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT {col_expr}) AS distinct_count,
            SUM(CASE WHEN {" OR ".join([f'"{c}" IS NULL' for c in columns])}
                THEN 1 ELSE 0 END) AS null_count
        FROM "{table}"
        """
    )
    total, distinct_count, null_count = cursor.fetchone()
    return {
        "total": total or 0,
        "distinct": distinct_count or 0,
        "nulls": null_count or 0,
    }


def _infer_primary_keys(
    conn: sqlite3.Connection,
    table: str,
    columns: List[str],
    pk_min_uniqueness: float,
    max_composite_key_size: int,
) -> List[List[str]]:
    candidates: List[List[str]] = []
    for col in columns:
        stats = _column_stats(conn, table, col)
        if stats["total"] == 0:
            continue
        uniqueness = stats["distinct"] / stats["total"]
        if stats["nulls"] == 0 and uniqueness >= pk_min_uniqueness:
            candidates.append([col])

    if max_composite_key_size >= 2:
        for r in range(2, max_composite_key_size + 1):
            for cols in combinations(columns, r):
                stats = _composite_stats(conn, table, cols)
                if stats["total"] == 0:
                    continue
                uniqueness = stats["distinct"] / stats["total"]
                if stats["nulls"] == 0 and uniqueness >= pk_min_uniqueness:
                    candidates.append(list(cols))
    return candidates


def _infer_foreign_keys(
    conn: sqlite3.Connection,
    tables: List[str],
    pk_candidates: Dict[str, List[List[str]]],
    fk_match_threshold: float,
) -> List[Dict[str, str]]:
    relationships: List[Dict[str, str]] = []
    for child_table in tables:
        child_columns = [col for col, _ in _get_columns(conn, child_table)]
        for parent_table in tables:
            if parent_table == child_table:
                continue
            for pk_cols in pk_candidates.get(parent_table, []):
                if len(pk_cols) != 1:
                    continue
                parent_pk = pk_cols[0]
                for child_col in child_columns:
                    cursor = conn.execute(
                        f"""
                        SELECT
                            COUNT(DISTINCT c."{child_col}") AS child_distinct,
                            COUNT(
                                DISTINCT CASE
                                    WHEN p."{parent_pk}" IS NOT NULL THEN c."{child_col}"
                                END
                            ) AS joined_distinct
                        FROM "{child_table}" c
                        LEFT JOIN "{parent_table}" p
                            ON c."{child_col}" = p."{parent_pk}"
                        WHERE c."{child_col}" IS NOT NULL
                        """
                    )
                    child_distinct, joined_distinct = cursor.fetchone()
                    if not child_distinct:
                        continue
                    match_ratio = (joined_distinct or 0) / child_distinct
                    if match_ratio >= fk_match_threshold:
                        relationships.append(
                            {
                                "parent_table": parent_table,
                                "parent_key": parent_pk,
                                "child_table": child_table,
                                "child_key": child_col,
                                "cardinality": "many-to-one",
                                "match_ratio": round(match_ratio, 4),
                            }
                        )
    return _deduplicate_circular_relationships(relationships)


def _deduplicate_circular_relationships(
    relationships: List[Dict[str, str]],
) -> List[Dict[str, str]]:
    """Remove circular FK pairs (A->B and B->A); keep parent before child alphabetically."""
    seen_pairs: set = set()
    result = []
    for rel in relationships:
        pair = (rel["parent_table"], rel["child_table"], rel["parent_key"], rel["child_key"])
        reverse_pair = (rel["child_table"], rel["parent_table"], rel["child_key"], rel["parent_key"])
        if reverse_pair in seen_pairs:
            continue  # Already have reverse; skip this one
        if pair in seen_pairs:
            continue  # Duplicate
        # For circular pairs, keep parent < child (items->matched_items not matched_items->items)
        if any(
            r["parent_table"] == rel["child_table"]
            and r["child_table"] == rel["parent_table"]
            and r["parent_key"] == rel["child_key"]
            and r["child_key"] == rel["parent_key"]
            for r in relationships
        ):
            if rel["parent_table"] >= rel["child_table"]:
                continue  # Skip; we'll keep the other direction
        seen_pairs.add(pair)
        result.append(rel)
    return result


def profile_database(
    db_path: str,
    pk_min_uniqueness: float = 0.999,
    fk_match_threshold: float = 0.9,
    max_composite_key_size: int = 2,
) -> Dict[str, object]:
    logger = get_logger(__name__)
    conn = sqlite3.connect(db_path)
    try:
        tables = _get_tables(conn)
        logger.info("Profiling tables: %s", tables)
        table_profiles: Dict[str, Dict[str, object]] = {}
        pk_candidates: Dict[str, List[List[str]]] = {}
        for table in tables:
            columns = _get_columns(conn, table)
            column_names = [col for col, _ in columns]
            pk_candidates[table] = _infer_primary_keys(
                conn,
                table,
                column_names,
                pk_min_uniqueness,
                max_composite_key_size,
            )
            logger.info(
                "PK candidates for %s: %s", table, pk_candidates[table] or "None"
            )
            table_profiles[table] = {
                "columns": [{"name": col, "type": col_type} for col, col_type in columns],
                "pk_candidates": pk_candidates[table],
            }

        relationships = _infer_foreign_keys(
            conn, tables, pk_candidates, fk_match_threshold
        )
        logger.info("Inferred relationships: %s", len(relationships))
        return {
            "tables": table_profiles,
            "relationships": relationships,
        }
    finally:
        conn.close()
