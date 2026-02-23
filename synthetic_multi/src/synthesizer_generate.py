import json
import os
import pickle
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import yaml

from .privacy_masking import apply_masking
from .logging_utils import get_logger


def _load_model(path: str):
    with open(path, "rb") as handle:
        return pickle.load(handle)


def _table_counts(conn: sqlite3.Connection) -> Dict[str, int]:
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    tables = [row[0] for row in cursor.fetchall()]
    counts = {}
    for table in tables:
        count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        counts[table] = count
    return counts


def _child_count_distribution(
    conn: sqlite3.Connection, child_table: str, child_key: str
) -> List[int]:
    cursor = conn.execute(
        f"""
        SELECT COUNT(*) AS child_count
        FROM "{child_table}"
        WHERE "{child_key}" IS NOT NULL
        GROUP BY "{child_key}"
        """
    )
    counts = [row[0] for row in cursor.fetchall()]
    return counts if counts else [0]


def _sample_child_counts(counts: List[int], num_parents: int) -> List[int]:
    import random

    if not counts:
        return [0] * num_parents
    return [random.choice(counts) for _ in range(num_parents)]


def _generate_with_fallback(
    db_path: str,
    metadata: Dict[str, object],
    model_dir: str,
    scale: float,
) -> Dict[str, pd.DataFrame]:
    conn = sqlite3.connect(db_path)
    try:
        table_counts = _table_counts(conn)
        relationships = metadata.get("relationships", [])
        parent_tables = {rel["parent_table"] for rel in relationships}
        child_tables = {rel["child_table"] for rel in relationships}

        tables = {}
        for table in metadata["tables"]:
            if table in child_tables:
                continue
            model_path = os.path.join(model_dir, f"{table}.pkl")
            synthesizer = _load_model(model_path)
            target_rows = int(table_counts.get(table, 0) * scale)
            tables[table] = synthesizer.sample(num_rows=target_rows)

        for rel in relationships:
            child_table = rel["child_table"]
            parent_table = rel["parent_table"]
            child_key = rel["child_key"]
            parent_key = rel["parent_key"]
            if child_table in tables:
                continue

            parent_df = tables[parent_table]
            counts = _child_count_distribution(conn, child_table, child_key)
            child_counts = _sample_child_counts(counts, len(parent_df))
            total_child_rows = sum(child_counts)

            model_path = os.path.join(model_dir, f"{child_table}.pkl")
            synthesizer = _load_model(model_path)
            child_df = synthesizer.sample(num_rows=total_child_rows)

            fk_values = []
            for parent_value, count in zip(parent_df[parent_key].tolist(), child_counts):
                fk_values.extend([parent_value] * count)

            child_df[child_key] = fk_values[: len(child_df)]
            tables[child_table] = child_df

        return tables
    finally:
        conn.close()


def generate_synthetic_data(
    db_path: str,
    metadata_path: str,
    privacy_rules_path: str,
    model_dir: str,
    output_dir: str,
    scale: float,
    salt_env_var: str,
    random_seed: Optional[int] = None,
) -> Dict[str, str]:
    if random_seed is not None:
        import random

        random.seed(random_seed)
        try:
            import numpy as np

            np.random.seed(random_seed)
        except Exception:
            pass
    logger = get_logger(__name__)
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    with open(metadata_path, "r", encoding="utf-8") as handle:
        metadata = json.load(handle)

    with open(privacy_rules_path, "r", encoding="utf-8") as handle:
        privacy_rules = yaml.safe_load(handle) or {}

    multi_model_path = os.path.join(model_dir, "multi_table.pkl")
    tables: Dict[str, pd.DataFrame]
    if os.path.exists(multi_model_path):
        model = _load_model(multi_model_path)
        tables = model.sample(scale=scale)
        logger.info("Generated synthetic tables with multi-table model.")
    else:
        tables = _generate_with_fallback(db_path, metadata, model_dir, scale)
        logger.info("Generated synthetic tables with fallback strategy.")

    tables = apply_masking(tables, privacy_rules, salt_env_var)

    outputs = {}
    for table_name, df in tables.items():
        out_path = os.path.join(output_dir, f"{table_name}.csv")
        df.to_csv(out_path, index=False)
        outputs[table_name] = out_path
        logger.info("Wrote synthetic table: %s", out_path)

    return outputs
