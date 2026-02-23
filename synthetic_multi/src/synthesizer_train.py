import json
import os
import pickle
import sqlite3
from pathlib import Path
from typing import Dict

import pandas as pd

from .logging_utils import get_logger


def _load_tables(conn: sqlite3.Connection, tables: Dict[str, object]) -> Dict[str, pd.DataFrame]:
    data = {}
    for table in tables:
        data[table] = pd.read_sql_query(f'SELECT * FROM "{table}"', conn)
    return data


def _build_sdv_metadata(metadata_dict: Dict[str, object], logger):
    try:
        from sdv.metadata import MultiTableMetadata
    except Exception as exc:  # pragma: no cover - fallback
        raise ImportError("SDV is required for training.") from exc

    sdv_metadata = {"tables": {}, "relationships": []}
    for table_name, table_info in metadata_dict["tables"].items():
        pk_list = table_info.get("primary_key", [])
        table_entry = {"columns": table_info["columns"]}
        if len(pk_list) == 1:
            table_entry["primary_key"] = pk_list[0]
        elif len(pk_list) > 1:
            logger.warning(
                "Composite PK detected for %s; SDV metadata will not set a primary key.",
                table_name,
            )
        sdv_metadata["tables"][table_name] = table_entry

    for rel in metadata_dict.get("relationships", []):
        sdv_metadata["relationships"].append(
            {
                "parent_table_name": rel["parent_table"],
                "parent_primary_key": rel["parent_key"],
                "child_table_name": rel["child_table"],
                "child_foreign_key": rel["child_key"],
            }
        )

    return MultiTableMetadata.load_from_dict(sdv_metadata)


def _get_multitable_synthesizer(metadata):
    try:
        from sdv.multi_table import HMASynthesizer
    except Exception:
        return None
    return HMASynthesizer(metadata)


def _get_single_table_synthesizer(metadata):
    try:
        from sdv.single_table import GaussianCopulaSynthesizer
    except Exception as exc:  # pragma: no cover
        raise ImportError("SDV single-table synthesizer not available.") from exc
    return GaussianCopulaSynthesizer(metadata)


def train_synthesizer(
    db_path: str,
    metadata_path: str,
    model_dir: str,
) -> Dict[str, str]:
    logger = get_logger(__name__)
    Path(model_dir).mkdir(parents=True, exist_ok=True)
    with open(metadata_path, "r", encoding="utf-8") as handle:
        metadata_dict = json.load(handle)

    conn = sqlite3.connect(db_path)
    try:
        tables = _load_tables(conn, metadata_dict["tables"])
    finally:
        conn.close()

    models: Dict[str, str] = {}
    metadata = _build_sdv_metadata(metadata_dict, logger)
    multi_model = _get_multitable_synthesizer(metadata)
    if multi_model:
        multi_model.fit(tables)
        model_path = os.path.join(model_dir, "multi_table.pkl")
        with open(model_path, "wb") as handle:
            pickle.dump(multi_model, handle)
        models["multi_table"] = model_path
        return models

    for table_name, table_meta in metadata_dict["tables"].items():
        from sdv.metadata import SingleTableMetadata

        table_metadata = SingleTableMetadata()
        table_metadata.add_columns(table_meta["columns"])
        primary_key = table_meta.get("primary_key") or []
        if len(primary_key) == 1:
            table_metadata.set_primary_key(primary_key[0])
        synthesizer = _get_single_table_synthesizer(table_metadata)
        synthesizer.fit(tables[table_name])
        model_path = os.path.join(model_dir, f"{table_name}.pkl")
        with open(model_path, "wb") as handle:
            pickle.dump(synthesizer, handle)
        models[table_name] = model_path
    return models
