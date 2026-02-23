import sqlite3
from pathlib import Path
from typing import Dict, List

import pandas as pd

from .logging_utils import get_logger, log_table_summary


def _infer_sqlite_type(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "INTEGER"
    if pd.api.types.is_integer_dtype(series):
        return "INTEGER"
    if pd.api.types.is_float_dtype(series):
        return "REAL"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TEXT"
    return "TEXT"


def _read_csv(csv_path: Path, empty_string_as_null: bool) -> pd.DataFrame:
    df = pd.read_csv(csv_path, dtype=None, keep_default_na=False)
    if empty_string_as_null:
        df = df.replace("", None)
    return df


def load_csvs_to_sqlite(
    data_dir: str,
    db_path: str,
    empty_string_as_null: bool = True,
) -> Dict[str, int]:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    logger = get_logger(__name__)
    row_counts: Dict[str, int] = {}
    try:
        for csv_file in sorted(Path(data_dir).glob("*.csv")):
            table_name = csv_file.stem
            df = _read_csv(csv_file, empty_string_as_null)
            log_table_summary(logger, table_name, df)
            dtype_map = {col: _infer_sqlite_type(df[col]) for col in df.columns}
            df.to_sql(
                table_name,
                conn,
                if_exists="replace",
                index=False,
                dtype=dtype_map,
            )
            row_counts[table_name] = len(df)
            logger.info("[ingest] %s: %s rows", table_name, row_counts[table_name])
    finally:
        conn.close()
    return row_counts


def list_csv_files(data_dir: str) -> List[Path]:
    return sorted(Path(data_dir).glob("*.csv"))
