import hmac
import os
import random
import string
from typing import Dict, List

from .logging_utils import get_logger

def _deterministic_random(value: str, salt: str) -> random.Random:
    key = salt.encode("utf-8")
    msg = value.encode("utf-8")
    digest = hmac.new(key, msg, "sha256").digest()
    seed = int.from_bytes(digest[:8], "big")
    return random.Random(seed)


def mask_value(value: object, salt: str) -> object:
    if value is None:
        return None
    text = str(value)
    rng = _deterministic_random(text, salt)
    masked = []
    for char in text:
        if char.isalpha():
            if char.isupper():
                masked.append(rng.choice(string.ascii_uppercase))
            else:
                masked.append(rng.choice(string.ascii_lowercase))
        elif char.isdigit():
            masked.append(rng.choice(string.digits))
        else:
            masked.append(char)
    return "".join(masked)


def apply_masking(
    tables: Dict[str, "pd.DataFrame"],
    privacy_rules: Dict[str, object],
    salt_env_var: str,
) -> Dict[str, "pd.DataFrame"]:
    import pandas as pd  # local import to avoid hard dependency at module import
    logger = get_logger(__name__)

    salt = os.getenv(salt_env_var, "")
    if not salt:
        raise ValueError(
            f"Missing salt environment variable: {salt_env_var}. "
            "Set it to ensure deterministic masking."
        )

    rules = privacy_rules.get("mask_columns", [])
    for rule in rules:
        table = rule["table"]
        columns = rule["columns"]
        if table not in tables:
            continue
        for col in columns:
            if col not in tables[table].columns:
                continue
            tables[table][col] = tables[table][col].apply(
                lambda value: mask_value(value, salt)
            )
        logger.info("Applied masking to %s columns on table '%s'.", columns, table)
    return tables
