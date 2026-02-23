import logging
import re
from pathlib import Path
from typing import Dict, Iterable, Optional, Set

import yaml
from rich.logging import RichHandler


_LOGGER_CACHE: Dict[str, logging.Logger] = {}


def _default_privacy_rules_path() -> Path:
    return Path(__file__).resolve().parents[1] / "artifacts" / "privacy_rules.yaml"


def _load_sensitive_columns(privacy_rules_path: Optional[str]) -> Set[str]:
    if not privacy_rules_path:
        privacy_rules_path = str(_default_privacy_rules_path())
    rules_path = Path(privacy_rules_path)
    if not rules_path.exists():
        return set()
    with open(rules_path, "r", encoding="utf-8") as handle:
        rules = yaml.safe_load(handle) or {}
    sensitive = set()
    for entry in rules.get("mask_columns", []):
        for column in entry.get("columns", []):
            sensitive.add(str(column))
    return sensitive


def _build_redaction_patterns(columns: Iterable[str]) -> Iterable[re.Pattern]:
    patterns = []
    for col in columns:
        escaped = re.escape(col)
        patterns.append(re.compile(rf"({escaped}\s*=\s*)([^,\s]+)"))
    return patterns


def redact_message(message: str, patterns: Iterable[re.Pattern]) -> str:
    redacted = message
    for pattern in patterns:
        redacted = pattern.sub(r"\1<redacted>", redacted)
    return redacted


class RedactingFormatter(logging.Formatter):
    def __init__(self, patterns: Iterable[re.Pattern]):
        super().__init__("%(message)s")
        self._patterns = list(patterns)

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        return redact_message(message, self._patterns)


def get_logger(name: str, privacy_rules_path: Optional[str] = None) -> logging.Logger:
    if name in _LOGGER_CACHE:
        return _LOGGER_CACHE[name]

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    sensitive_columns = _load_sensitive_columns(privacy_rules_path)
    patterns = _build_redaction_patterns(sensitive_columns)

    handler = RichHandler(rich_tracebacks=True, show_path=False)
    handler.setFormatter(RedactingFormatter(patterns))
    logger.addHandler(handler)

    _LOGGER_CACHE[name] = logger
    return logger


def log_table_summary(logger: logging.Logger, table: str, df) -> None:
    null_ratios = df.isna().mean().round(4).to_dict()
    dtypes = {col: str(dtype) for col, dtype in df.dtypes.items()}
    logger.info(
        "Table '%s': rows=%s, columns=%s, dtypes=%s, null_ratios=%s",
        table,
        len(df),
        list(df.columns),
        dtypes,
        null_ratios,
    )
