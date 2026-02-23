import csv
import random
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

from .logging_utils import get_logger


LOB_OPTIONS = [
    ("Equities", "EQ"),
    ("FixedIncome", "FI"),
    ("Derivatives", "DER"),
    ("FX", "FX"),
]

SOURCE_APPS = ["LEDGER_SYS", "STATEMENT_SYS"]
SOURCE_INSTANCES = ["INST1", "INST2", "INST3"]


def _random_date(rng: random.Random, start: date, end: date) -> str:
    delta = (end - start).days
    return (start + timedelta(days=rng.randint(0, delta))).isoformat()


def _write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _ensure_writable(path: Path, force: bool) -> None:
    if path.exists() and not force:
        raise FileExistsError(
            f"Refusing to overwrite existing file: {path}. Use --force to overwrite."
        )


def generate_accounts(rng: random.Random, count: int) -> List[Dict[str, str]]:
    accounts = []
    for idx in range(1, count + 1):
        letters = "".join(rng.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(4))
        digits = f"{rng.randint(0, 99999):05d}"
        account_number = f"{letters}-{digits}"
        sub_account_number = f"{rng.randint(1, 3):02d}"
        lob_name, lob_code = rng.choice(LOB_OPTIONS)
        accounts.append(
            {
                "account_number": account_number,
                "sub_account_number": sub_account_number,
                "source_application": rng.choice(SOURCE_APPS),
                "source_application_instance": rng.choice(SOURCE_INSTANCES),
                "line_of_business_name": lob_name,
                "line_of_business_code": lob_code,
                "client": f"CLIENT-{rng.randint(1, 99):02d}",
                "prod_parallel_flag": "Y" if rng.random() < 0.2 else "",
            }
        )
    return accounts


def generate_exceptions(rng: random.Random, count: int) -> List[Dict[str, str]]:
    start = date(2023, 1, 1)
    end = date(2024, 12, 31)
    exceptions = []
    for idx in range(1, count + 1):
        exceptions.append(
            {
                "exception_id": f"EXC-{idx:04d}",
                "exception_type": rng.choice(["BREAK", "WARNING"]),
                "exception_status": rng.choice(["OPEN", "CLOSED"]),
                "created_date": _random_date(rng, start, end),
            }
        )
    return exceptions


def generate_items(
    rng: random.Random,
    exceptions: List[Dict[str, str]],
    accounts: List[Dict[str, str]],
) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    item_index = 1
    for exc in exceptions:
        items_per_exception = rng.randint(2, 6)
        for _ in range(items_per_exception):
            account = rng.choice(accounts)
            item_type = rng.choice(["Ledger", "Statement", ""])
            items.append(
                {
                    "item_id": f"ITM-{item_index:04d}",
                    "exception_id": exc["exception_id"],
                    "account_number": account["account_number"],
                    "sub_account_number": account["sub_account_number"],
                    "source_application": account["source_application"],
                    "source_application_instance": account["source_application_instance"],
                    "item_type": item_type,
                    "reason_code": f"R{rng.randint(1, 15):02d}",
                    "executive_reason_code": (
                        f"ER{rng.randint(1, 10):02d}" if rng.random() < 0.6 else ""
                    ),
                }
            )
            item_index += 1
    return items


def generate_matched_items(
    rng: random.Random, items: List[Dict[str, str]]
) -> List[Dict[str, str]]:
    start = date(2023, 1, 1)
    end = date(2024, 12, 31)
    item_ids = [item["item_id"] for item in items]
    rng.shuffle(item_ids)

    groups: List[List[str]] = []
    idx = 0
    while idx < len(item_ids):
        remaining = len(item_ids) - idx
        if remaining == 1 and groups:
            groups[-1].append(item_ids[idx])
            break
        group_size = min(rng.randint(2, 4), remaining)
        groups.append(item_ids[idx : idx + group_size])
        idx += group_size

    matched_items: List[Dict[str, str]] = []
    for match_index, group in enumerate(groups, start=1):
        match_id = f"MATCH-{match_index:04d}"
        for item_id in group:
            matched_items.append(
                {
                    "match_id": match_id,
                    "item_id": item_id,
                    "match_type": rng.choice(["AUTO", "MANUAL"]),
                    "matched_date": _random_date(rng, start, end),
                }
            )
    return matched_items


def generate_sample_csvs(
    output_dir: str,
    accounts_count: int = 1000,
    exceptions_count: int = 1000,
    seed: int = 42,
    force: bool = False,
) -> Dict[str, int]:
    logger = get_logger(__name__)
    rng = random.Random(seed)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    accounts_path = output_path / "accounts.csv"
    exceptions_path = output_path / "exceptions.csv"
    items_path = output_path / "items.csv"
    matched_items_path = output_path / "matched_items.csv"

    for path in [accounts_path, exceptions_path, items_path, matched_items_path]:
        _ensure_writable(path, force)

    accounts = generate_accounts(rng, accounts_count)
    exceptions = generate_exceptions(rng, exceptions_count)
    items = generate_items(rng, exceptions, accounts)
    matched_items = generate_matched_items(rng, items)

    _write_csv(
        accounts_path,
        [
            "account_number",
            "sub_account_number",
            "source_application",
            "source_application_instance",
            "line_of_business_name",
            "line_of_business_code",
            "client",
            "prod_parallel_flag",
        ],
        accounts,
    )
    _write_csv(
        exceptions_path,
        ["exception_id", "exception_type", "exception_status", "created_date"],
        exceptions,
    )
    _write_csv(
        items_path,
        [
            "item_id",
            "exception_id",
            "account_number",
            "sub_account_number",
            "source_application",
            "source_application_instance",
            "item_type",
            "reason_code",
            "executive_reason_code",
        ],
        items,
    )
    _write_csv(
        matched_items_path,
        ["match_id", "item_id", "match_type", "matched_date"],
        matched_items,
    )

    counts = {
        "accounts": len(accounts),
        "exceptions": len(exceptions),
        "items": len(items),
        "matched_items": len(matched_items),
    }
    logger.info("Sample CSVs written to %s", output_dir)
    for name, count in counts.items():
        logger.info("sample.%s rows=%s", name, count)
    return counts
