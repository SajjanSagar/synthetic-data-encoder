import json
from typing import Dict, List

import yaml

from .logging_utils import get_logger


def _prompt(message: str, default: str = "", non_interactive: bool = False) -> str:
    if non_interactive:
        return default
    if default:
        prompt = f"{message} [{default}]: "
    else:
        prompt = f"{message}: "
    response = input(prompt).strip()
    return response if response else default


def _parse_columns(value: str) -> List[str]:
    return [col.strip() for col in value.split(",") if col.strip()]


def run_relationship_wizard(
    profile: Dict[str, object], non_interactive: bool = False
) -> Dict[str, object]:
    logger = get_logger(__name__)
    confirmed = {"tables": {}, "relationships": []}

    logger.info("Schema Confirmation Wizard")

    tables = profile.get("tables", {})
    for table_name, table_profile in tables.items():
        pk_candidates = table_profile.get("pk_candidates", [])
        default_pk = ",".join(pk_candidates[0]) if pk_candidates else ""
        response = _prompt(
            f"Primary key for table '{table_name}' (comma-separated for composite)",
            default_pk,
            non_interactive,
        )
        confirmed["tables"][table_name] = {
            "primary_key": _parse_columns(response) if response else []
        }

    inferred_relationships = profile.get("relationships", [])
    if inferred_relationships:
        logger.info("Inferred Relationships")
        for rel in inferred_relationships:
            logger.info(
                f"- {rel['child_table']}.{rel['child_key']} -> "
                f"{rel['parent_table']}.{rel['parent_key']} (match={rel['match_ratio']})"
            )
        response = _prompt(
            "Enter relationships as child_table.child_key=parent_table.parent_key "
            "(comma-separated), or press Enter to accept all",
            "",
            non_interactive,
        )
        if response:
            entries = [entry.strip() for entry in response.split(",") if entry.strip()]
            for entry in entries:
                child_part, parent_part = entry.split("=")
                child_table, child_key = child_part.strip().split(".")
                parent_table, parent_key = parent_part.strip().split(".")
                confirmed["relationships"].append(
                    {
                        "child_table": child_table,
                        "child_key": child_key,
                        "parent_table": parent_table,
                        "parent_key": parent_key,
                        "cardinality": "many-to-one",
                    }
                )
        else:
            confirmed["relationships"] = [
                {
                    "child_table": rel["child_table"],
                    "child_key": rel["child_key"],
                    "parent_table": rel["parent_table"],
                    "parent_key": rel["parent_key"],
                    "cardinality": rel.get("cardinality", "many-to-one"),
                }
                for rel in inferred_relationships
            ]

    return confirmed


def save_confirmed_schema(
    confirmed: Dict[str, object],
    metadata_path: str,
    relationships_path: str,
    privacy_rules_path: str,
    salt_env_var: str,
) -> None:
    tables_metadata = {
        table: {"primary_key": data["primary_key"]}
        for table, data in confirmed["tables"].items()
    }
    metadata = {"tables": tables_metadata, "relationships": confirmed["relationships"]}
    with open(metadata_path, "w", encoding="utf-8") as handle:
        json.dump(metadata, handle, indent=2)

    with open(relationships_path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(confirmed["relationships"], handle, sort_keys=False)

    privacy_rules = {"salt_env_var": salt_env_var, "mask_columns": []}
    seen = {}
    for table_name, table_data in confirmed["tables"].items():
        if table_data["primary_key"]:
            seen.setdefault(table_name, set()).update(table_data["primary_key"])

    for rel in confirmed["relationships"]:
        seen.setdefault(rel["child_table"], set()).add(rel["child_key"])

    for table_name, columns in seen.items():
        privacy_rules["mask_columns"].append(
            {"table": table_name, "columns": sorted(columns)}
        )

    with open(privacy_rules_path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(privacy_rules, handle, sort_keys=False)
