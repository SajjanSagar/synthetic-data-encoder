import argparse
import importlib.util
import json
from pathlib import Path
from typing import Optional

import yaml

from .logging_utils import get_logger
from .safety import enforce_local_only


def _load_config(config_path: Path) -> dict:
    with open(config_path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _resolve_path(base_dir: Path, relative_path: str) -> str:
    return str((base_dir / relative_path).resolve())


def ingest(config: dict, base_dir: Path) -> None:
    from .csv_to_sqlite import load_csvs_to_sqlite

    data_real_dir = _resolve_path(base_dir, config["project"]["data_real_dir"])
    staging_db = _resolve_path(base_dir, config["project"]["staging_db"])
    empty_as_null = config["ingest"]["empty_string_as_null"]
    load_csvs_to_sqlite(data_real_dir, staging_db, empty_as_null)


def inspect(config: dict, base_dir: Path) -> dict:
    from .profiling import profile_database

    staging_db = _resolve_path(base_dir, config["project"]["staging_db"])
    profile = profile_database(
        staging_db,
        config["profiling"]["pk_min_uniqueness"],
        config["profiling"]["fk_match_threshold"],
        config["profiling"]["max_composite_key_size"],
    )
    artifacts_dir = _resolve_path(base_dir, config["project"]["artifacts_dir"])
    Path(artifacts_dir).mkdir(parents=True, exist_ok=True)
    inferred_metadata_path = Path(artifacts_dir) / "metadata_inferred.json"
    inferred_relationships_path = Path(artifacts_dir) / "relationships_inferred.yaml"
    with open(inferred_metadata_path, "w", encoding="utf-8") as handle:
        json.dump(profile, handle, indent=2)
    with open(inferred_relationships_path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(profile.get("relationships", []), handle, sort_keys=False)
    return profile


def setup(config: dict, base_dir: Path) -> None:
    from .metadata_builder import build_metadata
    from .relationship_wizard import run_relationship_wizard, save_confirmed_schema

    staging_db = _resolve_path(base_dir, config["project"]["staging_db"])
    profile = inspect(config, base_dir)
    confirmed = run_relationship_wizard(profile)

    metadata_path = _resolve_path(base_dir, config["project"]["metadata_json"])
    relationships_path = _resolve_path(base_dir, config["project"]["relationships_yaml"])
    privacy_rules_path = _resolve_path(base_dir, config["project"]["privacy_rules_yaml"])
    save_confirmed_schema(
        confirmed,
        metadata_path,
        relationships_path,
        privacy_rules_path,
        config["privacy"]["salt_env_var"],
    )

    build_metadata(staging_db, confirmed, metadata_path)


def train(config: dict, base_dir: Path) -> None:
    from .synthesizer_train import train_synthesizer

    staging_db = _resolve_path(base_dir, config["project"]["staging_db"])
    metadata_path = _resolve_path(base_dir, config["project"]["metadata_json"])
    model_dir = _resolve_path(base_dir, config["project"]["model_dir"])
    train_synthesizer(staging_db, metadata_path, model_dir)


def generate(config: dict, base_dir: Path, scale_override: Optional[float]) -> None:
    from .synthesizer_generate import generate_synthetic_data

    staging_db = _resolve_path(base_dir, config["project"]["staging_db"])
    metadata_path = _resolve_path(base_dir, config["project"]["metadata_json"])
    privacy_rules_path = _resolve_path(base_dir, config["project"]["privacy_rules_yaml"])
    model_dir = _resolve_path(base_dir, config["project"]["model_dir"])
    output_dir = _resolve_path(base_dir, config["project"]["data_synthetic_dir"])
    scale = scale_override if scale_override is not None else config["generation"]["scale"]
    generate_synthetic_data(
        staging_db,
        metadata_path,
        privacy_rules_path,
        model_dir,
        output_dir,
        scale,
        config["privacy"]["salt_env_var"],
        config["generation"]["random_seed"],
    )


def validate(config: dict, base_dir: Path) -> dict:
    from .validator import validate_synthetic

    staging_db = _resolve_path(base_dir, config["project"]["staging_db"])
    synthetic_dir = _resolve_path(base_dir, config["project"]["data_synthetic_dir"])
    synthetic_db = _resolve_path(base_dir, config["project"]["synthetic_db"])
    relationships_path = _resolve_path(base_dir, config["project"]["relationships_yaml"])
    reports_dir = _resolve_path(base_dir, config["project"]["reports_dir"])
    return validate_synthetic(
        staging_db, synthetic_dir, synthetic_db, relationships_path, reports_dir
    )


def _run_security_check(base_dir: Path) -> None:
    script_path = base_dir / "scripts" / "security_check.py"
    if not script_path.exists():
        raise FileNotFoundError(f"Missing security check script: {script_path}")
    spec = importlib.util.spec_from_file_location("security_check", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    result = module.run_security_check(base_dir)
    if not result:
        raise SystemExit(1)


def main() -> None:
    enforce_local_only()
    base_dir = Path(__file__).resolve().parents[1]
    config_path = base_dir / "config.yaml"
    config = _load_config(config_path)
    logger = get_logger(__name__)

    parser = argparse.ArgumentParser(description="Synthetic multi-table data generator")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("ingest", help="Load CSVs into SQLite staging DB")
    subparsers.add_parser("inspect", help="Profile tables and infer relationships")
    subparsers.add_parser("setup", help="Confirm schema and relationships")
    subparsers.add_parser("train", help="Train synthetic data generator")

    generate_parser = subparsers.add_parser("generate", help="Generate synthetic data")
    generate_parser.add_argument("--scale", type=float, default=None, help="Scale row counts")

    validate_parser = subparsers.add_parser(
        "validate", help="Validate synthetic data integrity"
    )
    validate_parser.add_argument(
        "--summary", action="store_true", help="Print validation summary"
    )
    subparsers.add_parser("all", help="Run full pipeline")
    sample_parser = subparsers.add_parser(
        "generate-sample", help="Generate sample CSV input data"
    )
    sample_parser.add_argument("--accounts", type=int, default=1000)
    sample_parser.add_argument("--exceptions", type=int, default=1000)
    sample_parser.add_argument("--seed", type=int, default=42)
    sample_parser.add_argument("--force", action="store_true")

    args = parser.parse_args()
    _run_security_check(base_dir)

    if args.command == "ingest":
        logger.info("Running ingest")
        ingest(config, base_dir)
    elif args.command == "inspect":
        logger.info("Running inspect")
        inspect(config, base_dir)
    elif args.command == "setup":
        logger.info("Running setup")
        setup(config, base_dir)
    elif args.command == "train":
        logger.info("Running train")
        train(config, base_dir)
    elif args.command == "generate":
        logger.info("Running generate")
        generate(config, base_dir, args.scale)
    elif args.command == "validate":
        logger.info("Running validate")
        report = validate(config, base_dir)
        if args.summary:
            from .validator import summarize_report

            summary = summarize_report(report)
            logger.info("Schema parity ok: %s", summary["schema_parity_ok"])
            logger.info("FK orphan rows: %s", summary["fk_orphan_rows"])
            for item in summary["cardinality"]:
                logger.info(
                    "Cardinality %s: real_mean=%.4f synthetic_mean=%.4f "
                    "real_median=%.4f synthetic_median=%.4f",
                    item["relationship"],
                    item["real_mean"],
                    item["synthetic_mean"],
                    item["real_median"],
                    item["synthetic_median"],
                )
    elif args.command == "generate-sample":
        from .sample_data_generator import generate_sample_csvs

        logger.info("Running sample data generator")
        data_real_dir = _resolve_path(base_dir, config["project"]["data_real_dir"])
        generate_sample_csvs(
            data_real_dir,
            accounts_count=args.accounts,
            exceptions_count=args.exceptions,
            seed=args.seed,
            force=args.force,
        )
    elif args.command == "all":
        logger.info("Running full pipeline")
        ingest(config, base_dir)
        inspect(config, base_dir)
        setup(config, base_dir)
        train(config, base_dir)
        generate(config, base_dir, None)
        validate(config, base_dir)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
