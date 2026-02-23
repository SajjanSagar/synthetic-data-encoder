# Synthetic Multi-Table Data Generator

This project generates privacy-safe synthetic data from multiple related CSV tables.
All ingestion, profiling, relationship discovery, training, generation, and validation
flow through an internal SQLite staging database.

## Why SQLite Internally
- CSVs are loaded into `artifacts/staging.db` for consistent profiling and joins.
- Relationships and validation are performed with SQL queries rather than raw CSVs.
- SQLite is a staging layer only; final outputs are still CSV files.

## How It Works
1. `ingest` loads CSVs from `data/real/` into SQLite tables.
2. `inspect` profiles tables to infer types and PK/FK candidates.
3. `setup` launches an interactive wizard to confirm PK/FK relationships.
4. `train` builds SDV metadata and trains a synthesizer.
5. `generate` creates synthetic tables and applies deterministic masking.
6. `validate` checks schema parity, FK integrity, and cardinality similarity.

## Privacy Masking
Masking is deterministic and format-preserving:
- Alphabetic chars → random alphabetic (preserve case)
- Numeric chars → random digits
- Special chars → unchanged

It uses HMAC with a secret salt (env var `SYNTHETIC_DATA_SALT`) so the same input
always maps to the same output. The same masking function is applied to parent
primary keys and child foreign keys to preserve referential integrity.

## Quick Start
From the `synthetic_multi` directory:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export SYNTHETIC_DATA_SALT="your-secret-salt"

# SDV requires network modules; use SAFE_MODE=false for local runs
export SAFE_MODE=false

python -m src.cli ingest
python -m src.cli inspect
python -m src.cli setup
python -m src.cli train
python -m src.cli generate --scale 1.0
python -m src.cli validate
python -m src.cli validate --summary
```

Or run the full pipeline (with sample data):

```bash
python -m src.cli generate-sample --accounts 1000 --exceptions 1000 --force
python -m src.cli all --non-interactive   # Auto-accepts inferred schema
```

## Generating Sample Input Data
Use the built-in generator to create realistic fake CSVs for demos, tests, and CI.
This data is synthetic and intended only for local testing.

```bash
python -m src.cli generate-sample --accounts 1000 --exceptions 1000
```

Add `--force` to overwrite existing files in `data/real/`.

## Outputs
- `artifacts/staging.db`: internal SQLite staging database
- `artifacts/metadata.json`: confirmed schema metadata
- `artifacts/relationships.yaml`: confirmed FK relationships
- `artifacts/privacy_rules.yaml`: columns to mask
- `data/synthetic/*.csv`: synthetic tables
- `reports/validation.json`: validation summary
- `reports/validation.md`: human-readable validation report

## Security & Data Locality
- The app runs fully local; no outbound network calls are required.
- `SAFE_MODE=true` (default) blocks common networking libraries at import time.
- **Note:** SDV (synthetic data library) depends on modules like `socket`; use `SAFE_MODE=false` for local runs.
- All staging data is stored locally in `artifacts/staging.db` (SQLite).
- Avoid running inside iCloud/OneDrive/Dropbox synced folders to prevent accidental leakage.

## Notes
- Composite primary keys can be confirmed in the wizard.
- Single-table fallback training is used if SDV multi-table synthesizers are unavailable.
