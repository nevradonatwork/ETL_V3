python scripts/database_creator.py
python scripts/generate_sample_data.py

python scripts/run_pipeline.py --import raw CSV data, transform raw to staging, create report tables


python -m streamlit run dashboard/app.py — view the dashboard


python scripts/csv_importer.py — import raw CSV data
python scripts/raw_to_stg.py — transform raw to staging
python scripts/stg_to_rpt.py — create report tables




https://sianda5j3sfbroenuz8pnf.streamlit.app/

ETL_V2/
│
├── config/
│   ├── database_config.json       # Database connection settings
│   ├── csv_import_config.json     # CSV import settings
│   └── table_keys_config.json     # Primary keys for deduplication
│
├── data/
│   ├── incoming/                  # Drop daily CSV files here
│   └── processed/                 # Archived files after import
│       └── YYYY-MM-DD/            # Organized by date
│
├── scripts/
│   ├── database_creator.py        # Step 0: Create database
│   ├── csv_importer.py            # Step 1: CSV → Raw tables
│   └── raw_to_stg.py              # Step 2: Raw → Staging tables
│
├── logs/
│   └── etl_YYYYMMDD.log           # Daily log files
│
├── database/
│   └── banking.db                 # SQLite database file
│
└── docs/
    └── ETL_SYSTEM_DOCUMENTATION.md  # This file

