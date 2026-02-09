# Banking ETL Project, Interview Guide

## 30-Second Elevator Pitch

"I designed and built a Banking ETL Pipeline that processes daily transaction data through a 3-layer architecture, Raw, Staging, and Reporting. The system uses Python for data processing, Pandas for transformations, SQLite for storage, and Streamlit with Plotly for an interactive real-time dashboard. It handles automated CSV ingestion, key-based deduplication, and generates pre-aggregated KPIs for business analytics."

## Technologies Used

| Technology | Why I Chose It | How I Used It |
|------------|----------------|---------------|
| Python | Industry standard for data engineering, readable, extensive libraries | Core language for all 6 ETL scripts |
| Pandas | Fast data manipulation, handles CSV parsing, memory-efficient | Reading CSVs, data transformations, type handling |
| SQLite | Zero configuration, single-file database, perfect for portfolios | Stores 30+ tables across 3 layers (raw/stg/rpt) |
| Streamlit | Rapid dashboard development, Python-native, free cloud hosting | Interactive dashboard with filters, search, SQL query interface |
| Plotly | Interactive charts, professional visualizations, Streamlit integration | Pie charts, bar charts, KPI cards |
| Git | Version control, collaboration, deployment | GitHub repo + Streamlit Cloud deployment |

## 3-Layer Architecture

1. Raw Layer, Preserves all incoming data as an immutable audit trail
2. Staging Layer, Applies key-based deduplication using configurable primary keys
3. Report Layer, Pre-aggregates metrics so the dashboard loads instantly

## Key Scripts

| Script | Purpose |
|--------|---------|
| run_pipeline.py | Orchestrates all 3 ETL steps in sequence |
| csv_importer.py | Imports CSVs with column validation |
| raw_to_stg.py | Deduplicates raw data to staging tables |
| stg_to_rpt.py | Creates report aggregates for dashboard |
| generate_new_data.py | Creates test data using Popular_Baby_Names.csv (6,782 unique names) |
| generate_sample_data.py | Creates initial sample data |
| enrich_existing_data.py | Boosts KPIs by adding data for existing customers |

## Bug Fix Story (Good Interview Example)

"The dashboard was showing all zeros. I debugged and found a SQL bug in the LIKE pattern, 'NOT LIKE underscore percent' was filtering out ALL tables because underscore is a wildcard in SQLite. I fixed it by using 'NOT LIKE exclamation underscore percent ESCAPE exclamation' to treat underscore literally. This shows how SQL edge cases can cause silent failures."

## Dashboard KPIs Tracked

- Total Customers, Total Accounts, Total Balance
- Active Loans, Credit Cards
- Pending Transactions, Failed Transactions
- Branches, Employees, ATMs
- Investments

## Project Links

- Live Dashboard: sianda5j3sfbroenuz8pnf.streamlit.app
- GitHub: github.com/nevradonatwork/ETL_V3

## Interview Q&A

Q: Why SQLite instead of PostgreSQL?
A: "For a portfolio project, SQLite provides zero-configuration deployment and the database file can be committed to Git. In production, I would use PostgreSQL or a cloud database."

Q: How do you handle data quality?
A: "Three ways: (1) Column validation on import rejects malformed CSVs, (2) Key-based deduplication prevents duplicate records, (3) The raw layer preserves original data for auditing."

Q: What would you improve?
A: "Add incremental loading with timestamps instead of full refreshes, implement data validation rules like balance cannot be negative for savings, and add unit tests for the ETL functions."

Q: How does deduplication work?
A: "I use INSERT WHERE NOT EXISTS with key matching. The key columns are configurable per table in table_keys_config.json."

Q: Why a 3-layer architecture?
A: "Raw preserves audit history, Staging ensures clean deduplicated data, and Report pre-aggregates for fast dashboard queries. This separation makes debugging easier, I can always trace back to the original data."

## Project Numbers

- Table types supported: 22
- Unique names in test data: 6,782
- Lines of Python code: ~2,500
- Dashboard pages: 6 (Overview, Customers, Accounts, Loans, Operations, Analytics)
- KPIs tracked: 18
