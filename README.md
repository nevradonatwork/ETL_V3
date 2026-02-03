# Banking ETL System

A complete ETL (Extract, Transform, Load) pipeline with interactive dashboard for banking data analytics.

## Features

- **Automated CSV Import** - Auto-detect and import CSV files with column validation
- **3-Layer Architecture** - Raw (audit) -> Staging (clean) -> Reporting (aggregated)
- **Key-Based Deduplication** - Smart deduplication using primary keys
- **Interactive Dashboard** - Real-time KPIs, charts, and data exploration
- **SQL Query Interface** - Execute custom queries against the database

## Tech Stack

| Technology | Purpose |
|------------|---------|
| **Python** | ETL scripts, data processing |
| **Pandas** | Data manipulation |
| **SQLite** | Database storage |
| **Streamlit** | Interactive dashboard |
| **Plotly** | Data visualizations |

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Generate Sample Data

```bash
python scripts/generate_sample_data.py
```

### 3. Run ETL Pipeline

```bash
# Create database
python scripts/database_creator.py

# Import CSVs to raw tables
python scripts/csv_importer.py --sample

# Move to staging (deduplicate)
python scripts/raw_to_stg.py

# Create report tables
python scripts/stg_to_rpt.py
```

### 4. Launch Dashboard

```bash
streamlit run dashboard/app.py
```

## Architecture

```
CSV Files (Daily)
      |
      v
+-------------+
| RAW Tables  |  <- All imported data (audit trail)
| rawAccount  |
+------+------+
       |
       v
+-------------+
| STG Tables  |  <- Deduplicated by primary key
| stgAccount  |
+------+------+
       |
       v
+-------------+
| RPT Tables  |  <- Aggregated for dashboard
| rptSummary  |
+------+------+
       |
       v
+-------------+
| DASHBOARD   |  <- Streamlit visualization
+-------------+
```

## Project Structure

```
ETL_V2/
|
+-- config/
|   +-- database_config.json       # Database settings
|   +-- csv_import_config.json     # Import settings
|   +-- table_keys_config.json     # Primary keys for dedup
|
+-- data/
|   +-- incoming/                  # Drop CSVs here
|   +-- processed/                 # Archived files
|   +-- sample/                    # Sample data
|
+-- scripts/
|   +-- database_creator.py        # Create database
|   +-- csv_importer.py            # Import CSVs
|   +-- raw_to_stg.py              # Raw to staging
|   +-- stg_to_rpt.py              # Staging to reports
|   +-- generate_sample_data.py    # Generate test data
|
+-- dashboard/
|   +-- app.py                     # Streamlit dashboard
|
+-- sql/
|   +-- sample_queries.sql         # SQL examples
|
+-- database/
|   +-- banking.db                 # SQLite database
|
+-- requirements.txt
+-- README.md
```

## Database Schema

### Tables (22 types)

| Domain | Tables |
|--------|--------|
| **Customer** | CustomerProfiles, CustomerDocuments, CustomerPreferences |
| **Account** | AccountProducts, AccountStatements, AccountLimits, AccountBeneficiaries |
| **Transaction** | PendingTransactions, FailedTransactions, RecurringPayments, TransactionDisputes |
| **Products** | Loans, CreditCards, Investments, SavingsGoals |
| **Operations** | Branches, AtmLocations, Employees, AuditLog |
| **Analytics** | MonthlyBalances, CategorySpending, MerchantFrequency |

### Data Layers

| Layer | Prefix | Purpose | Duplicates |
|-------|--------|---------|------------|
| Raw | `raw` | Audit trail | Allowed |
| Staging | `stg` | Clean data | Deduplicated |
| Report | `rpt` | Aggregations | N/A |

## CSV Naming Convention

```
{table_name}_{YYYYMMDD}.csv

Examples:
- customer_profiles_20260202.csv -> rawCustomerProfiles
- loans_20260202.csv -> rawLoans
- branches_20260202.csv -> rawBranches
```

## Key Features Explained

### Auto-Detection

The CSV importer automatically:
1. Scans the incoming folder for CSV files
2. Parses filename to determine table name
3. Creates tables if they don't exist
4. Validates columns if table exists
5. Imports data and archives processed files

### Deduplication

Staging tables use primary keys for deduplication:
- `CustomerProfiles`: customer_id
- `Loans`: loan_id
- `AccountStatements`: (account_id, statement_date)

### Dashboard

Interactive Streamlit dashboard with:
- KPI metrics cards
- Pie/Bar charts for distribution
- Filterable data tables
- SQL query interface

## Skills Demonstrated

- **Python**: Data processing, OOP, file handling
- **SQL**: Queries, joins, aggregations, CTEs, window functions
- **ETL**: Data pipeline design, validation, transformation
- **Data Engineering**: Schema design, deduplication strategies
- **Data Visualization**: Interactive dashboards
- **Software Engineering**: Modular code, documentation

## Author

**Nevra Donat**

## License

This project is for portfolio/educational purposes.
