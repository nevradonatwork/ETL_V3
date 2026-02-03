# ETL System Documentation
## Banking Data Pipeline

**Version:** 1.0
**Created:** 2026-02-02
**Author:** Nevra Donat

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture](#2-architecture)
3. [File Structure](#3-file-structure)
4. [Database Schema](#4-database-schema)
5. [Configuration Files](#5-configuration-files)
6. [Scripts Reference](#6-scripts-reference)
7. [Daily Operations](#7-daily-operations)
8. [CSV File Specifications](#8-csv-file-specifications)
9. [Error Handling](#9-error-handling)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. System Overview

### Purpose

This ETL (Extract, Transform, Load) system processes daily banking CSV files into a SQLite database. The system maintains two layers of data:

- **Raw Layer**: Contains all imported data exactly as received (audit trail)
- **Staging Layer**: Contains deduplicated, clean data ready for analysis

### Key Features

- Auto-detection of CSV files from incoming folder
- Automatic table creation based on CSV structure
- Column validation for existing tables
- Key-based deduplication for staging tables
- Full audit trail in raw tables

### Data Flow

```
CSV Files (Daily)
      │
      ▼
┌─────────────────┐
│  csv_importer   │  ← Validates columns, creates tables if needed
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Raw Tables    │  ← All data, including duplicates
│ (rawTableName)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  raw_to_stg     │  ← Deduplicates by primary key
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Staging Tables  │  ← Clean, unique records only
│ (stgTableName)  │
└─────────────────┘
```

---

## 2. Architecture

### System Components

| Component | Description | Run Frequency |
|-----------|-------------|---------------|
| database_creator.py | Creates SQLite database | Once (initial setup) |
| csv_importer.py | Imports CSVs to raw tables | Daily |
| raw_to_stg.py | Moves data to staging tables | Daily |

### Data Layers

| Layer | Prefix | Purpose | Duplicates |
|-------|--------|---------|------------|
| Raw | `raw` | Audit trail, all imported data | Allowed |
| Staging | `stg` | Clean data for analysis | Not allowed (deduplicated by key) |

### Technology Stack

- **Language:** Python 3.x
- **Database:** SQLite
- **Libraries:** pandas, sqlite3, json

---

## 3. File Structure

```
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
```

---

## 4. Database Schema

### 4.1 Entity Relationship Overview

```
CUSTOMER ──────┬────── DOCUMENTS
               ├────── PREFERENCES
               └────── ACCOUNTS ──────┬────── STATEMENTS
                                      ├────── LIMITS
                                      ├────── BENEFICIARIES
                                      ├────── TRANSACTIONS (pending/failed)
                                      ├────── RECURRING PAYMENTS
                                      ├────── LOANS
                                      ├────── CREDIT CARDS
                                      ├────── INVESTMENTS
                                      ├────── SAVINGS GOALS
                                      └────── ANALYTICS (balances/spending)

BRANCHES ──────┬────── EMPLOYEES
               └────── ATM LOCATIONS

AUDIT LOG (standalone)
```

### 4.2 Table Definitions

#### Customer Domain

| Table | Primary Key | Foreign Keys | Description |
|-------|-------------|--------------|-------------|
| customer_profiles | customer_id | - | Customer demographics and contact info |
| customer_documents | document_id | customer_id | ID verification documents (metadata) |
| customer_preferences | preference_id | customer_id | Communication and accessibility preferences |

**customer_profiles columns:**
```
customer_id       (TEXT)    - Unique customer identifier
name              (TEXT)    - Full name
email             (TEXT)    - Email address
phone             (TEXT)    - Phone number
address           (TEXT)    - Physical address
segment           (TEXT)    - Customer segment (retail/premium/private)
risk_rating       (TEXT)    - Risk classification (low/medium/high)
created_date      (TEXT)    - Account creation date
status            (TEXT)    - Active/Inactive/Closed
```

**customer_documents columns:**
```
document_id       (TEXT)    - Unique document identifier
customer_id       (TEXT)    - FK to customer_profiles
document_type     (TEXT)    - Type (passport/license/utility_bill)
file_path         (TEXT)    - Storage location (metadata only)
upload_date       (TEXT)    - When document was uploaded
verified          (INTEGER) - 1=verified, 0=pending
expiry_date       (TEXT)    - Document expiration date
```

**customer_preferences columns:**
```
preference_id     (TEXT)    - Unique preference record ID
customer_id       (TEXT)    - FK to customer_profiles
language          (TEXT)    - Preferred language
comm_channel      (TEXT)    - email/sms/phone/mail
marketing_opt_in  (INTEGER) - 1=opted in, 0=opted out
accessibility     (TEXT)    - Special accessibility needs
```

#### Account Domain

| Table | Primary Key | Foreign Keys | Description |
|-------|-------------|--------------|-------------|
| account_products | account_id | customer_id, branch_id | All account types |
| account_statements | statement_id | account_id | Monthly statement summaries |
| account_limits | limit_id | account_id | Transaction limits |
| account_beneficiaries | beneficiary_id | account_id | Linked transfer recipients |

**account_products columns:**
```
account_id        (TEXT)    - Unique account identifier
customer_id       (TEXT)    - FK to customer_profiles
branch_id         (TEXT)    - FK to branches
account_type      (TEXT)    - checking/savings/credit/loan
account_number    (TEXT)    - Masked account number
balance           (REAL)    - Current balance
currency          (TEXT)    - Currency code (USD/EUR/GBP)
opened_date       (TEXT)    - Account opening date
status            (TEXT)    - active/dormant/closed
```

**account_statements columns:**
```
statement_id      (TEXT)    - Unique statement identifier
account_id        (TEXT)    - FK to account_products
statement_date    (TEXT)    - Statement period end date
opening_balance   (REAL)    - Balance at period start
closing_balance   (REAL)    - Balance at period end
total_credits     (REAL)    - Sum of deposits
total_debits      (REAL)    - Sum of withdrawals
transaction_count (INTEGER) - Number of transactions
```

**account_limits columns:**
```
limit_id          (TEXT)    - Unique limit record ID
account_id        (TEXT)    - FK to account_products
daily_withdrawal  (REAL)    - Max daily ATM withdrawal
daily_transfer    (REAL)    - Max daily transfer amount
overdraft_limit   (REAL)    - Overdraft allowance
effective_date    (TEXT)    - When limits became effective
```

**account_beneficiaries columns:**
```
beneficiary_id    (TEXT)    - Unique beneficiary ID
account_id        (TEXT)    - FK to account_products (source)
beneficiary_name  (TEXT)    - Recipient name
beneficiary_acc   (TEXT)    - Recipient account number
bank_code         (TEXT)    - Recipient bank code/SWIFT
nickname          (TEXT)    - User-defined nickname
added_date        (TEXT)    - When beneficiary was added
status            (TEXT)    - active/deleted
```

#### Transaction Domain

| Table | Primary Key | Foreign Keys | Description |
|-------|-------------|--------------|-------------|
| pending_transactions | transaction_id | account_id | Awaiting clearance |
| failed_transactions | transaction_id | account_id | Declined with error codes |
| recurring_payments | payment_id | account_id | Standing orders, direct debits |
| transaction_disputes | dispute_id | transaction_id, account_id | Chargebacks |

**pending_transactions columns:**
```
transaction_id    (TEXT)    - Unique transaction ID
account_id        (TEXT)    - FK to account_products
amount            (REAL)    - Transaction amount
currency          (TEXT)    - Currency code
transaction_type  (TEXT)    - debit/credit
description       (TEXT)    - Transaction description
status            (TEXT)    - pending/processing/cleared
created_date      (TEXT)    - When transaction initiated
expected_clear    (TEXT)    - Expected clearance date
```

**failed_transactions columns:**
```
transaction_id    (TEXT)    - Unique transaction ID
account_id        (TEXT)    - FK to account_products
amount            (REAL)    - Attempted amount
currency          (TEXT)    - Currency code
transaction_type  (TEXT)    - debit/credit
error_code        (TEXT)    - System error code
error_message     (TEXT)    - Human-readable error
attempted_date    (TEXT)    - When transaction was attempted
merchant          (TEXT)    - Merchant name (if applicable)
```

**recurring_payments columns:**
```
payment_id        (TEXT)    - Unique payment schedule ID
account_id        (TEXT)    - FK to account_products
amount            (REAL)    - Payment amount
currency          (TEXT)    - Currency code
frequency         (TEXT)    - daily/weekly/monthly/yearly
next_execution    (TEXT)    - Next scheduled date
beneficiary_id    (TEXT)    - FK to account_beneficiaries
description       (TEXT)    - Payment description
status            (TEXT)    - active/paused/cancelled
created_date      (TEXT)    - When schedule was created
```

**transaction_disputes columns:**
```
dispute_id        (TEXT)    - Unique dispute ID
transaction_id    (TEXT)    - FK to original transaction
account_id        (TEXT)    - FK to account_products
reason            (TEXT)    - Dispute reason category
description       (TEXT)    - Customer description
status            (TEXT)    - open/investigating/resolved/rejected
filed_date        (TEXT)    - When dispute was filed
resolution_date   (TEXT)    - When dispute was resolved
refund_amount     (REAL)    - Amount refunded (if any)
```

#### Financial Products Domain

| Table | Primary Key | Foreign Keys | Description |
|-------|-------------|--------------|-------------|
| loans | loan_id | account_id | Loan details and schedules |
| credit_cards | card_id | account_id | Credit card information |
| investments | investment_id | account_id | Investment holdings |
| savings_goals | goal_id | account_id | Savings targets |

**loans columns:**
```
loan_id           (TEXT)    - Unique loan identifier
account_id        (TEXT)    - FK to account_products
loan_type         (TEXT)    - personal/mortgage/auto/business
principal         (REAL)    - Original loan amount
interest_rate     (REAL)    - Annual interest rate (%)
term_months       (INTEGER) - Loan term in months
monthly_payment   (REAL)    - Monthly payment amount
outstanding       (REAL)    - Remaining balance
start_date        (TEXT)    - Loan start date
end_date          (TEXT)    - Expected payoff date
status            (TEXT)    - active/paid_off/defaulted
```

**credit_cards columns:**
```
card_id           (TEXT)    - Unique card identifier
account_id        (TEXT)    - FK to account_products
card_number       (TEXT)    - Masked card number (last 4 digits)
card_type         (TEXT)    - visa/mastercard/amex
credit_limit      (REAL)    - Maximum credit limit
available_credit  (REAL)    - Current available credit
apr               (REAL)    - Annual percentage rate
expiry_date       (TEXT)    - Card expiration date
status            (TEXT)    - active/blocked/expired
```

**investments columns:**
```
investment_id     (TEXT)    - Unique investment ID
account_id        (TEXT)    - FK to account_products
asset_type        (TEXT)    - stock/bond/mutual_fund/etf
symbol            (TEXT)    - Ticker symbol
quantity          (REAL)    - Number of units held
purchase_price    (REAL)    - Average purchase price
current_price     (REAL)    - Current market price
current_value     (REAL)    - Total current value
purchase_date     (TEXT)    - Initial purchase date
```

**savings_goals columns:**
```
goal_id           (TEXT)    - Unique goal identifier
account_id        (TEXT)    - FK to account_products
goal_name         (TEXT)    - User-defined goal name
target_amount     (REAL)    - Target savings amount
current_amount    (REAL)    - Current progress
target_date       (TEXT)    - Goal target date
status            (TEXT)    - active/achieved/cancelled
created_date      (TEXT)    - When goal was created
```

#### Operational Domain

| Table | Primary Key | Foreign Keys | Description |
|-------|-------------|--------------|-------------|
| branches | branch_id | - | Bank branch locations |
| atm_locations | atm_id | branch_id | ATM machines |
| employees | employee_id | branch_id | Bank staff |
| audit_log | log_id | employee_id, customer_id | System audit trail |

**branches columns:**
```
branch_id         (TEXT)    - Unique branch identifier
branch_name       (TEXT)    - Branch name
address           (TEXT)    - Street address
city              (TEXT)    - City
state             (TEXT)    - State/Province
postal_code       (TEXT)    - ZIP/Postal code
phone             (TEXT)    - Branch phone number
hours             (TEXT)    - Operating hours
services          (TEXT)    - Available services (comma-separated)
manager_id        (TEXT)    - FK to employees
```

**atm_locations columns:**
```
atm_id            (TEXT)    - Unique ATM identifier
branch_id         (TEXT)    - FK to branches (nullable for standalone)
address           (TEXT)    - ATM location address
city              (TEXT)    - City
latitude          (REAL)    - GPS latitude
longitude         (REAL)    - GPS longitude
available_24h     (INTEGER) - 1=24/7, 0=limited hours
withdrawal_fee    (REAL)    - Fee for non-customers
deposit_enabled   (INTEGER) - 1=accepts deposits
status            (TEXT)    - operational/maintenance/offline
```

**employees columns:**
```
employee_id       (TEXT)    - Unique employee identifier
branch_id         (TEXT)    - FK to branches
name              (TEXT)    - Full name
role              (TEXT)    - Job title/role
email             (TEXT)    - Work email
phone             (TEXT)    - Work phone
hire_date         (TEXT)    - Employment start date
status            (TEXT)    - active/on_leave/terminated
```

**audit_log columns:**
```
log_id            (TEXT)    - Unique log entry ID
employee_id       (TEXT)    - FK to employees (who made change)
customer_id       (TEXT)    - FK to customer_profiles (affected)
action_type       (TEXT)    - create/update/delete/view
table_affected    (TEXT)    - Which table was changed
record_id         (TEXT)    - ID of affected record
old_value         (TEXT)    - Previous value (JSON)
new_value         (TEXT)    - New value (JSON)
timestamp         (TEXT)    - When action occurred
ip_address        (TEXT)    - Source IP address
```

#### Analytics Domain

| Table | Primary Key | Foreign Keys | Description |
|-------|-------------|--------------|-------------|
| monthly_balances | (account_id, month) | account_id | Balance snapshots |
| category_spending | (account_id, category, month) | account_id | Spending by category |
| merchant_frequency | (account_id, merchant_name) | account_id | Merchant visit stats |

**monthly_balances columns:**
```
account_id        (TEXT)    - FK to account_products
month             (TEXT)    - Month (YYYY-MM format)
avg_balance       (REAL)    - Average daily balance
min_balance       (REAL)    - Minimum balance in month
max_balance       (REAL)    - Maximum balance in month
end_balance       (REAL)    - Month-end balance
```

**category_spending columns:**
```
account_id        (TEXT)    - FK to account_products
category          (TEXT)    - Spending category
month             (TEXT)    - Month (YYYY-MM format)
total_amount      (REAL)    - Total spent in category
transaction_count (INTEGER) - Number of transactions
avg_transaction   (REAL)    - Average transaction size
```

**merchant_frequency columns:**
```
account_id        (TEXT)    - FK to account_products
merchant_name     (TEXT)    - Merchant name
visit_count       (INTEGER) - Total number of visits
total_spent       (REAL)    - Total amount spent
avg_transaction   (REAL)    - Average transaction
first_visit       (TEXT)    - First transaction date
last_visit        (TEXT)    - Most recent transaction
```

---

## 5. Configuration Files

### 5.1 database_config.json

Controls database connection settings.

```json
{
  "database_name": "database/banking.db",
  "database_type": "sqlite",
  "description": "Banking ETL Database"
}
```

| Setting | Description | Default |
|---------|-------------|---------|
| database_name | Path to SQLite database file | banking.db |
| database_type | Database type (currently only sqlite) | sqlite |
| description | Human-readable description | - |

### 5.2 csv_import_config.json

Controls CSV import behavior.

```json
{
  "csv_folder": "data/incoming/",
  "processed_folder": "data/processed/",
  "filename_pattern": "{base_name}_{date}.csv",
  "date_format": "YYYYMMDD",
  "import_settings": {
    "encoding_attempts": ["utf-8", "latin-1", "cp1252"],
    "skip_empty_rows": true,
    "trim_whitespace": true
  }
}
```

| Setting | Description | Default |
|---------|-------------|---------|
| csv_folder | Folder to scan for incoming CSVs | data/incoming/ |
| processed_folder | Where to archive processed files | data/processed/ |
| filename_pattern | Expected filename format | {base_name}_{date}.csv |
| date_format | Date format in filename | YYYYMMDD |

### 5.3 table_keys_config.json

Defines primary keys for staging table deduplication.

```json
{
  "CustomerProfiles": ["customer_id"],
  "CustomerDocuments": ["document_id"],
  "CustomerPreferences": ["preference_id"],
  "AccountProducts": ["account_id"],
  "AccountStatements": ["account_id", "statement_date"],
  "AccountLimits": ["account_id", "effective_date"],
  "AccountBeneficiaries": ["beneficiary_id"],
  "PendingTransactions": ["transaction_id"],
  "FailedTransactions": ["transaction_id"],
  "RecurringPayments": ["payment_id"],
  "TransactionDisputes": ["dispute_id"],
  "Loans": ["loan_id"],
  "CreditCards": ["card_id"],
  "Investments": ["investment_id"],
  "SavingsGoals": ["goal_id"],
  "Branches": ["branch_id"],
  "AtmLocations": ["atm_id"],
  "Employees": ["employee_id"],
  "AuditLog": ["log_id"],
  "MonthlyBalances": ["account_id", "month"],
  "CategorySpending": ["account_id", "category", "month"],
  "MerchantFrequency": ["account_id", "merchant_name"]
}
```

**Note:** Tables with composite keys (multiple columns) use all listed columns together to determine uniqueness.

---

## 6. Scripts Reference

### 6.1 database_creator.py

**Purpose:** Create the SQLite database file.

**Run:** Once during initial setup.

**Usage:**
```bash
python scripts/database_creator.py
```

**What it does:**
1. Reads database_config.json
2. Creates database file if it doesn't exist
3. Creates metadata table for tracking

**Output:**
```
================================================================================
DATABASE CREATOR
================================================================================

Creating database 'database/banking.db'...
Database created successfully!

================================================================================
```

### 6.2 csv_importer.py

**Purpose:** Import CSV files into raw tables.

**Run:** Daily (or whenever new CSV files arrive).

**Usage:**
```bash
python scripts/csv_importer.py
```

**What it does:**
1. Scans csv_folder for CSV files
2. For each CSV file:
   - Parses filename to extract table name (customer_profiles_20260202.csv → CustomerProfiles)
   - Checks if rawCustomerProfiles table exists
   - If table exists: validates columns match
   - If table doesn't exist: creates table based on CSV structure
   - Inserts all rows into raw table
3. Moves processed files to processed_folder

**Output:**
```
================================================================================
CSV IMPORTER
================================================================================

Scanning data/incoming/...
Found 5 CSV files

[1/5] customer_profiles_20260202.csv
      Table: rawCustomerProfiles
      Status: Table exists, validating columns...
      Columns: 9 columns match
      Inserted: 1,234 rows
      Total in table: 5,678 rows

[2/5] loans_20260202.csv
      Table: rawLoans
      Status: Table does not exist, creating...
      Created: rawLoans with 11 columns
      Inserted: 567 rows

================================================================================
IMPORT COMPLETE
================================================================================
Files processed: 5
Rows imported: 8,901
Errors: 0
================================================================================
```

### 6.3 raw_to_stg.py

**Purpose:** Move data from raw tables to staging tables with deduplication.

**Run:** Daily (after csv_importer.py).

**Usage:**
```bash
python scripts/raw_to_stg.py
```

**What it does:**
1. Finds all tables starting with 'raw'
2. For each raw table:
   - Determines corresponding stg table name (rawCustomerProfiles → stgCustomerProfiles)
   - Creates stg table if it doesn't exist
   - Looks up primary key(s) from table_keys_config.json
   - Inserts only rows where key doesn't already exist in stg table

**Output:**
```
================================================================================
RAW TO STAGING PROCESSOR
================================================================================

Found 22 raw tables

[1/22] rawCustomerProfiles → stgCustomerProfiles
       Key column(s): customer_id
       Rows in raw: 5,678
       Already in stg: 4,444
       New rows added: 1,234
       Total in stg: 5,678

[2/22] rawLoans → stgLoans
       Key column(s): loan_id
       Rows in raw: 567
       Already in stg: 0
       New rows added: 567
       Total in stg: 567

================================================================================
STAGING COMPLETE
================================================================================
Tables processed: 22
New rows added: 8,901
Duplicates skipped: 12,345
================================================================================
```

---

## 7. Daily Operations

### 7.1 Standard Daily Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│                     DAILY ETL WORKFLOW                          │
└─────────────────────────────────────────────────────────────────┘

  08:00 AM   CSV files arrive in data/incoming/
             - customer_profiles_20260202.csv
             - account_products_20260202.csv
             - loans_20260202.csv
             - ... (22 files total)
                    │
                    ▼
  08:30 AM   Run csv_importer.py
             $ python scripts/csv_importer.py

             - Validates all CSV files
             - Creates new tables if needed
             - Imports data to raw tables
             - Archives files to data/processed/2026-02-02/
                    │
                    ▼
  09:00 AM   Run raw_to_stg.py
             $ python scripts/raw_to_stg.py

             - Processes all raw tables
             - Deduplicates by primary key
             - Updates staging tables
                    │
                    ▼
  09:30 AM   ETL Complete - Data ready for analysis
```

### 7.2 First-Time Setup

```bash
# Step 1: Create database (run once)
python scripts/database_creator.py

# Step 2: Place first CSV files in data/incoming/

# Step 3: Run import (creates all tables)
python scripts/csv_importer.py

# Step 4: Run staging (creates stg tables)
python scripts/raw_to_stg.py
```

### 7.3 Adding a New CSV Type

When a new type of CSV file is introduced:

1. Add the file to `data/incoming/` following naming convention
2. Add primary key(s) to `table_keys_config.json`
3. Run normal daily workflow - tables will be created automatically

**Example:** Adding a new `fraud_alerts.csv`

```json
// Add to table_keys_config.json:
{
  "FraudAlerts": ["alert_id"]
}
```

Then place `fraud_alerts_20260202.csv` in `data/incoming/` and run the scripts.

---

## 8. CSV File Specifications

### 8.1 Filename Convention

```
{base_name}_{date}.csv

Where:
- base_name: Table identifier using underscores (e.g., customer_profiles)
- date: Date in YYYYMMDD format (e.g., 20260202)
```

**Examples:**
| Filename | Base Name | Table Created |
|----------|-----------|---------------|
| customer_profiles_20260202.csv | customer_profiles | rawCustomerProfiles |
| account_products_20260202.csv | account_products | rawAccountProducts |
| failed_transactions_20260202.csv | failed_transactions | rawFailedTransactions |

### 8.2 Base Name to Table Name Conversion

```
customer_profiles → CustomerProfiles
account_products  → AccountProducts
audit_log         → AuditLog
atm_locations     → AtmLocations
```

**Rule:** Underscores removed, each word capitalized (PascalCase)

### 8.3 Required CSV Format

- **Encoding:** UTF-8 (preferred), Latin-1, or CP1252
- **Delimiter:** Comma (,)
- **Header:** First row must contain column names
- **Quoting:** Use double quotes for fields containing commas

### 8.4 Complete CSV File List

| # | CSV Filename | Table Name | Primary Key |
|---|--------------|------------|-------------|
| 1 | customer_profiles_{date}.csv | CustomerProfiles | customer_id |
| 2 | customer_documents_{date}.csv | CustomerDocuments | document_id |
| 3 | customer_preferences_{date}.csv | CustomerPreferences | preference_id |
| 4 | account_products_{date}.csv | AccountProducts | account_id |
| 5 | account_statements_{date}.csv | AccountStatements | account_id, statement_date |
| 6 | account_limits_{date}.csv | AccountLimits | account_id, effective_date |
| 7 | account_beneficiaries_{date}.csv | AccountBeneficiaries | beneficiary_id |
| 8 | pending_transactions_{date}.csv | PendingTransactions | transaction_id |
| 9 | failed_transactions_{date}.csv | FailedTransactions | transaction_id |
| 10 | recurring_payments_{date}.csv | RecurringPayments | payment_id |
| 11 | transaction_disputes_{date}.csv | TransactionDisputes | dispute_id |
| 12 | loans_{date}.csv | Loans | loan_id |
| 13 | credit_cards_{date}.csv | CreditCards | card_id |
| 14 | investments_{date}.csv | Investments | investment_id |
| 15 | savings_goals_{date}.csv | SavingsGoals | goal_id |
| 16 | branches_{date}.csv | Branches | branch_id |
| 17 | atm_locations_{date}.csv | AtmLocations | atm_id |
| 18 | employees_{date}.csv | Employees | employee_id |
| 19 | audit_log_{date}.csv | AuditLog | log_id |
| 20 | monthly_balances_{date}.csv | MonthlyBalances | account_id, month |
| 21 | category_spending_{date}.csv | CategorySpending | account_id, category, month |
| 22 | merchant_frequency_{date}.csv | MerchantFrequency | account_id, merchant_name |

---

## 9. Error Handling

### 9.1 Column Mismatch Error

**Scenario:** CSV has different columns than existing table.

**Behavior:** File is skipped, error logged.

**Example:**
```
[ERROR] customer_profiles_20260203.csv
        Column mismatch detected!

        CSV has extra columns:
          - middle_name
          - nickname

        Table missing columns:
          - middle_name
          - nickname

        Action: SKIPPED - Please fix CSV or update table schema
```

**Resolution:**
1. Check if CSV columns are correct
2. If new columns are needed, manually add them to the table
3. Re-run import

### 9.2 Missing Primary Key in Config

**Scenario:** Table not found in table_keys_config.json.

**Behavior:** raw_to_stg uses ALL columns for deduplication (DISTINCT *).

**Example:**
```
[WARNING] rawNewTable has no key defined in table_keys_config.json
          Using all columns for deduplication (DISTINCT *)
```

**Resolution:**
Add the table and its key(s) to table_keys_config.json.

### 9.3 File Encoding Error

**Scenario:** CSV cannot be read with any attempted encoding.

**Behavior:** File is skipped, error logged.

**Example:**
```
[ERROR] corrupted_file_20260202.csv
        Could not read file with any encoding
        Attempted: utf-8, latin-1, cp1252
        Action: SKIPPED
```

**Resolution:**
1. Open file in text editor
2. Re-save with UTF-8 encoding
3. Re-run import

### 9.4 Database Connection Error

**Scenario:** Cannot connect to database.

**Behavior:** Script exits with error.

**Resolution:**
1. Check database_config.json path is correct
2. Ensure database file exists (run database_creator.py)
3. Check file permissions

---

## 10. Troubleshooting

### Q: CSV file not being detected

**Check:**
1. File is in `data/incoming/` folder
2. Filename follows pattern: `{name}_{YYYYMMDD}.csv`
3. File has .csv extension (lowercase)

### Q: Table not being created

**Check:**
1. CSV has valid header row
2. CSV is not empty
3. No special characters in column names

### Q: Duplicates appearing in staging table

**Check:**
1. Primary key is defined in table_keys_config.json
2. Key column(s) exist in the CSV
3. Key values are not NULL

### Q: Import is slow

**Tips:**
1. Process fewer files at once
2. Increase batch size in settings
3. Archive old data from raw tables periodically

### Q: Need to re-import a file

**Steps:**
1. Move file from `data/processed/` back to `data/incoming/`
2. (Optional) Clear existing data from raw table
3. Run csv_importer.py

### Q: Need to add a column to existing table

**Steps:**
1. Manually run ALTER TABLE in SQLite:
   ```sql
   ALTER TABLE rawCustomerProfiles ADD COLUMN new_column TEXT;
   ALTER TABLE stgCustomerProfiles ADD COLUMN new_column TEXT;
   ```
2. Update CSV files to include new column
3. Resume normal imports

---

## Appendix A: SQL Reference

### View all tables
```sql
SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;
```

### Count rows in all raw tables
```sql
SELECT 'rawCustomerProfiles' as table_name, COUNT(*) as rows FROM rawCustomerProfiles
UNION ALL
SELECT 'rawLoans', COUNT(*) FROM rawLoans
-- ... add more tables
```

### Compare raw vs staging counts
```sql
SELECT
    'CustomerProfiles' as table_name,
    (SELECT COUNT(*) FROM rawCustomerProfiles) as raw_count,
    (SELECT COUNT(*) FROM stgCustomerProfiles) as stg_count;
```

### Find duplicates in raw table
```sql
SELECT customer_id, COUNT(*) as count
FROM rawCustomerProfiles
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

---

## Appendix B: Glossary

| Term | Definition |
|------|------------|
| ETL | Extract, Transform, Load - data pipeline process |
| Raw Table | First landing zone for imported data, keeps all records |
| Staging Table | Cleaned layer with deduplicated records |
| Primary Key | Column(s) that uniquely identify a record |
| Composite Key | Primary key made of multiple columns |
| Deduplication | Process of removing duplicate records |

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-02-02 | Nevra Donat | Initial documentation |

---

**End of Documentation**
