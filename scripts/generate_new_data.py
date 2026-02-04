"""
New Data Generator
==================
Generates new realistic banking CSV data for the incoming folder.
Creates brand new customers, accounts, loans, etc. with different names.
Also creates transactions for EXISTING accounts from the database.

Usage:
    python scripts/generate_new_data.py

Author: Nevra Donat
"""

import csv
import os
import random
import sqlite3
import json
from datetime import datetime, timedelta


# ============================================================================
# CONFIGURATION
# ============================================================================

def get_project_root():
    """Get the project root directory."""
    return os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))


def load_db_config():
    """Load database config."""
    config_path = os.path.join(get_project_root(), 'config', 'database_config.json')
    with open(config_path, 'r') as f:
        return json.load(f)


ROOT = get_project_root()
INCOMING_FOLDER = os.path.normpath(os.path.join(ROOT, 'data', 'incoming'))
BABY_NAMES_FILE = os.path.normpath(os.path.join(ROOT, 'data', 'Popular_Baby_Names.csv'))
DATE_STR = datetime.now().strftime("%Y%m%d")


# --------------------------------------------------------------------------
# Load first names from Popular_Baby_Names.csv
# --------------------------------------------------------------------------
def load_baby_names():
    """Load unique first names from the Popular_Baby_Names.csv file."""
    if not os.path.exists(BABY_NAMES_FILE):
        print(f"  WARNING: {BABY_NAMES_FILE} not found, using fallback names")
        return [
            'Aisha', 'Yuki', 'Priya', 'Liam', 'Fatima', 'Carlos', 'Mei',
            'Zara', 'Kenji', 'Omar', 'Chloe', 'Elena', 'Luna', 'Mateo',
            'Felix', 'Anya', 'Rafael', 'Marco', 'Nora', 'Leo','Nevra'
        ]

    names = set()
    with open(BABY_NAMES_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get('name', '').strip()
            if name and len(name) >= 2:
                names.add(name)

    return sorted(names)


FIRST_NAMES = load_baby_names()

LAST_NAMES = [
    'Nakamura', 'Okafor', 'Petrov', 'Svensson', 'Al-Rashid', 'Moreau',
    'Kowalski', 'Fernandez', 'Yamamoto', 'Ibrahim', 'Johansson', 'Dubois',
    'Khatri', 'Volkov', 'Mendoza', 'Takahashi', 'Osei', 'Lindqvist',
    'Castellano', 'Singh', 'Antonov', 'Fujimoto', 'Diallo', 'Bergstrom',
    'Almeida', 'Kozlov', 'Tanaka', 'Okonkwo', 'Ericsson', 'Rivera',
    'Sharma', 'Ivanov', 'Kimura', 'Adeyemi', 'Larsson', 'Santos',
    'Gupta', 'Sorokin', 'Watanabe', 'Mensah', 'Nilsson', 'Costa'
]

CITIES = [
    'Miami', 'Atlanta', 'Minneapolis', 'Nashville', 'Sacramento',
    'Las Vegas', 'Baltimore', 'Milwaukee', 'Tampa', 'Orlando',
    'Raleigh', 'Pittsburgh', 'Cincinnati', 'Kansas City', 'Cleveland',
    'Salt Lake City', 'Richmond', 'Memphis', 'Louisville', 'Tucson'
]

STATES = [
    'FL', 'GA', 'MN', 'TN', 'CA',
    'NV', 'MD', 'WI', 'FL', 'FL',
    'NC', 'PA', 'OH', 'MO', 'OH',
    'UT', 'VA', 'TN', 'KY', 'AZ'
]

STREET_NAMES = [
    'Oak Avenue', 'Maple Drive', 'Cedar Lane', 'Pine Road', 'Elm Boulevard',
    'Birch Court', 'Willow Way', 'Aspen Circle', 'Spruce Terrace', 'Cypress Path',
    'Redwood Place', 'Magnolia Street', 'Chestnut Lane', 'Hickory Drive', 'Poplar Road'
]

SEGMENTS = ['retail', 'premium', 'private', 'business']
RISK_RATINGS = ['low', 'medium', 'high']
ACCOUNT_TYPES = ['checking', 'savings', 'credit', 'loan']
LOAN_TYPES = ['personal', 'mortgage', 'auto', 'business']
CARD_TYPES = ['visa', 'mastercard', 'amex']
ASSET_TYPES = ['stock', 'bond', 'mutual_fund', 'etf']
ERROR_CODES = {
    'E001': 'Insufficient funds',
    'E002': 'Card declined',
    'E003': 'Invalid PIN',
    'E004': 'Expired card',
    'E005': 'Fraud suspected'
}
MERCHANTS = [
    'Whole Foods', 'Home Depot', 'Starbucks', 'Netflix', 'Uber',
    'Apple Store', 'Nike', 'Trader Joes', 'Shell Gas', 'Spotify',
    'Walgreens', 'Chipotle', 'Delta Airlines', 'Airbnb', 'Lyft'
]
SYMBOLS = [
    'AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA', 'AMD',
    'NFLX', 'DIS', 'PYPL', 'SQ', 'SHOP', 'COIN', 'PLTR',
    'BND', 'AGG', 'TLT', 'VFIAX', 'SPY', 'QQQ', 'VTI', 'IWM', 'VOO'
]
ROLES = ['Teller', 'Manager', 'Loan Officer', 'Customer Service', 'Financial Advisor']


# ============================================================================
# HELPERS
# ============================================================================

def random_date(start_year=2024, end_year=2026):
    """Generate random date string."""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    return (start + timedelta(days=random.randint(0, delta.days))).strftime("%Y-%m-%d")


def random_phone():
    """Generate random phone number."""
    return f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"


def random_email(first, last):
    """Generate email from name."""
    domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'protonmail.com', 'icloud.com']
    return f"{first.lower()}.{last.lower()}@{random.choice(domains)}"


def write_csv(filename, headers, rows):
    """Write CSV file to incoming folder."""
    filepath = os.path.join(INCOMING_FOLDER, filename)
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"  Created: {filename} ({len(rows)} rows)")


def get_max_ids_from_db(db_path):
    """Query the database to find max IDs so new data starts after them."""
    ids = {}
    if not os.path.exists(db_path):
        return ids

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    queries = {
        'customer': ("stgCustomerProfiles", "customer_id", "CUST", 6),
        'account': ("stgAccountProducts", "account_id", "ACC", 8),
        'loan': ("stgLoans", "loan_id", "LOAN", 6),
        'card': ("stgCreditCards", "card_id", "CARD", 6),
        'branch': ("stgBranches", "branch_id", "BR", 3),
        'employee': ("stgEmployees", "employee_id", "EMP", 5),
        'atm': ("stgAtmLocations", "atm_id", "ATM", 4),
        'investment': ("stgInvestments", "investment_id", "INV", 6),
        'transaction': ("stgPendingTransactions", "transaction_id", "TXN", 8),
        'failed_txn': ("stgFailedTransactions", "transaction_id", "FTXN", 7),
    }

    for key, (table, col, prefix, digits) in queries.items():
        try:
            cursor.execute(f"SELECT MAX(CAST(SUBSTR({col}, {len(prefix)+1}) AS INTEGER)) FROM [{table}]")
            result = cursor.fetchone()[0]
            ids[key] = result if result else 0
        except Exception:
            ids[key] = 0

    conn.close()
    return ids


def get_existing_accounts_from_db(db_path):
    """Get existing account IDs from the database for creating transactions."""
    if not os.path.exists(db_path):
        return []

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT account_id FROM stgAccountProducts WHERE status = 'active'")
        accounts = [row[0] for row in cursor.fetchall()]
    except Exception:
        accounts = []
    conn.close()
    return accounts


def get_existing_branches_from_db(db_path):
    """Get existing branch IDs from the database."""
    if not os.path.exists(db_path):
        return []

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT branch_id FROM stgBranches")
        branches = [row[0] for row in cursor.fetchall()]
    except Exception:
        branches = []
    conn.close()
    return branches


# ============================================================================
# DATA GENERATORS
# ============================================================================

def generate_customer_profiles(start_id, n=200):
    """Generate new customer profiles with diverse names."""
    headers = ['customer_id', 'name', 'email', 'phone', 'address', 'city', 'state',
               'segment', 'risk_rating', 'created_date', 'status']
    rows = []

    for i in range(1, n + 1):
        cid = start_id + i
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        city_idx = random.randint(0, len(CITIES) - 1)

        rows.append([
            f"CUST{cid:06d}",
            f"{first} {last}",
            random_email(first, last),
            random_phone(),
            f"{random.randint(100, 9999)} {random.choice(STREET_NAMES)}",
            CITIES[city_idx],
            STATES[city_idx],
            random.choice(SEGMENTS),
            random.choice(RISK_RATINGS),
            random_date(2024, 2026),
            random.choices(['active', 'inactive', 'closed'], weights=[85, 10, 5])[0]
        ])

    write_csv(f"customer_profiles_{DATE_STR}.csv", headers, rows)
    return [row[0] for row in rows]


def generate_account_products(customer_ids, existing_branches, start_id, n=350):
    """Generate new accounts for the new customers."""
    headers = ['account_id', 'customer_id', 'branch_id', 'account_type', 'account_number',
               'balance', 'currency', 'opened_date', 'status']
    rows = []

    for i in range(1, n + 1):
        aid = start_id + i
        acc_type = random.choice(ACCOUNT_TYPES)

        if acc_type == 'checking':
            balance = round(random.uniform(100, 50000), 2)
        elif acc_type == 'savings':
            balance = round(random.uniform(1000, 200000), 2)
        elif acc_type == 'credit':
            balance = round(random.uniform(-10000, 0), 2)
        else:
            balance = round(random.uniform(-500000, -10000), 2)

        branch = random.choice(existing_branches) if existing_branches else f"BR{random.randint(1,50):03d}"

        rows.append([
            f"ACC{aid:08d}",
            random.choice(customer_ids),
            branch,
            acc_type,
            f"****{random.randint(1000, 9999)}",
            balance,
            random.choices(['USD', 'EUR'], weights=[85, 15])[0],
            random_date(2023, 2026),
            random.choices(['active', 'dormant', 'closed'], weights=[90, 7, 3])[0]
        ])

    write_csv(f"account_products_{DATE_STR}.csv", headers, rows)
    return [row[0] for row in rows]


def generate_loans(account_ids, start_id, n=80):
    """Generate new loans."""
    headers = ['loan_id', 'account_id', 'loan_type', 'principal', 'interest_rate',
               'term_months', 'monthly_payment', 'outstanding', 'start_date', 'end_date', 'status']
    rows = []

    for i in range(1, n + 1):
        lid = start_id + i
        loan_type = random.choice(LOAN_TYPES)

        if loan_type == 'mortgage':
            principal = round(random.uniform(100000, 1000000), 2)
            term = random.choice([180, 240, 360])
        elif loan_type == 'auto':
            principal = round(random.uniform(10000, 80000), 2)
            term = random.choice([36, 48, 60, 72])
        elif loan_type == 'business':
            principal = round(random.uniform(50000, 500000), 2)
            term = random.choice([12, 24, 36, 60])
        else:
            principal = round(random.uniform(5000, 50000), 2)
            term = random.choice([12, 24, 36, 48, 60])

        rate = round(random.uniform(3.5, 15.0), 2)
        monthly = round(principal * (rate / 100 / 12) / (1 - (1 + rate / 100 / 12) ** (-term)), 2)
        paid_months = random.randint(0, term)
        outstanding = round(principal * (1 - paid_months / term), 2)

        start = random_date(2023, 2026)
        start_dt = datetime.strptime(start, "%Y-%m-%d")
        end_dt = start_dt + timedelta(days=term * 30)

        rows.append([
            f"LOAN{lid:06d}",
            random.choice(account_ids),
            loan_type,
            principal,
            rate,
            term,
            monthly,
            outstanding,
            start,
            end_dt.strftime("%Y-%m-%d"),
            random.choices(['active', 'paid_off', 'defaulted'], weights=[70, 25, 5])[0]
        ])

    write_csv(f"loans_{DATE_STR}.csv", headers, rows)


def generate_credit_cards(account_ids, start_id, n=120):
    """Generate new credit cards."""
    headers = ['card_id', 'account_id', 'card_number', 'card_type', 'credit_limit',
               'available_credit', 'apr', 'expiry_date', 'status']
    rows = []

    for i in range(1, n + 1):
        cid = start_id + i
        limit = random.choice([1000, 2500, 5000, 10000, 15000, 25000, 50000])
        used = round(random.uniform(0, limit * 0.8), 2)

        rows.append([
            f"CARD{cid:06d}",
            random.choice(account_ids),
            f"****-****-****-{random.randint(1000, 9999)}",
            random.choice(CARD_TYPES),
            limit,
            round(limit - used, 2),
            round(random.uniform(12.99, 24.99), 2),
            f"20{random.randint(27, 31)}-{random.randint(1, 12):02d}",
            random.choices(['active', 'blocked', 'expired'], weights=[90, 5, 5])[0]
        ])

    write_csv(f"credit_cards_{DATE_STR}.csv", headers, rows)


def generate_branches(start_id, n=10):
    """Generate a few new branches."""
    headers = ['branch_id', 'branch_name', 'address', 'city', 'state', 'postal_code',
               'phone', 'hours', 'services']
    rows = []

    for i in range(1, n + 1):
        bid = start_id + i
        city_idx = random.randint(0, len(CITIES) - 1)
        rows.append([
            f"BR{bid:03d}",
            f"{CITIES[city_idx]} Branch {bid}",
            f"{random.randint(100, 9999)} {random.choice(STREET_NAMES)}",
            CITIES[city_idx],
            STATES[city_idx],
            f"{random.randint(10000, 99999)}",
            random_phone(),
            "Mon-Fri 9AM-5PM, Sat 9AM-1PM",
            "deposits,withdrawals,loans,investments"
        ])

    write_csv(f"branches_{DATE_STR}.csv", headers, rows)
    return [row[0] for row in rows]


def generate_employees(branch_ids, start_id, n=40):
    """Generate new employees for new branches."""
    headers = ['employee_id', 'branch_id', 'name', 'role', 'email', 'phone', 'hire_date', 'status']
    rows = []

    for i in range(1, n + 1):
        eid = start_id + i
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)

        rows.append([
            f"EMP{eid:05d}",
            random.choice(branch_ids),
            f"{first} {last}",
            random.choice(ROLES),
            f"{first.lower()}.{last.lower()}@bank.com",
            random_phone(),
            random_date(2020, 2026),
            random.choices(['active', 'on_leave', 'terminated'], weights=[90, 7, 3])[0]
        ])

    write_csv(f"employees_{DATE_STR}.csv", headers, rows)


def generate_atm_locations(branch_ids, start_id, n=20):
    """Generate new ATM locations."""
    headers = ['atm_id', 'branch_id', 'address', 'city', 'latitude', 'longitude',
               'available_24h', 'withdrawal_fee', 'deposit_enabled', 'status']
    rows = []

    for i in range(1, n + 1):
        aid = start_id + i
        city_idx = random.randint(0, len(CITIES) - 1)
        rows.append([
            f"ATM{aid:04d}",
            random.choice(branch_ids),
            f"{random.randint(100, 9999)} {random.choice(STREET_NAMES)}",
            CITIES[city_idx],
            round(random.uniform(25.0, 48.0), 6),
            round(random.uniform(-122.0, -71.0), 6),
            random.choice([0, 1]),
            round(random.choice([0, 2.5, 3.0, 3.5]), 2),
            random.choice([0, 1]),
            random.choices(['operational', 'maintenance', 'offline'], weights=[90, 7, 3])[0]
        ])

    write_csv(f"atm_locations_{DATE_STR}.csv", headers, rows)


def generate_investments(account_ids, start_id, n=50):
    """Generate new investments."""
    headers = ['investment_id', 'account_id', 'asset_type', 'symbol', 'quantity',
               'purchase_price', 'current_price', 'current_value', 'purchase_date']
    rows = []

    for i in range(1, n + 1):
        iid = start_id + i
        asset = random.choice(ASSET_TYPES)
        symbol = random.choice(SYMBOLS)
        qty = round(random.uniform(1, 500), 2)
        purchase = round(random.uniform(50, 500), 2)
        current = round(purchase * random.uniform(0.7, 1.5), 2)

        rows.append([
            f"INV{iid:06d}",
            random.choice(account_ids),
            asset,
            symbol,
            qty,
            purchase,
            current,
            round(qty * current, 2),
            random_date(2024, 2026)
        ])

    write_csv(f"investments_{DATE_STR}.csv", headers, rows)


def generate_pending_transactions(all_account_ids, start_id, n=200):
    """
    Generate pending transactions for BOTH new and existing accounts.
    Uses existing accounts from DB to make transactions for current customers too.
    """
    headers = ['transaction_id', 'account_id', 'amount', 'currency', 'transaction_type',
               'description', 'status', 'created_date', 'expected_clear']
    rows = []

    descriptions_credit = [
        'Direct Deposit - Payroll', 'Wire Transfer In', 'Deposit - Ref#{}',
        'ACH Credit - Refund', 'Mobile Deposit', 'Transfer In - Savings',
        'Interest Payment', 'Cashback Reward'
    ]
    descriptions_debit = [
        'Payment - Ref#{}', 'Bill Pay - Utilities', 'ACH Debit - Insurance',
        'Wire Transfer Out', 'Online Purchase', 'Transfer Out - Checking',
        'Subscription Payment', 'Mortgage Payment', 'Rent Payment'
    ]

    for i in range(1, n + 1):
        tid = start_id + i
        txn_type = random.choice(['credit', 'debit'])
        amount = round(random.uniform(10, 8000), 2)
        created = datetime.now() - timedelta(days=random.randint(0, 5))
        expected = created + timedelta(days=random.randint(1, 3))

        if txn_type == 'credit':
            desc = random.choice(descriptions_credit).format(random.randint(10000, 99999))
        else:
            desc = random.choice(descriptions_debit).format(random.randint(10000, 99999))

        rows.append([
            f"TXN{tid:08d}",
            random.choice(all_account_ids),
            amount if txn_type == 'credit' else -amount,
            'USD',
            txn_type,
            desc,
            random.choice(['pending', 'processing']),
            created.strftime("%Y-%m-%d %H:%M:%S"),
            expected.strftime("%Y-%m-%d")
        ])

    write_csv(f"pending_transactions_{DATE_STR}.csv", headers, rows)


def generate_failed_transactions(all_account_ids, start_id, n=60):
    """
    Generate failed transactions for BOTH new and existing accounts.
    """
    headers = ['transaction_id', 'account_id', 'amount', 'currency', 'transaction_type',
               'error_code', 'error_message', 'attempted_date', 'merchant']
    rows = []

    for i in range(1, n + 1):
        fid = start_id + i
        err_code = random.choice(list(ERROR_CODES.keys()))

        rows.append([
            f"FTXN{fid:07d}",
            random.choice(all_account_ids),
            round(random.uniform(10, 3000), 2),
            'USD',
            'debit',
            err_code,
            ERROR_CODES[err_code],
            random_date(2025, 2026),
            random.choice(MERCHANTS)
        ])

    write_csv(f"failed_transactions_{DATE_STR}.csv", headers, rows)


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Generate all new data files for the incoming folder."""

    print("=" * 70)
    print("NEW DATA GENERATOR")
    print("Banking ETL System")
    print("=" * 70)

    # Ensure incoming folder exists
    os.makedirs(INCOMING_FOLDER, exist_ok=True)
    print(f"\nOutput folder: {INCOMING_FOLDER}")
    print(f"Date suffix: {DATE_STR}")

    # Get database path
    db_config = load_db_config()
    db_path = os.path.normpath(os.path.join(ROOT, db_config.get('database_name', 'database/banking.db')))

    # Get max IDs from database so we don't create duplicates
    print(f"\nReading existing data from: {db_path}")
    max_ids = get_max_ids_from_db(db_path)
    existing_accounts = get_existing_accounts_from_db(db_path)
    existing_branches = get_existing_branches_from_db(db_path)

    print(f"  Existing accounts found: {len(existing_accounts)}")
    print(f"  Existing branches found: {len(existing_branches)}")

    if max_ids:
        print(f"  Starting IDs after: {max_ids}")

    # Generate new data
    print("\nGenerating CSV files...")

    # 1. New customers (200 new customers with diverse names)
    new_customer_ids = generate_customer_profiles(
        max_ids.get('customer', 500), n=200
    )

    # 2. New accounts for new customers
    new_account_ids = generate_account_products(
        new_customer_ids, existing_branches,
        max_ids.get('account', 800), n=350
    )

    # 3. New branches
    new_branch_ids = generate_branches(
        max_ids.get('branch', 50), n=10
    )
    all_branches = existing_branches + new_branch_ids

    # 4. New employees for both existing and new branches
    generate_employees(
        all_branches,
        max_ids.get('employee', 250), n=40
    )

    # 5. New ATM locations
    generate_atm_locations(
        all_branches,
        max_ids.get('atm', 100), n=20
    )

    # 6. New loans for new accounts
    generate_loans(
        new_account_ids,
        max_ids.get('loan', 200), n=80
    )

    # 7. New credit cards for new accounts
    generate_credit_cards(
        new_account_ids,
        max_ids.get('card', 300), n=120
    )

    # 8. New investments for new accounts
    generate_investments(
        new_account_ids,
        max_ids.get('investment', 100), n=50
    )

    # 9. Transactions for BOTH existing AND new accounts
    all_accounts = existing_accounts + new_account_ids
    print(f"\n  Generating transactions for {len(all_accounts)} accounts "
          f"({len(existing_accounts)} existing + {len(new_account_ids)} new)")

    generate_pending_transactions(
        all_accounts,
        max_ids.get('transaction', 150), n=200
    )

    generate_failed_transactions(
        all_accounts,
        max_ids.get('failed_txn', 50), n=60
    )

    # Summary
    print("\n" + "=" * 70)
    print("DATA GENERATION COMPLETE")
    print("=" * 70)

    files = sorted([f for f in os.listdir(INCOMING_FOLDER) if f.endswith('.csv')])
    total_rows = 0
    print(f"\nGenerated {len(files)} CSV files in data/incoming/:")
    for f in files:
        filepath = os.path.join(INCOMING_FOLDER, f)
        with open(filepath, 'r') as fh:
            count = sum(1 for _ in fh) - 1
        total_rows += count
        print(f"  - {f} ({count:,} rows)")

    print(f"\nTotal new rows: {total_rows:,}")
    print(f"\n{'=' * 70}")
    print("Next step: python scripts/run_pipeline.py")
    print("  (This will import, transform, and create reports)")
    print("=" * 70)


if __name__ == "__main__":
    main()
