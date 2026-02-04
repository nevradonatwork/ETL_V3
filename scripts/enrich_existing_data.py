"""
Enrich Existing Data
====================
Reads existing customers from stgCustomerProfiles and creates additional
accounts, loans, credit cards, transactions, investments, and operations
data for them. Inserts directly into staging tables to boost dashboard KPIs.

After running, execute: python scripts/stg_to_rpt.py
to refresh report tables and see updated dashboard numbers.

Usage:
    python scripts/enrich_existing_data.py

Author: Nevra Donat
"""

import sqlite3
import json
import os
import random
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
NOW = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

LOAN_TYPES = ['personal', 'mortgage', 'auto', 'business']
CARD_TYPES = ['visa', 'mastercard', 'amex']
ASSET_TYPES = ['stock', 'bond', 'mutual_fund', 'etf']
SYMBOLS = [
    'AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA', 'AMD',
    'NFLX', 'DIS', 'PYPL', 'SQ', 'SHOP', 'COIN', 'PLTR',
    'BND', 'AGG', 'TLT', 'SPY', 'QQQ', 'VTI', 'IWM', 'VOO'
]
MERCHANTS = [
    'Whole Foods', 'Home Depot', 'Starbucks', 'Netflix', 'Uber',
    'Apple Store', 'Nike', 'Trader Joes', 'Shell Gas', 'Spotify',
    'Walgreens', 'Chipotle', 'Delta Airlines', 'Airbnb', 'Amazon',
    'Walmart', 'Target', 'Best Buy', 'Costco', 'CVS Pharmacy'
]
ERROR_CODES = {
    'E001': 'Insufficient funds',
    'E002': 'Card declined',
    'E003': 'Invalid PIN',
    'E004': 'Expired card',
    'E005': 'Fraud suspected'
}
ROLES = ['Teller', 'Manager', 'Loan Officer', 'Customer Service', 'Financial Advisor']
CITIES = [
    'Miami', 'Atlanta', 'Minneapolis', 'Nashville', 'Sacramento',
    'Las Vegas', 'Baltimore', 'Milwaukee', 'Tampa', 'Orlando',
    'Raleigh', 'Pittsburgh', 'Cincinnati', 'Kansas City', 'Cleveland'
]
STATES = ['FL', 'GA', 'MN', 'TN', 'CA', 'NV', 'MD', 'WI', 'FL', 'FL',
          'NC', 'PA', 'OH', 'MO', 'OH']
STREET_NAMES = [
    'Oak Avenue', 'Maple Drive', 'Cedar Lane', 'Pine Road', 'Elm Boulevard',
    'Birch Court', 'Willow Way', 'Aspen Circle', 'Spruce Terrace', 'Cypress Path'
]


# ============================================================================
# HELPERS
# ============================================================================

def random_date(start_year=2024, end_year=2026):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return (start + timedelta(days=random.randint(0, (end - start).days))).strftime("%Y-%m-%d")


def random_phone():
    return f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"


def get_max_id(conn, table, column, prefix):
    """Get the max numeric ID from a table."""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX(CAST(SUBSTR({column}, {len(prefix)+1}) AS INTEGER)) FROM [{table}]")
        result = cursor.fetchone()[0]
        return result if result else 0
    except Exception:
        return 0


# ============================================================================
# DATA GENERATORS — insert directly into staging tables
# ============================================================================

def create_accounts(conn, customer_ids, branch_ids, n=500):
    """Create new checking/savings accounts with POSITIVE balances for existing customers."""
    start_id = get_max_id(conn, 'stgAccountProducts', 'account_id', 'ACC')
    cursor = conn.cursor()
    new_account_ids = []

    for i in range(1, n + 1):
        aid = start_id + i
        acc_type = random.choices(
            ['checking', 'savings'],
            weights=[50, 50]
        )[0]

        if acc_type == 'checking':
            balance = round(random.uniform(1000, 75000), 2)
        else:
            balance = round(random.uniform(5000, 300000), 2)

        account_id = f"ACC{aid:08d}"
        new_account_ids.append(account_id)

        cursor.execute("""
            INSERT INTO stgAccountProducts
            (account_id, customer_id, branch_id, account_type, account_number,
             balance, currency, opened_date, status, _stg_loaded_at, _stg_source_table)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            account_id,
            random.choice(customer_ids),
            random.choice(branch_ids),
            acc_type,
            f"****{random.randint(1000, 9999)}",
            balance,
            'USD',
            random_date(2023, 2026),
            'active',
            NOW, 'enrichment'
        ))

    conn.commit()
    return new_account_ids


def create_loans(conn, account_ids, n=200):
    """Create new active loans."""
    start_id = get_max_id(conn, 'stgLoans', 'loan_id', 'LOAN')
    cursor = conn.cursor()

    for i in range(1, n + 1):
        lid = start_id + i
        loan_type = random.choice(LOAN_TYPES)

        if loan_type == 'mortgage':
            principal = round(random.uniform(150000, 800000), 2)
            term = random.choice([180, 240, 360])
        elif loan_type == 'auto':
            principal = round(random.uniform(15000, 60000), 2)
            term = random.choice([36, 48, 60, 72])
        elif loan_type == 'business':
            principal = round(random.uniform(50000, 400000), 2)
            term = random.choice([12, 24, 36, 60])
        else:
            principal = round(random.uniform(5000, 40000), 2)
            term = random.choice([12, 24, 36, 48, 60])

        rate = round(random.uniform(3.5, 14.0), 2)
        monthly = round(principal * (rate / 100 / 12) / (1 - (1 + rate / 100 / 12) ** (-term)), 2)
        paid_months = random.randint(0, term // 2)
        outstanding = round(principal * (1 - paid_months / term), 2)

        start = random_date(2023, 2026)
        start_dt = datetime.strptime(start, "%Y-%m-%d")
        end_dt = start_dt + timedelta(days=term * 30)

        cursor.execute("""
            INSERT INTO stgLoans
            (loan_id, account_id, loan_type, principal, interest_rate, term_months,
             monthly_payment, outstanding, start_date, end_date, status,
             _stg_loaded_at, _stg_source_table)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            f"LOAN{lid:06d}",
            random.choice(account_ids),
            loan_type,
            principal, rate, term, monthly, outstanding,
            start, end_dt.strftime("%Y-%m-%d"),
            'active',
            NOW, 'enrichment'
        ))

    conn.commit()


def create_credit_cards(conn, account_ids, n=300):
    """Create new active credit cards."""
    start_id = get_max_id(conn, 'stgCreditCards', 'card_id', 'CARD')
    cursor = conn.cursor()

    for i in range(1, n + 1):
        cid = start_id + i
        limit = random.choice([5000, 10000, 15000, 25000, 50000])
        used = round(random.uniform(0, limit * 0.6), 2)

        cursor.execute("""
            INSERT INTO stgCreditCards
            (card_id, account_id, card_number, card_type, credit_limit,
             available_credit, apr, expiry_date, status,
             _stg_loaded_at, _stg_source_table)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            f"CARD{cid:06d}",
            random.choice(account_ids),
            f"****-****-****-{random.randint(1000, 9999)}",
            random.choice(CARD_TYPES),
            limit,
            round(limit - used, 2),
            round(random.uniform(12.99, 22.99), 2),
            f"20{random.randint(27, 31)}-{random.randint(1, 12):02d}",
            'active',
            NOW, 'enrichment'
        ))

    conn.commit()


def create_pending_transactions(conn, account_ids, n=400):
    """Create new pending transactions for existing and new accounts."""
    start_id = get_max_id(conn, 'stgPendingTransactions', 'transaction_id', 'TXN')
    cursor = conn.cursor()

    descriptions_credit = [
        'Direct Deposit - Payroll', 'Wire Transfer In', 'Deposit - Ref#{}',
        'ACH Credit - Refund', 'Mobile Deposit', 'Transfer In - Savings',
        'Interest Payment', 'Cashback Reward', 'Insurance Reimbursement',
        'Tax Refund', 'Bonus Payment', 'Freelance Income'
    ]
    descriptions_debit = [
        'Payment - Ref#{}', 'Bill Pay - Utilities', 'ACH Debit - Insurance',
        'Wire Transfer Out', 'Online Purchase', 'Transfer Out - Checking',
        'Subscription Payment', 'Mortgage Payment', 'Rent Payment',
        'Grocery Store', 'Gas Station', 'Restaurant'
    ]

    for i in range(1, n + 1):
        tid = start_id + i
        txn_type = random.choice(['credit', 'debit'])
        amount = round(random.uniform(25, 8000), 2)
        created = datetime.now() - timedelta(days=random.randint(0, 5))
        expected = created + timedelta(days=random.randint(1, 3))

        if txn_type == 'credit':
            desc = random.choice(descriptions_credit).format(random.randint(10000, 99999))
        else:
            desc = random.choice(descriptions_debit).format(random.randint(10000, 99999))

        cursor.execute("""
            INSERT INTO stgPendingTransactions
            (transaction_id, account_id, amount, currency, transaction_type,
             description, status, created_date, expected_clear,
             _stg_loaded_at, _stg_source_table)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            f"TXN{tid:08d}",
            random.choice(account_ids),
            amount if txn_type == 'credit' else -amount,
            'USD',
            txn_type,
            desc,
            random.choice(['pending', 'processing']),
            created.strftime("%Y-%m-%d %H:%M:%S"),
            expected.strftime("%Y-%m-%d"),
            NOW, 'enrichment'
        ))

    conn.commit()


def create_failed_transactions(conn, account_ids, n=100):
    """Create new failed transactions."""
    start_id = get_max_id(conn, 'stgFailedTransactions', 'transaction_id', 'FTXN')
    cursor = conn.cursor()

    for i in range(1, n + 1):
        fid = start_id + i
        err_code = random.choice(list(ERROR_CODES.keys()))

        cursor.execute("""
            INSERT INTO stgFailedTransactions
            (transaction_id, account_id, amount, currency, transaction_type,
             error_code, error_message, attempted_date, merchant,
             _stg_loaded_at, _stg_source_table)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            f"FTXN{fid:07d}",
            random.choice(account_ids),
            round(random.uniform(20, 3000), 2),
            'USD', 'debit',
            err_code, ERROR_CODES[err_code],
            random_date(2025, 2026),
            random.choice(MERCHANTS),
            NOW, 'enrichment'
        ))

    conn.commit()


def create_investments(conn, account_ids, n=150):
    """Create new investments."""
    start_id = get_max_id(conn, 'stgInvestments', 'investment_id', 'INV')
    cursor = conn.cursor()

    for i in range(1, n + 1):
        iid = start_id + i
        symbol = random.choice(SYMBOLS)
        qty = round(random.uniform(5, 500), 2)
        purchase = round(random.uniform(50, 500), 2)
        current = round(purchase * random.uniform(0.8, 1.4), 2)

        cursor.execute("""
            INSERT INTO stgInvestments
            (investment_id, account_id, asset_type, symbol, quantity,
             purchase_price, current_price, current_value, purchase_date,
             _stg_loaded_at, _stg_source_table)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            f"INV{iid:06d}",
            random.choice(account_ids),
            random.choice(ASSET_TYPES),
            symbol, qty, purchase, current,
            round(qty * current, 2),
            random_date(2024, 2026),
            NOW, 'enrichment'
        ))

    conn.commit()


def create_branches(conn, n=20):
    """Create new branches."""
    start_id = get_max_id(conn, 'stgBranches', 'branch_id', 'BR')
    cursor = conn.cursor()
    new_branch_ids = []

    for i in range(1, n + 1):
        bid = start_id + i
        city_idx = random.randint(0, len(CITIES) - 1)
        branch_id = f"BR{bid:03d}"
        new_branch_ids.append(branch_id)

        cursor.execute("""
            INSERT INTO stgBranches
            (branch_id, branch_name, address, city, state, postal_code,
             phone, hours, services, _stg_loaded_at, _stg_source_table)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            branch_id,
            f"{CITIES[city_idx]} Branch {bid}",
            f"{random.randint(100, 9999)} {random.choice(STREET_NAMES)}",
            CITIES[city_idx], STATES[city_idx],
            random.randint(10000, 99999),
            random_phone(),
            "Mon-Fri 9AM-5PM, Sat 9AM-1PM",
            "deposits,withdrawals,loans,investments",
            NOW, 'enrichment'
        ))

    conn.commit()
    return new_branch_ids


def create_employees(conn, branch_ids, n=80):
    """Create new employees for branches."""
    start_id = get_max_id(conn, 'stgEmployees', 'employee_id', 'EMP')
    cursor = conn.cursor()

    # Load baby names if available
    baby_names_file = os.path.join(ROOT, 'data', 'Popular_Baby_Names.csv')
    names = []
    if os.path.exists(baby_names_file):
        import csv
        with open(baby_names_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            name_set = set()
            for row in reader:
                name = row.get('name', '').strip()
                if name and len(name) >= 2:
                    name_set.add(name)
            names = list(name_set)

    if not names:
        names = ['Alex', 'Jordan', 'Morgan', 'Taylor', 'Casey', 'Riley', 'Quinn', 'Harper']

    last_names = [
        'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller',
        'Davis', 'Rodriguez', 'Martinez', 'Wilson', 'Anderson', 'Thomas',
        'Taylor', 'Moore', 'Jackson', 'Lee', 'Thompson', 'White', 'Harris'
    ]

    for i in range(1, n + 1):
        eid = start_id + i
        first = random.choice(names)
        last = random.choice(last_names)

        cursor.execute("""
            INSERT INTO stgEmployees
            (employee_id, branch_id, name, role, email, phone, hire_date, status,
             _stg_loaded_at, _stg_source_table)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            f"EMP{eid:05d}",
            random.choice(branch_ids),
            f"{first} {last}",
            random.choice(ROLES),
            f"{first.lower()}.{last.lower()}@bank.com",
            random_phone(),
            random_date(2020, 2026),
            random.choices(['active', 'on_leave'], weights=[92, 8])[0],
            NOW, 'enrichment'
        ))

    conn.commit()


def create_atm_locations(conn, branch_ids, n=30):
    """Create new ATM locations."""
    start_id = get_max_id(conn, 'stgAtmLocations', 'atm_id', 'ATM')
    cursor = conn.cursor()

    for i in range(1, n + 1):
        aid = start_id + i
        city_idx = random.randint(0, len(CITIES) - 1)

        cursor.execute("""
            INSERT INTO stgAtmLocations
            (atm_id, branch_id, address, city, latitude, longitude,
             available_24h, withdrawal_fee, deposit_enabled, status,
             _stg_loaded_at, _stg_source_table)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            f"ATM{aid:04d}",
            random.choice(branch_ids),
            f"{random.randint(100, 9999)} {random.choice(STREET_NAMES)}",
            CITIES[city_idx],
            round(random.uniform(25.0, 48.0), 6),
            round(random.uniform(-122.0, -71.0), 6),
            random.choice([0, 1]),
            round(random.choice([0, 2.5, 3.0, 3.5]), 2),
            random.choice([0, 1]),
            'operational',
            NOW, 'enrichment'
        ))

    conn.commit()


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 70)
    print("ENRICH EXISTING DATA")
    print("Banking ETL System")
    print("=" * 70)

    # Connect to database
    db_config = load_db_config()
    db_path = os.path.normpath(os.path.join(ROOT, db_config.get('database_name', 'database/banking.db')))

    if not os.path.exists(db_path):
        print(f"\nERROR: Database not found: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get existing data
    print(f"\nDatabase: {db_path}")
    print("\nReading existing data...")

    cursor.execute("SELECT customer_id FROM stgCustomerProfiles")
    customer_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT account_id FROM stgAccountProducts")
    existing_account_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT branch_id FROM stgBranches")
    existing_branch_ids = [row[0] for row in cursor.fetchall()]

    # Current counts
    tables_info = [
        ('stgCustomerProfiles', 'Customers'),
        ('stgAccountProducts', 'Accounts'),
        ('stgLoans', 'Loans'),
        ('stgCreditCards', 'Credit Cards'),
        ('stgPendingTransactions', 'Pending Txns'),
        ('stgFailedTransactions', 'Failed Txns'),
        ('stgBranches', 'Branches'),
        ('stgEmployees', 'Employees'),
        ('stgAtmLocations', 'ATMs'),
        ('stgInvestments', 'Investments'),
    ]

    print("\n  BEFORE:")
    before_counts = {}
    for table, label in tables_info:
        cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
        count = cursor.fetchone()[0]
        before_counts[table] = count
        print(f"    {label}: {count:,}")

    cursor.execute("SELECT COALESCE(SUM(balance), 0) FROM stgAccountProducts")
    balance_before = cursor.fetchone()[0]
    print(f"    Total Balance: ${balance_before:,.2f}")

    # Generate new data
    print("\nGenerating new data for existing customers...")

    # 1. New branches + employees + ATMs
    print("\n  [1/7] Creating new branches...")
    new_branch_ids = create_branches(conn, n=20)
    all_branch_ids = existing_branch_ids + new_branch_ids
    print(f"         +20 branches")

    print("  [2/7] Creating new employees...")
    create_employees(conn, all_branch_ids, n=80)
    print(f"         +80 employees")

    print("  [3/7] Creating new ATM locations...")
    create_atm_locations(conn, all_branch_ids, n=30)
    print(f"         +30 ATMs")

    # 2. New accounts for existing customers (positive balances)
    print("  [4/7] Creating new accounts (checking/savings)...")
    new_account_ids = create_accounts(conn, customer_ids, all_branch_ids, n=500)
    all_account_ids = existing_account_ids + new_account_ids
    print(f"         +500 accounts")

    # 3. New loans (active)
    print("  [5/7] Creating new active loans...")
    create_loans(conn, all_account_ids, n=200)
    print(f"         +200 active loans")

    # 4. New credit cards (active)
    print("  [6/7] Creating new credit cards...")
    create_credit_cards(conn, all_account_ids, n=300)
    print(f"         +300 credit cards")

    # 5. Transactions + investments
    print("  [7/7] Creating transactions & investments...")
    create_pending_transactions(conn, all_account_ids, n=400)
    create_failed_transactions(conn, all_account_ids, n=100)
    create_investments(conn, all_account_ids, n=150)
    print(f"         +400 pending txns, +100 failed txns, +150 investments")

    # After counts
    print("\n  AFTER:")
    for table, label in tables_info:
        cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
        count = cursor.fetchone()[0]
        diff = count - before_counts[table]
        print(f"    {label}: {count:,} (+{diff:,})")

    cursor.execute("SELECT COALESCE(SUM(balance), 0) FROM stgAccountProducts")
    balance_after = cursor.fetchone()[0]
    balance_diff = balance_after - balance_before
    print(f"    Total Balance: ${balance_after:,.2f} (+${balance_diff:,.2f})")

    conn.close()

    # Now refresh reports
    print(f"\n{'=' * 70}")
    print("DATA ENRICHMENT COMPLETE")
    print(f"{'=' * 70}")
    print("\nRefreshing report tables...")

    import subprocess, sys
    result = subprocess.run(
        [sys.executable, os.path.join(ROOT, 'scripts', 'stg_to_rpt.py')],
        cwd=ROOT
    )

    if result.returncode == 0:
        print(f"\n{'=' * 70}")
        print("ALL DONE - Dashboard KPIs updated!")
        print(f"{'=' * 70}")
        print("\nDashboard ready! Run: streamlit run dashboard/app.py")
    else:
        print("\nWARNING: Report refresh failed. Run manually: python scripts/stg_to_rpt.py")


if __name__ == "__main__":
    main()
