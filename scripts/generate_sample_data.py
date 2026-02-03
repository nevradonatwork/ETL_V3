"""
Sample Data Generator
=====================
Generates realistic sample CSV data for testing and demo purposes.

Usage:
    python scripts/generate_sample_data.py

Author: Nevra Donat
"""

import csv
import os
import random
from datetime import datetime, timedelta


# Configuration
def get_project_root():
    """Get the project root directory (normalized for Windows)."""
    return os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

SAMPLE_FOLDER = os.path.normpath(os.path.join(get_project_root(), 'data', 'sample'))
DATE_STR = datetime.now().strftime("%Y%m%d")

# Random data pools
FIRST_NAMES = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda',
               'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica',
               'Thomas', 'Sarah', 'Charles', 'Karen', 'Emma', 'Oliver', 'Ava', 'Noah', 'Sophia']

LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
              'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
              'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Thompson', 'White']

CITIES = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia',
          'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville',
          'Fort Worth', 'Columbus', 'Charlotte', 'Seattle', 'Denver', 'Boston', 'Detroit', 'Portland']

STATES = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA',
          'TX', 'FL', 'TX', 'OH', 'NC', 'WA', 'CO', 'MA', 'MI', 'OR']

SEGMENTS = ['retail', 'premium', 'private', 'business']
RISK_RATINGS = ['low', 'medium', 'high']
ACCOUNT_TYPES = ['checking', 'savings', 'credit', 'loan']
CURRENCIES = ['USD', 'EUR', 'GBP']
LOAN_TYPES = ['personal', 'mortgage', 'auto', 'business']
CARD_TYPES = ['visa', 'mastercard', 'amex']
ASSET_TYPES = ['stock', 'bond', 'mutual_fund', 'etf']
TRANSACTION_TYPES = ['debit', 'credit']
ERROR_CODES = ['E001', 'E002', 'E003', 'E004', 'E005']
FREQUENCIES = ['daily', 'weekly', 'monthly', 'yearly']


def random_date(start_year=2024, end_year=2026):
    """Generate random date string."""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime("%Y-%m-%d")


def random_phone():
    """Generate random phone number."""
    return f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"


def random_email(first, last):
    """Generate email from name."""
    domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'email.com']
    return f"{first.lower()}.{last.lower()}@{random.choice(domains)}"


def write_csv(filename, headers, rows):
    """Write CSV file."""
    filepath = os.path.join(SAMPLE_FOLDER, filename)
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"  Created: {filename} ({len(rows)} rows)")


def generate_customer_profiles(n=500):
    """Generate customer_profiles CSV."""
    headers = ['customer_id', 'name', 'email', 'phone', 'address', 'city', 'state',
               'segment', 'risk_rating', 'created_date', 'status']
    rows = []

    for i in range(1, n + 1):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        city_idx = random.randint(0, len(CITIES) - 1)

        rows.append([
            f"CUST{i:06d}",
            f"{first} {last}",
            random_email(first, last),
            random_phone(),
            f"{random.randint(100, 9999)} {last} Street",
            CITIES[city_idx],
            STATES[city_idx],
            random.choice(SEGMENTS),
            random.choice(RISK_RATINGS),
            random_date(2020, 2025),
            random.choices(['active', 'inactive', 'closed'], weights=[85, 10, 5])[0]
        ])

    write_csv(f"customer_profiles_{DATE_STR}.csv", headers, rows)
    return [row[0] for row in rows]  # Return customer IDs


def generate_account_products(customer_ids, n=800):
    """Generate account_products CSV."""
    headers = ['account_id', 'customer_id', 'branch_id', 'account_type', 'account_number',
               'balance', 'currency', 'opened_date', 'status']
    rows = []

    for i in range(1, n + 1):
        acc_type = random.choice(ACCOUNT_TYPES)
        if acc_type == 'checking':
            balance = round(random.uniform(100, 50000), 2)
        elif acc_type == 'savings':
            balance = round(random.uniform(1000, 200000), 2)
        elif acc_type == 'credit':
            balance = round(random.uniform(-10000, 0), 2)
        else:
            balance = round(random.uniform(-500000, -10000), 2)

        rows.append([
            f"ACC{i:08d}",
            random.choice(customer_ids),
            f"BR{random.randint(1, 50):03d}",
            acc_type,
            f"****{random.randint(1000, 9999)}",
            balance,
            random.choices(CURRENCIES, weights=[80, 15, 5])[0],
            random_date(2018, 2025),
            random.choices(['active', 'dormant', 'closed'], weights=[90, 7, 3])[0]
        ])

    write_csv(f"account_products_{DATE_STR}.csv", headers, rows)
    return [row[0] for row in rows]  # Return account IDs


def generate_loans(account_ids, n=200):
    """Generate loans CSV."""
    headers = ['loan_id', 'account_id', 'loan_type', 'principal', 'interest_rate',
               'term_months', 'monthly_payment', 'outstanding', 'start_date', 'end_date', 'status']
    rows = []

    for i in range(1, n + 1):
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

        start = random_date(2020, 2025)
        start_dt = datetime.strptime(start, "%Y-%m-%d")
        end_dt = start_dt + timedelta(days=term * 30)

        rows.append([
            f"LOAN{i:06d}",
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


def generate_credit_cards(account_ids, n=300):
    """Generate credit_cards CSV."""
    headers = ['card_id', 'account_id', 'card_number', 'card_type', 'credit_limit',
               'available_credit', 'apr', 'expiry_date', 'status']
    rows = []

    for i in range(1, n + 1):
        limit = random.choice([1000, 2500, 5000, 10000, 15000, 25000, 50000])
        used = round(random.uniform(0, limit * 0.8), 2)

        rows.append([
            f"CARD{i:06d}",
            random.choice(account_ids),
            f"****-****-****-{random.randint(1000, 9999)}",
            random.choice(CARD_TYPES),
            limit,
            round(limit - used, 2),
            round(random.uniform(12.99, 24.99), 2),
            f"20{random.randint(26, 30)}-{random.randint(1, 12):02d}",
            random.choices(['active', 'blocked', 'expired'], weights=[90, 5, 5])[0]
        ])

    write_csv(f"credit_cards_{DATE_STR}.csv", headers, rows)


def generate_branches(n=50):
    """Generate branches CSV."""
    headers = ['branch_id', 'branch_name', 'address', 'city', 'state', 'postal_code',
               'phone', 'hours', 'services']
    rows = []

    for i in range(1, n + 1):
        city_idx = random.randint(0, len(CITIES) - 1)
        rows.append([
            f"BR{i:03d}",
            f"{CITIES[city_idx]} Branch {i}",
            f"{random.randint(100, 9999)} Main Street",
            CITIES[city_idx],
            STATES[city_idx],
            f"{random.randint(10000, 99999)}",
            random_phone(),
            "Mon-Fri 9AM-5PM, Sat 9AM-1PM",
            "deposits,withdrawals,loans,investments"
        ])

    write_csv(f"branches_{DATE_STR}.csv", headers, rows)
    return [row[0] for row in rows]


def generate_employees(branch_ids, n=250):
    """Generate employees CSV."""
    headers = ['employee_id', 'branch_id', 'name', 'role', 'email', 'phone', 'hire_date', 'status']
    roles = ['Teller', 'Manager', 'Loan Officer', 'Customer Service', 'Financial Advisor']
    rows = []

    for i in range(1, n + 1):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)

        rows.append([
            f"EMP{i:05d}",
            random.choice(branch_ids),
            f"{first} {last}",
            random.choice(roles),
            f"{first.lower()}.{last.lower()}@bank.com",
            random_phone(),
            random_date(2015, 2025),
            random.choices(['active', 'on_leave', 'terminated'], weights=[90, 7, 3])[0]
        ])

    write_csv(f"employees_{DATE_STR}.csv", headers, rows)


def generate_pending_transactions(account_ids, n=150):
    """Generate pending_transactions CSV."""
    headers = ['transaction_id', 'account_id', 'amount', 'currency', 'transaction_type',
               'description', 'status', 'created_date', 'expected_clear']
    rows = []

    for i in range(1, n + 1):
        txn_type = random.choice(TRANSACTION_TYPES)
        amount = round(random.uniform(10, 5000), 2)
        created = datetime.now() - timedelta(days=random.randint(0, 5))
        expected = created + timedelta(days=random.randint(1, 3))

        rows.append([
            f"TXN{i:08d}",
            random.choice(account_ids),
            amount if txn_type == 'credit' else -amount,
            'USD',
            txn_type,
            f"{'Deposit' if txn_type == 'credit' else 'Payment'} - Ref#{random.randint(10000, 99999)}",
            random.choice(['pending', 'processing']),
            created.strftime("%Y-%m-%d %H:%M:%S"),
            expected.strftime("%Y-%m-%d")
        ])

    write_csv(f"pending_transactions_{DATE_STR}.csv", headers, rows)


def generate_failed_transactions(account_ids, n=50):
    """Generate failed_transactions CSV."""
    headers = ['transaction_id', 'account_id', 'amount', 'currency', 'transaction_type',
               'error_code', 'error_message', 'attempted_date', 'merchant']
    merchants = ['Amazon', 'Walmart', 'Target', 'Best Buy', 'Costco', 'Gas Station', 'Restaurant']
    error_messages = ['Insufficient funds', 'Card declined', 'Invalid PIN', 'Expired card', 'Fraud suspected']
    rows = []

    for i in range(1, n + 1):
        err_idx = random.randint(0, len(ERROR_CODES) - 1)
        rows.append([
            f"FTXN{i:07d}",
            random.choice(account_ids),
            round(random.uniform(10, 2000), 2),
            'USD',
            'debit',
            ERROR_CODES[err_idx],
            error_messages[err_idx],
            random_date(2025, 2026),
            random.choice(merchants)
        ])

    write_csv(f"failed_transactions_{DATE_STR}.csv", headers, rows)


def generate_investments(account_ids, n=100):
    """Generate investments CSV."""
    headers = ['investment_id', 'account_id', 'asset_type', 'symbol', 'quantity',
               'purchase_price', 'current_price', 'current_value', 'purchase_date']
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'V', 'JNJ',
               'BND', 'AGG', 'TLT', 'VFIAX', 'FXAIX', 'SPY', 'QQQ', 'VTI', 'IWM']
    rows = []

    for i in range(1, n + 1):
        asset = random.choice(ASSET_TYPES)
        symbol = random.choice(symbols)
        qty = round(random.uniform(1, 500), 2)
        purchase = round(random.uniform(50, 500), 2)
        current = round(purchase * random.uniform(0.7, 1.5), 2)

        rows.append([
            f"INV{i:06d}",
            random.choice(account_ids),
            asset,
            symbol,
            qty,
            purchase,
            current,
            round(qty * current, 2),
            random_date(2020, 2025)
        ])

    write_csv(f"investments_{DATE_STR}.csv", headers, rows)


def generate_atm_locations(branch_ids, n=100):
    """Generate atm_locations CSV."""
    headers = ['atm_id', 'branch_id', 'address', 'city', 'latitude', 'longitude',
               'available_24h', 'withdrawal_fee', 'deposit_enabled', 'status']
    rows = []

    for i in range(1, n + 1):
        city_idx = random.randint(0, len(CITIES) - 1)
        rows.append([
            f"ATM{i:04d}",
            random.choice(branch_ids) if random.random() > 0.3 else '',
            f"{random.randint(100, 9999)} {random.choice(LAST_NAMES)} Ave",
            CITIES[city_idx],
            round(random.uniform(25.0, 48.0), 6),
            round(random.uniform(-122.0, -71.0), 6),
            random.choice([0, 1]),
            round(random.choice([0, 2.5, 3.0, 3.5]), 2),
            random.choice([0, 1]),
            random.choices(['operational', 'maintenance', 'offline'], weights=[90, 7, 3])[0]
        ])

    write_csv(f"atm_locations_{DATE_STR}.csv", headers, rows)


def main():
    """Generate all sample data files."""

    print("=" * 70)
    print("SAMPLE DATA GENERATOR")
    print("Banking ETL System")
    print("=" * 70)

    # Ensure sample folder exists
    os.makedirs(SAMPLE_FOLDER, exist_ok=True)

    print(f"\nOutput folder: {SAMPLE_FOLDER}")
    print(f"Date suffix: {DATE_STR}")
    print("\nGenerating CSV files...")

    # Generate data with dependencies
    customer_ids = generate_customer_profiles(500)
    account_ids = generate_account_products(customer_ids, 800)
    branch_ids = generate_branches(50)

    # Generate related data
    generate_loans(account_ids, 200)
    generate_credit_cards(account_ids, 300)
    generate_employees(branch_ids, 250)
    generate_pending_transactions(account_ids, 150)
    generate_failed_transactions(account_ids, 50)
    generate_investments(account_ids, 100)
    generate_atm_locations(branch_ids, 100)

    print("\n" + "=" * 70)
    print("SAMPLE DATA GENERATION COMPLETE")
    print("=" * 70)

    # List files
    files = os.listdir(SAMPLE_FOLDER)
    csv_files = [f for f in files if f.endswith('.csv')]

    print(f"\nGenerated {len(csv_files)} CSV files:")
    for f in sorted(csv_files):
        filepath = os.path.join(SAMPLE_FOLDER, f)
        size = os.path.getsize(filepath)
        print(f"  - {f} ({size:,} bytes)")

    print("\n" + "=" * 70)
    print("Next steps:")
    print("  1. python scripts/database_creator.py")
    print("  2. python scripts/csv_importer.py --sample")
    print("  3. python scripts/raw_to_stg.py")
    print("  4. python scripts/stg_to_rpt.py")
    print("  5. streamlit run dashboard/app.py")
    print("=" * 70)


if __name__ == "__main__":
    main()
