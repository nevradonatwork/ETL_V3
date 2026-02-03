"""
Staging to Reporting Processor
==============================
Creates aggregated reporting tables from staging data for the dashboard.
These tables are optimized for fast dashboard queries.

Usage:
    python scripts/stg_to_rpt.py

Author: Nevra Donat
"""

import sqlite3
import json
import os
from datetime import datetime


# ============================================================================
# CONFIGURATION
# ============================================================================

def get_project_root():
    """Get the project root directory (normalized for Windows)."""
    return os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))


def load_configs():
    """Load configuration files."""
    config_dir = os.path.join(get_project_root(), 'config')

    with open(os.path.join(config_dir, 'database_config.json'), 'r') as f:
        return json.load(f)


def get_db_path(config):
    """Get the full database path."""
    db_name = config.get('database_name', 'database/banking.db')
    return os.path.normpath(os.path.join(get_project_root(), db_name))


def table_exists(conn, table_name):
    """Check if a table exists."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    return cursor.fetchone() is not None


# ============================================================================
# REPORT GENERATORS
# ============================================================================

def create_kpi_summary(conn):
    """
    Create rptDashboardKPIs - Main KPIs for dashboard header.
    """
    print("\n  Creating rptDashboardKPIs...")

    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Drop and recreate
    cursor.execute("DROP TABLE IF EXISTS rptDashboardKPIs")

    cursor.execute("""
        CREATE TABLE rptDashboardKPIs (
            kpi_name TEXT PRIMARY KEY,
            kpi_value REAL,
            kpi_format TEXT,
            kpi_category TEXT,
            updated_at TEXT
        )
    """)

    # Calculate KPIs based on available tables
    kpis = []

    # Customer count
    if table_exists(conn, 'stgCustomerProfiles'):
        cursor.execute("SELECT COUNT(*) FROM stgCustomerProfiles")
        count = cursor.fetchone()[0]
        kpis.append(('total_customers', count, 'number', 'customer', now))

        # Active customers
        cursor.execute("SELECT COUNT(*) FROM stgCustomerProfiles WHERE status = 'active' OR status IS NULL")
        active = cursor.fetchone()[0]
        kpis.append(('active_customers', active, 'number', 'customer', now))

    # Account count and balance
    if table_exists(conn, 'stgAccountProducts'):
        cursor.execute("SELECT COUNT(*) FROM stgAccountProducts")
        count = cursor.fetchone()[0]
        kpis.append(('total_accounts', count, 'number', 'account', now))

        cursor.execute("SELECT COALESCE(SUM(balance), 0) FROM stgAccountProducts")
        balance = cursor.fetchone()[0]
        kpis.append(('total_balance', balance, 'currency', 'account', now))

        cursor.execute("SELECT COALESCE(AVG(balance), 0) FROM stgAccountProducts")
        avg_balance = cursor.fetchone()[0]
        kpis.append(('avg_account_balance', avg_balance, 'currency', 'account', now))

    # Loan metrics
    if table_exists(conn, 'stgLoans'):
        cursor.execute("SELECT COUNT(*) FROM stgLoans")
        count = cursor.fetchone()[0]
        kpis.append(('total_loans', count, 'number', 'loan', now))

        cursor.execute("SELECT COALESCE(SUM(principal), 0) FROM stgLoans")
        total = cursor.fetchone()[0]
        kpis.append(('total_loan_amount', total, 'currency', 'loan', now))

        cursor.execute("SELECT COUNT(*) FROM stgLoans WHERE status = 'active' OR status IS NULL")
        active = cursor.fetchone()[0]
        kpis.append(('active_loans', active, 'number', 'loan', now))

    # Transaction metrics
    if table_exists(conn, 'stgPendingTransactions'):
        cursor.execute("SELECT COUNT(*) FROM stgPendingTransactions")
        count = cursor.fetchone()[0]
        kpis.append(('pending_transactions', count, 'number', 'transaction', now))

        cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM stgPendingTransactions")
        amount = cursor.fetchone()[0]
        kpis.append(('pending_amount', amount, 'currency', 'transaction', now))

    if table_exists(conn, 'stgFailedTransactions'):
        cursor.execute("SELECT COUNT(*) FROM stgFailedTransactions")
        count = cursor.fetchone()[0]
        kpis.append(('failed_transactions', count, 'number', 'transaction', now))

    # Branch and employee counts
    if table_exists(conn, 'stgBranches'):
        cursor.execute("SELECT COUNT(*) FROM stgBranches")
        count = cursor.fetchone()[0]
        kpis.append(('total_branches', count, 'number', 'operations', now))

    if table_exists(conn, 'stgEmployees'):
        cursor.execute("SELECT COUNT(*) FROM stgEmployees")
        count = cursor.fetchone()[0]
        kpis.append(('total_employees', count, 'number', 'operations', now))

    if table_exists(conn, 'stgAtmLocations'):
        cursor.execute("SELECT COUNT(*) FROM stgAtmLocations")
        count = cursor.fetchone()[0]
        kpis.append(('total_atms', count, 'number', 'operations', now))

    # Credit card metrics
    if table_exists(conn, 'stgCreditCards'):
        cursor.execute("SELECT COUNT(*) FROM stgCreditCards")
        count = cursor.fetchone()[0]
        kpis.append(('total_credit_cards', count, 'number', 'products', now))

        cursor.execute("SELECT COALESCE(SUM(credit_limit), 0) FROM stgCreditCards")
        total = cursor.fetchone()[0]
        kpis.append(('total_credit_limit', total, 'currency', 'products', now))

    # Investment metrics
    if table_exists(conn, 'stgInvestments'):
        cursor.execute("SELECT COUNT(*) FROM stgInvestments")
        count = cursor.fetchone()[0]
        kpis.append(('total_investments', count, 'number', 'products', now))

        cursor.execute("SELECT COALESCE(SUM(current_value), 0) FROM stgInvestments")
        total = cursor.fetchone()[0]
        kpis.append(('total_investment_value', total, 'currency', 'products', now))

    # Insert KPIs
    for kpi in kpis:
        cursor.execute("""
            INSERT INTO rptDashboardKPIs (kpi_name, kpi_value, kpi_format, kpi_category, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, kpi)

    conn.commit()
    print(f"    Created {len(kpis)} KPIs")
    return len(kpis)


def create_customer_summary(conn):
    """
    Create rptCustomerSummary - Customer analytics by segment.
    """
    if not table_exists(conn, 'stgCustomerProfiles'):
        print("\n  Skipping rptCustomerSummary - stgCustomerProfiles not found")
        return 0

    print("\n  Creating rptCustomerSummary...")

    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("DROP TABLE IF EXISTS rptCustomerSummary")

    cursor.execute("""
        CREATE TABLE rptCustomerSummary AS
        SELECT
            COALESCE(segment, 'Unknown') as segment,
            COALESCE(risk_rating, 'Unknown') as risk_rating,
            COUNT(*) as customer_count,
            COALESCE(status, 'active') as status,
            ? as updated_at
        FROM stgCustomerProfiles
        GROUP BY segment, risk_rating, status
    """, (now,))

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM rptCustomerSummary")
    count = cursor.fetchone()[0]
    print(f"    Created {count} summary rows")
    return count


def create_account_summary(conn):
    """
    Create rptAccountSummary - Account analytics by type.
    """
    if not table_exists(conn, 'stgAccountProducts'):
        print("\n  Skipping rptAccountSummary - stgAccountProducts not found")
        return 0

    print("\n  Creating rptAccountSummary...")

    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("DROP TABLE IF EXISTS rptAccountSummary")

    cursor.execute("""
        CREATE TABLE rptAccountSummary AS
        SELECT
            COALESCE(account_type, 'Unknown') as account_type,
            COALESCE(currency, 'USD') as currency,
            COALESCE(status, 'active') as status,
            COUNT(*) as account_count,
            COALESCE(SUM(balance), 0) as total_balance,
            COALESCE(AVG(balance), 0) as avg_balance,
            COALESCE(MIN(balance), 0) as min_balance,
            COALESCE(MAX(balance), 0) as max_balance,
            ? as updated_at
        FROM stgAccountProducts
        GROUP BY account_type, currency, status
    """, (now,))

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM rptAccountSummary")
    count = cursor.fetchone()[0]
    print(f"    Created {count} summary rows")
    return count


def create_transaction_summary(conn):
    """
    Create rptTransactionSummary - Transaction analytics.
    """
    print("\n  Creating rptTransactionSummary...")

    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("DROP TABLE IF EXISTS rptTransactionSummary")

    cursor.execute("""
        CREATE TABLE rptTransactionSummary (
            transaction_type TEXT,
            status TEXT,
            transaction_count INTEGER,
            total_amount REAL,
            avg_amount REAL,
            source_table TEXT,
            updated_at TEXT
        )
    """)

    # Pending transactions
    if table_exists(conn, 'stgPendingTransactions'):
        cursor.execute("""
            INSERT INTO rptTransactionSummary
            SELECT
                COALESCE(transaction_type, 'Unknown') as transaction_type,
                COALESCE(status, 'pending') as status,
                COUNT(*) as transaction_count,
                COALESCE(SUM(amount), 0) as total_amount,
                COALESCE(AVG(amount), 0) as avg_amount,
                'stgPendingTransactions' as source_table,
                ? as updated_at
            FROM stgPendingTransactions
            GROUP BY transaction_type, status
        """, (now,))

    # Failed transactions
    if table_exists(conn, 'stgFailedTransactions'):
        cursor.execute("""
            INSERT INTO rptTransactionSummary
            SELECT
                COALESCE(transaction_type, 'Unknown') as transaction_type,
                'failed' as status,
                COUNT(*) as transaction_count,
                COALESCE(SUM(amount), 0) as total_amount,
                COALESCE(AVG(amount), 0) as avg_amount,
                'stgFailedTransactions' as source_table,
                ? as updated_at
            FROM stgFailedTransactions
            GROUP BY transaction_type
        """, (now,))

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM rptTransactionSummary")
    count = cursor.fetchone()[0]
    print(f"    Created {count} summary rows")
    return count


def create_loan_summary(conn):
    """
    Create rptLoanSummary - Loan portfolio analytics.
    """
    if not table_exists(conn, 'stgLoans'):
        print("\n  Skipping rptLoanSummary - stgLoans not found")
        return 0

    print("\n  Creating rptLoanSummary...")

    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("DROP TABLE IF EXISTS rptLoanSummary")

    cursor.execute("""
        CREATE TABLE rptLoanSummary AS
        SELECT
            COALESCE(loan_type, 'Unknown') as loan_type,
            COALESCE(status, 'active') as status,
            COUNT(*) as loan_count,
            COALESCE(SUM(principal), 0) as total_principal,
            COALESCE(AVG(principal), 0) as avg_principal,
            COALESCE(AVG(interest_rate), 0) as avg_interest_rate,
            COALESCE(SUM(outstanding), 0) as total_outstanding,
            ? as updated_at
        FROM stgLoans
        GROUP BY loan_type, status
    """, (now,))

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM rptLoanSummary")
    count = cursor.fetchone()[0]
    print(f"    Created {count} summary rows")
    return count


def create_branch_summary(conn):
    """
    Create rptBranchSummary - Branch performance metrics.
    """
    if not table_exists(conn, 'stgBranches'):
        print("\n  Skipping rptBranchSummary - stgBranches not found")
        return 0

    print("\n  Creating rptBranchSummary...")

    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("DROP TABLE IF EXISTS rptBranchSummary")

    # Basic branch summary
    cursor.execute("""
        CREATE TABLE rptBranchSummary AS
        SELECT
            b.branch_id,
            b.branch_name,
            b.city,
            COALESCE(b.state, '') as state,
            0 as customer_count,
            0 as account_count,
            0 as employee_count,
            ? as updated_at
        FROM stgBranches b
    """, (now,))

    # Update employee counts if available
    if table_exists(conn, 'stgEmployees'):
        cursor.execute("""
            UPDATE rptBranchSummary
            SET employee_count = (
                SELECT COUNT(*) FROM stgEmployees e
                WHERE e.branch_id = rptBranchSummary.branch_id
            )
        """)

    # Update account counts if available (and branch_id exists)
    if table_exists(conn, 'stgAccountProducts'):
        try:
            cursor.execute("""
                UPDATE rptBranchSummary
                SET account_count = (
                    SELECT COUNT(*) FROM stgAccountProducts a
                    WHERE a.branch_id = rptBranchSummary.branch_id
                )
            """)
        except:
            pass  # branch_id might not exist in accounts

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM rptBranchSummary")
    count = cursor.fetchone()[0]
    print(f"    Created {count} summary rows")
    return count


def create_daily_metrics(conn):
    """
    Create rptDailyMetrics - Time series data for charts.
    """
    print("\n  Creating rptDailyMetrics...")

    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today = datetime.now().strftime("%Y-%m-%d")

    cursor.execute("DROP TABLE IF EXISTS rptDailyMetrics")

    cursor.execute("""
        CREATE TABLE rptDailyMetrics (
            metric_date TEXT,
            metric_name TEXT,
            metric_value REAL,
            updated_at TEXT,
            PRIMARY KEY (metric_date, metric_name)
        )
    """)

    # Count records imported today (from raw tables metadata)
    metrics = []

    # Get table counts by _imported_at date
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name LIKE 'raw%'
    """)
    raw_tables = [row[0] for row in cursor.fetchall()]

    for table in raw_tables:
        try:
            cursor.execute(f"""
                SELECT DATE(_imported_at) as import_date, COUNT(*) as count
                FROM [{table}]
                WHERE _imported_at IS NOT NULL
                GROUP BY DATE(_imported_at)
            """)
            for row in cursor.fetchall():
                if row[0]:
                    metrics.append((row[0], f'imported_{table}', row[1], now))
        except:
            pass

    # Insert metrics
    for metric in metrics:
        cursor.execute("""
            INSERT OR REPLACE INTO rptDailyMetrics (metric_date, metric_name, metric_value, updated_at)
            VALUES (?, ?, ?, ?)
        """, metric)

    conn.commit()
    print(f"    Created {len(metrics)} daily metrics")
    return len(metrics)


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main entry point for staging to reporting processor."""

    print("=" * 70)
    print("STAGING TO REPORTING PROCESSOR")
    print("Banking ETL System")
    print("=" * 70)

    # Load configuration
    try:
        config = load_configs()
    except Exception as e:
        print(f"\nERROR loading configuration: {str(e)}")
        return False

    # Get database path
    db_path = get_db_path(config)

    if not os.path.exists(db_path):
        print(f"\nERROR: Database does not exist: {db_path}")
        print("Please run: python scripts/database_creator.py")
        return False

    print(f"\nDatabase: {db_path}")

    # Connect to database
    conn = sqlite3.connect(db_path)

    print("\n" + "=" * 70)
    print("CREATING REPORT TABLES")
    print("=" * 70)

    # Create all report tables
    results = {}

    results['kpis'] = create_kpi_summary(conn)
    results['customer'] = create_customer_summary(conn)
    results['account'] = create_account_summary(conn)
    results['transaction'] = create_transaction_summary(conn)
    results['loan'] = create_loan_summary(conn)
    results['branch'] = create_branch_summary(conn)
    results['daily'] = create_daily_metrics(conn)

    # Update metadata
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO _etl_metadata (key, value, created_at, updated_at)
            VALUES ('last_report_refresh', ?, ?, ?)
        """, (now, now, now))
        conn.commit()
    except:
        pass

    conn.close()

    # Summary
    print("\n" + "=" * 70)
    print("REPORTING SUMMARY")
    print("=" * 70)

    total_rows = sum(results.values())
    print(f"\nReport tables created/updated:")
    print(f"  - rptDashboardKPIs: {results['kpis']} rows")
    print(f"  - rptCustomerSummary: {results['customer']} rows")
    print(f"  - rptAccountSummary: {results['account']} rows")
    print(f"  - rptTransactionSummary: {results['transaction']} rows")
    print(f"  - rptLoanSummary: {results['loan']} rows")
    print(f"  - rptBranchSummary: {results['branch']} rows")
    print(f"  - rptDailyMetrics: {results['daily']} rows")

    print(f"\nTotal rows: {total_rows}")

    print("\n" + "=" * 70)
    print("REPORTING COMPLETE")
    print("=" * 70)
    print("\nDashboard ready! Run: streamlit run dashboard/app.py")

    return True


if __name__ == "__main__":
    main()
