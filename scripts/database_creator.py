"""
Database Creator
================
Creates the SQLite database for the Banking ETL System.
Run this script ONCE during initial setup.

Author: Nevra Donat
"""

import sqlite3
import json
import os
from datetime import datetime


def get_project_root():
    """Get the project root directory (normalized for Windows)."""
    return os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))


def load_config():
    """Load database configuration from JSON file."""
    config_path = os.path.join(get_project_root(), 'config', 'database_config.json')

    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Configuration file not found at {config_path}")
        return None
    except json.JSONDecodeError:
        print("ERROR: Invalid JSON in configuration file")
        return None


def create_database():
    """Create the SQLite database and metadata table."""

    print("=" * 70)
    print("DATABASE CREATOR")
    print("Banking ETL System")
    print("=" * 70)

    # Load configuration
    config = load_config()
    if not config:
        return False

    db_name = config.get('database_name', 'database/banking.db')

    # Ensure database directory exists
    db_dir = os.path.dirname(db_name)
    if db_dir:
        # Get absolute path relative to project root
        project_root = get_project_root()
        db_path = os.path.join(project_root, db_name)
        db_dir_path = os.path.dirname(db_path)

        if not os.path.exists(db_dir_path):
            os.makedirs(db_dir_path)
            print(f"\nCreated directory: {db_dir_path}")
    else:
        project_root = get_project_root()
        db_path = os.path.join(project_root, db_name)

    # Check if database already exists
    if os.path.exists(db_path):
        print(f"\nDatabase already exists: {db_path}")
        response = input("Do you want to recreate it? (yes/no): ").strip().lower()
        if response != 'yes':
            print("Database creation cancelled.")
            return False
        os.remove(db_path)
        print("Existing database removed.")

    # Create database
    try:
        print(f"\nCreating database: {db_path}")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create metadata table for tracking ETL operations
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS _etl_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT UNIQUE NOT NULL,
                value TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        ''')

        # Create ETL log table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS _etl_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation TEXT NOT NULL,
                table_name TEXT,
                rows_affected INTEGER,
                status TEXT,
                message TEXT,
                started_at TEXT,
                completed_at TEXT
            )
        ''')

        # Insert initial metadata
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute('''
            INSERT INTO _etl_metadata (key, value, created_at, updated_at)
            VALUES (?, ?, ?, ?)
        ''', ('database_created', now, now, now))

        cursor.execute('''
            INSERT INTO _etl_metadata (key, value, created_at, updated_at)
            VALUES (?, ?, ?, ?)
        ''', ('database_version', '1.0', now, now))

        conn.commit()
        conn.close()

        print("\n" + "=" * 70)
        print("DATABASE CREATED SUCCESSFULLY!")
        print("=" * 70)
        print(f"\nDatabase: {db_path}")
        print(f"Created at: {now}")
        print("\nNext steps:")
        print("  1. Place CSV files in data/incoming/")
        print("  2. Run: python scripts/csv_importer.py")
        print("  3. Run: python scripts/raw_to_stg.py")
        print("=" * 70)

        return True

    except Exception as e:
        print(f"\nERROR creating database: {str(e)}")
        return False


def get_database_info():
    """Display information about the existing database."""

    config = load_config()
    if not config:
        return

    db_name = config.get('database_name', 'database/banking.db')
    project_root = get_project_root()
    db_path = os.path.normpath(os.path.join(project_root, db_name))

    if not os.path.exists(db_path):
        print(f"Database does not exist: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("=" * 70)
    print("DATABASE INFORMATION")
    print("=" * 70)

    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = cursor.fetchall()

    print(f"\nDatabase: {db_path}")
    print(f"Total tables: {len(tables)}")

    # Count tables by type
    raw_tables = [t[0] for t in tables if t[0].startswith('raw')]
    stg_tables = [t[0] for t in tables if t[0].startswith('stg')]
    rpt_tables = [t[0] for t in tables if t[0].startswith('rpt')]
    system_tables = [t[0] for t in tables if t[0].startswith('_')]

    print(f"\nTable breakdown:")
    print(f"  Raw tables (raw*): {len(raw_tables)}")
    print(f"  Staging tables (stg*): {len(stg_tables)}")
    print(f"  Report tables (rpt*): {len(rpt_tables)}")
    print(f"  System tables (_*): {len(system_tables)}")

    # Show row counts for each table
    print("\n" + "-" * 70)
    print("Table Details:")
    print("-" * 70)

    for table in tables:
        table_name = table[0]
        cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
        count = cursor.fetchone()[0]
        print(f"  {table_name}: {count:,} rows")

    conn.close()
    print("=" * 70)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == '--info':
        get_database_info()
    else:
        create_database()
