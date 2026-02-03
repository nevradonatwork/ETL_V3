"""
Raw to Staging Processor
========================
Moves data from raw tables to staging tables with key-based deduplication.
Only inserts records that don't already exist in staging (based on primary key).

Usage:
    python scripts/raw_to_stg.py

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
    """Load all configuration files."""
    config_dir = os.path.join(get_project_root(), 'config')

    configs = {}

    # Database config
    with open(os.path.join(config_dir, 'database_config.json'), 'r') as f:
        configs['database'] = json.load(f)

    # Table keys config
    with open(os.path.join(config_dir, 'table_keys_config.json'), 'r') as f:
        configs['keys'] = json.load(f)

    return configs


def get_db_path(configs):
    """Get the full database path."""
    db_name = configs['database'].get('database_name', 'database/banking.db')
    return os.path.normpath(os.path.join(get_project_root(), db_name))


# ============================================================================
# DATABASE OPERATIONS
# ============================================================================

def get_all_raw_tables(conn):
    """Get all table names that start with 'raw'."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table'
        AND name LIKE 'raw%'
        AND name NOT LIKE '!_%' ESCAPE '!'
        ORDER BY name
    """)
    return [row[0] for row in cursor.fetchall()]


def get_table_columns(conn, table_name):
    """Get column names and types from a table."""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info([{table_name}])")
    return [(row[1], row[2]) for row in cursor.fetchall()]


def table_exists(conn, table_name):
    """Check if a table exists."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    return cursor.fetchone() is not None


def get_row_count(conn, table_name):
    """Get row count of a table."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
    return cursor.fetchone()[0]


def raw_to_stg_name(raw_table_name):
    """
    Convert raw table name to stg table name.

    Example: rawCustomerProfiles -> stgCustomerProfiles
    """
    if raw_table_name.startswith('raw'):
        return 'stg' + raw_table_name[3:]
    return 'stg' + raw_table_name


def get_base_table_name(raw_table_name):
    """
    Get base table name for key lookup.

    Example: rawCustomerProfiles -> CustomerProfiles
    """
    if raw_table_name.startswith('raw'):
        return raw_table_name[3:]
    return raw_table_name


# ============================================================================
# STAGING LOGIC
# ============================================================================

def create_stg_table(conn, raw_table_name, stg_table_name):
    """
    Create staging table with same structure as raw table.
    Adds staging metadata columns.
    """
    cursor = conn.cursor()

    # Get columns from raw table
    columns = get_table_columns(conn, raw_table_name)

    # Filter out raw metadata columns (we'll add stg metadata)
    columns = [(name, dtype) for name, dtype in columns
               if not name.startswith('_')]

    # Build column definitions
    col_defs = [f'"{name}" {dtype}' for name, dtype in columns]

    # Add staging metadata columns
    col_defs.append('"_stg_loaded_at" TEXT')
    col_defs.append('"_stg_source_table" TEXT')

    # Create table
    sql = f"""
        CREATE TABLE IF NOT EXISTS [{stg_table_name}] (
            {', '.join(col_defs)}
        )
    """
    cursor.execute(sql)
    conn.commit()

    return [name for name, _ in columns]


def get_key_columns(base_name, configs):
    """
    Get primary key column(s) for a table from config.

    Returns list of column names, or None if not configured.
    """
    keys_config = configs['keys']

    if base_name in keys_config:
        return keys_config[base_name]

    return None


def move_data_to_stg(conn, raw_table_name, stg_table_name, key_columns, data_columns):
    """
    Move data from raw to staging with deduplication.

    If key_columns is provided, only insert rows where key doesn't exist in stg.
    If key_columns is None, use DISTINCT on all columns.
    """
    cursor = conn.cursor()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Get counts before
    stg_count_before = get_row_count(conn, stg_table_name)

    # Build column list (excluding metadata columns)
    col_list = ', '.join([f'"{col}"' for col in data_columns])

    if key_columns:
        # Key-based deduplication
        # Build WHERE NOT EXISTS clause
        key_conditions = ' AND '.join([
            f'stg."{key}" = raw."{key}"' for key in key_columns
        ])

        sql = f"""
            INSERT INTO [{stg_table_name}] ({col_list}, "_stg_loaded_at", "_stg_source_table")
            SELECT DISTINCT {col_list}, '{now}', '{raw_table_name}'
            FROM [{raw_table_name}] raw
            WHERE NOT EXISTS (
                SELECT 1 FROM [{stg_table_name}] stg
                WHERE {key_conditions}
            )
        """
    else:
        # No key defined - use DISTINCT on all columns
        # Check if row already exists (all columns match)
        sql = f"""
            INSERT INTO [{stg_table_name}] ({col_list}, "_stg_loaded_at", "_stg_source_table")
            SELECT DISTINCT {col_list}, '{now}', '{raw_table_name}'
            FROM [{raw_table_name}]
            WHERE NOT EXISTS (
                SELECT 1 FROM [{stg_table_name}] stg
                WHERE {' AND '.join([f'stg."{col}" IS raw."{col}"' for col in data_columns])}
            )
        """

    cursor.execute(sql)
    conn.commit()

    # Get counts after
    stg_count_after = get_row_count(conn, stg_table_name)
    rows_inserted = stg_count_after - stg_count_before

    return {
        'rows_inserted': rows_inserted,
        'stg_before': stg_count_before,
        'stg_after': stg_count_after
    }


def process_single_table(conn, raw_table_name, configs, log_entries):
    """
    Process a single raw table to staging.

    Returns dict with results.
    """
    result = {
        'raw_table': raw_table_name,
        'stg_table': None,
        'success': False,
        'rows_inserted': 0,
        'message': ''
    }

    stg_table_name = raw_to_stg_name(raw_table_name)
    base_name = get_base_table_name(raw_table_name)
    result['stg_table'] = stg_table_name

    print(f"\n{'─' * 70}")
    print(f"Processing: {raw_table_name} -> {stg_table_name}")
    print(f"{'─' * 70}")

    # Get raw table row count
    raw_count = get_row_count(conn, raw_table_name)
    print(f"  Rows in raw: {raw_count:,}")

    if raw_count == 0:
        result['message'] = "Raw table is empty"
        print(f"  SKIPPED: Raw table is empty")
        return result

    # Check if stg table exists
    stg_exists = table_exists(conn, stg_table_name)

    # Get or create stg table
    if stg_exists:
        print(f"  Staging table exists")
        # Get data columns (non-metadata) from raw table
        raw_cols = get_table_columns(conn, raw_table_name)
        data_columns = [name for name, _ in raw_cols if not name.startswith('_')]
    else:
        print(f"  Creating staging table...")
        data_columns = create_stg_table(conn, raw_table_name, stg_table_name)
        print(f"  Created: {stg_table_name}")

    # Get key columns
    key_columns = get_key_columns(base_name, configs)

    if key_columns:
        # Verify key columns exist in data
        missing_keys = [k for k in key_columns if k not in data_columns]
        if missing_keys:
            result['message'] = f"Key columns not found: {missing_keys}"
            print(f"  ERROR: Key columns not found in table: {missing_keys}")
            return result
        print(f"  Key column(s): {key_columns}")
    else:
        print(f"  WARNING: No key defined, using DISTINCT on all columns")

    # Move data
    try:
        stats = move_data_to_stg(conn, raw_table_name, stg_table_name, key_columns, data_columns)

        result['success'] = True
        result['rows_inserted'] = stats['rows_inserted']
        result['message'] = 'Success'

        print(f"  Already in stg: {stats['stg_before']:,}")
        print(f"  New rows added: {stats['rows_inserted']:,}")
        print(f"  Total in stg: {stats['stg_after']:,}")

        # Log entry
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entries.append({
            'operation': 'raw_to_stg',
            'table_name': stg_table_name,
            'rows_affected': stats['rows_inserted'],
            'status': 'success',
            'message': f"From {raw_table_name}",
            'started_at': now,
            'completed_at': now
        })

    except Exception as e:
        result['message'] = str(e)
        print(f"  ERROR: {str(e)}")

    return result


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main entry point for raw to staging processor."""

    print("=" * 70)
    print("RAW TO STAGING PROCESSOR")
    print("Banking ETL System")
    print("=" * 70)

    # Load configuration
    try:
        configs = load_configs()
    except Exception as e:
        print(f"\nERROR loading configuration: {str(e)}")
        return False

    # Get database path
    db_path = get_db_path(configs)

    if not os.path.exists(db_path):
        print(f"\nERROR: Database does not exist: {db_path}")
        print("Please run: python scripts/database_creator.py")
        return False

    print(f"\nDatabase: {db_path}")

    # Connect to database
    conn = sqlite3.connect(db_path)

    # Get all raw tables
    raw_tables = get_all_raw_tables(conn)

    if not raw_tables:
        print("\nNo raw tables found in database.")
        print("Please run: python scripts/csv_importer.py")
        conn.close()
        return False

    print(f"\nFound {len(raw_tables)} raw table(s)")

    # Process each table
    results = []
    log_entries = []

    for i, raw_table in enumerate(raw_tables, 1):
        print(f"\n[{i}/{len(raw_tables)}]")
        result = process_single_table(conn, raw_table, configs, log_entries)
        results.append(result)

    # Write log entries
    if log_entries:
        cursor = conn.cursor()
        for entry in log_entries:
            try:
                cursor.execute('''
                    INSERT INTO _etl_log (operation, table_name, rows_affected, status, message, started_at, completed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (entry['operation'], entry['table_name'], entry['rows_affected'],
                      entry['status'], entry['message'], entry['started_at'], entry['completed_at']))
            except:
                pass
        conn.commit()

    conn.close()

    # Summary
    print("\n" + "=" * 70)
    print("STAGING SUMMARY")
    print("=" * 70)

    success_count = sum(1 for r in results if r['success'])
    failed_count = len(results) - success_count
    total_rows = sum(r['rows_inserted'] for r in results)

    print(f"\nTables processed: {len(results)}")
    print(f"Successful: {success_count}")
    print(f"Failed/Skipped: {failed_count}")
    print(f"Total new rows in staging: {total_rows:,}")

    if failed_count > 0:
        print("\nFailed/Skipped tables:")
        for r in results:
            if not r['success']:
                print(f"  - {r['raw_table']}: {r['message']}")

    print("\n" + "=" * 70)
    print("STAGING COMPLETE")
    print("=" * 70)
    print("\nNext step: python scripts/stg_to_rpt.py")

    return failed_count == 0


if __name__ == "__main__":
    main()
