"""
CSV Importer
============
Imports CSV files from the incoming folder into raw database tables.
Auto-detects files, validates columns, and creates tables as needed.

Usage:
    python scripts/csv_importer.py              # Process incoming folder
    python scripts/csv_importer.py --sample     # Process sample folder (demo)

Author: Nevra Donat
"""

import pandas as pd
import sqlite3
import json
import os
import re
import shutil
from datetime import datetime
from glob import glob


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

    # CSV import config
    with open(os.path.join(config_dir, 'csv_import_config.json'), 'r') as f:
        configs['csv'] = json.load(f)

    return configs


def get_db_path(configs):
    """Get the full database path."""
    db_name = configs['database'].get('database_name', 'database/banking.db')
    return os.path.normpath(os.path.join(get_project_root(), db_name))


# ============================================================================
# FILENAME PARSING
# ============================================================================

def parse_filename(filename):
    """
    Parse CSV filename to extract base name and date.

    Examples:
        customer_profiles_20260202.csv -> ('customer_profiles', '20260202')
        loans_20260202.csv -> ('loans', '20260202')

    Returns:
        (base_name, date_str) or (None, None) if invalid
    """
    # Remove .csv extension
    name = os.path.splitext(filename)[0]

    # Try to extract date (YYYYMMDD) from the end
    match = re.match(r'^(.+)_(\d{8})$', name)

    if match:
        base_name = match.group(1)
        date_str = match.group(2)
        return base_name, date_str

    # If no date pattern, use the whole name as base
    return name, None


def base_name_to_table_name(base_name):
    """
    Convert base name to PascalCase table name.

    Examples:
        customer_profiles -> CustomerProfiles
        account_products -> AccountProducts
        audit_log -> AuditLog
    """
    # Split by underscore and capitalize each word
    words = base_name.split('_')
    return ''.join(word.capitalize() for word in words)


# ============================================================================
# DATABASE OPERATIONS
# ============================================================================

def get_table_columns(conn, table_name):
    """Get column names from a database table."""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info([{table_name}])")
    columns = [row[1] for row in cursor.fetchall()]
    return columns


def table_exists(conn, table_name):
    """Check if a table exists in the database."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    return cursor.fetchone() is not None


def compare_columns(table_cols, csv_cols):
    """
    Compare table columns with CSV columns (case-insensitive).

    Returns:
        (match: bool, missing_in_csv: list, extra_in_csv: list)
    """
    # Normalize to lowercase for comparison
    table_set = set(col.lower() for col in table_cols)
    csv_set = set(col.lower() for col in csv_cols)

    # Exclude system columns that start with underscore
    table_set = set(col for col in table_set if not col.startswith('_'))

    missing_in_csv = table_set - csv_set
    extra_in_csv = csv_set - table_set

    match = (len(missing_in_csv) == 0 and len(extra_in_csv) == 0)

    return match, list(missing_in_csv), list(extra_in_csv)


# ============================================================================
# CSV READING
# ============================================================================

def read_csv_file(file_path, configs):
    """
    Read CSV file with multiple encoding attempts.

    Returns:
        (DataFrame, encoding_used) or (None, error_message)
    """
    settings = configs['csv'].get('import_settings', {})
    encodings = settings.get('encoding_attempts', ['utf-8', 'latin-1', 'cp1252'])

    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding)

            # Clean column names
            if settings.get('trim_whitespace', True):
                df.columns = df.columns.str.strip()
                df.columns = df.columns.str.replace(r'\s+', ' ', regex=True)

            # Skip empty rows
            if settings.get('skip_empty_rows', True):
                df = df.dropna(how='all')

            return df, encoding

        except (UnicodeDecodeError, UnicodeError):
            continue
        except Exception as e:
            return None, str(e)

    return None, "Could not read file with any encoding"


def get_csv_columns(file_path, configs):
    """Get column names from CSV file without loading all data."""
    settings = configs['csv'].get('import_settings', {})
    encodings = settings.get('encoding_attempts', ['utf-8', 'latin-1', 'cp1252'])

    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding, nrows=0)
            columns = df.columns.str.strip().tolist()
            return columns, None
        except (UnicodeDecodeError, UnicodeError):
            continue
        except Exception as e:
            return None, str(e)

    return None, "Could not read file with any encoding"


# ============================================================================
# IMPORT LOGIC
# ============================================================================

def import_single_csv(conn, file_path, configs, log_entries):
    """
    Import a single CSV file into the database.

    Returns:
        dict with import results
    """
    filename = os.path.basename(file_path)
    result = {
        'filename': filename,
        'success': False,
        'table_name': None,
        'rows_imported': 0,
        'message': ''
    }

    # Parse filename
    base_name, date_str = parse_filename(filename)
    if not base_name:
        result['message'] = "Invalid filename format"
        return result

    # Convert to table name
    table_name = 'raw' + base_name_to_table_name(base_name)
    result['table_name'] = table_name

    print(f"\n{'─' * 70}")
    print(f"File: {filename}")
    print(f"Table: {table_name}")
    print(f"{'─' * 70}")

    # Check if table exists
    exists = table_exists(conn, table_name)

    if exists:
        # Validate columns
        print("  Status: Table exists, validating columns...")

        table_cols = get_table_columns(conn, table_name)
        csv_cols, error = get_csv_columns(file_path, configs)

        if error:
            result['message'] = f"Error reading CSV: {error}"
            print(f"  ERROR: {error}")
            return result

        match, missing, extra = compare_columns(table_cols, csv_cols)

        if not match:
            result['message'] = f"Column mismatch - Missing: {missing}, Extra: {extra}"
            print(f"  ERROR: Column mismatch!")
            if missing:
                print(f"    Missing in CSV: {missing}")
            if extra:
                print(f"    Extra in CSV: {extra}")
            print(f"  SKIPPED - Please fix CSV or update table schema")
            return result

        print(f"  Columns: {len(csv_cols)} columns match")

    else:
        # Create new table
        print("  Status: Table does not exist, creating...")

    # Read full CSV
    df, encoding = read_csv_file(file_path, configs)

    if df is None:
        result['message'] = f"Error reading CSV: {encoding}"
        print(f"  ERROR: {encoding}")
        return result

    print(f"  Encoding: {encoding}")
    print(f"  Rows in CSV: {len(df):,}")

    # Add metadata columns
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['_imported_at'] = now
    df['_source_file'] = filename
    if date_str:
        df['_file_date'] = date_str

    # Import to database
    try:
        if exists:
            df.to_sql(table_name, conn, if_exists='append', index=False)
        else:
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"  Created: {table_name} with {len(df.columns)} columns")

        result['success'] = True
        result['rows_imported'] = len(df)
        result['message'] = "Success"

        # Get total count
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
        total = cursor.fetchone()[0]

        print(f"  Inserted: {len(df):,} rows")
        print(f"  Total in table: {total:,} rows")

        # Log the import
        log_entries.append({
            'operation': 'csv_import',
            'table_name': table_name,
            'rows_affected': len(df),
            'status': 'success',
            'message': f"Imported from {filename}",
            'started_at': now,
            'completed_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    except Exception as e:
        result['message'] = str(e)
        print(f"  ERROR: {str(e)}")

    return result


def process_csv_folder(folder_path, configs, conn):
    """
    Process all CSV files in a folder.

    Returns:
        list of import results
    """
    # Find all CSV files
    csv_pattern = os.path.join(folder_path, '*.csv')
    csv_files = sorted(glob(csv_pattern))

    if not csv_files:
        print(f"\nNo CSV files found in {folder_path}")
        return []

    print(f"\nFound {len(csv_files)} CSV file(s)")

    results = []
    log_entries = []

    for i, file_path in enumerate(csv_files, 1):
        print(f"\n[{i}/{len(csv_files)}] Processing...")
        result = import_single_csv(conn, file_path, configs, log_entries)
        results.append(result)

    # Write log entries to database
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
                pass  # Log table might not exist
        conn.commit()

    return results


def move_processed_files(folder_path, configs):
    """Move processed CSV files to the processed folder."""
    processed_folder = configs['csv'].get('processed_folder', 'data/processed/')
    project_root = get_project_root()
    processed_path = os.path.join(project_root, processed_folder)

    # Create date-based subfolder
    date_folder = datetime.now().strftime("%Y-%m-%d")
    target_path = os.path.join(processed_path, date_folder)

    if not os.path.exists(target_path):
        os.makedirs(target_path)

    # Move files
    csv_files = glob(os.path.join(folder_path, '*.csv'))
    for file_path in csv_files:
        filename = os.path.basename(file_path)
        shutil.move(file_path, os.path.join(target_path, filename))

    return len(csv_files), target_path


# ============================================================================
# MAIN
# ============================================================================

def main(use_sample=False):
    """Main entry point for CSV importer."""

    print("=" * 70)
    print("CSV IMPORTER")
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

    # Get folder path
    project_root = get_project_root()
    if use_sample:
        folder = configs['csv'].get('sample_folder', 'data/sample/')
    else:
        folder = configs['csv'].get('csv_folder', 'data/incoming/')
    folder_path = os.path.join(project_root, folder)

    print(f"\nDatabase: {db_path}")
    print(f"CSV Folder: {folder_path}")

    if not os.path.exists(folder_path):
        print(f"\nERROR: Folder does not exist: {folder_path}")
        return False

    # Connect to database
    conn = sqlite3.connect(db_path)

    # Process CSV files
    results = process_csv_folder(folder_path, configs, conn)

    # Summary
    print("\n" + "=" * 70)
    print("IMPORT SUMMARY")
    print("=" * 70)

    success_count = sum(1 for r in results if r['success'])
    failed_count = len(results) - success_count
    total_rows = sum(r['rows_imported'] for r in results)

    print(f"\nFiles processed: {len(results)}")
    print(f"Successful: {success_count}")
    print(f"Failed: {failed_count}")
    print(f"Total rows imported: {total_rows:,}")

    if failed_count > 0:
        print("\nFailed files:")
        for r in results:
            if not r['success']:
                print(f"  - {r['filename']}: {r['message']}")

    conn.close()

    # Move processed files (only from incoming, not sample)
    if not use_sample and success_count > 0:
        print("\n" + "-" * 70)
        move = input("Move processed files to archive? (yes/no): ").strip().lower()
        if move == 'yes':
            count, path = move_processed_files(folder_path, configs)
            print(f"Moved {count} files to {path}")

    print("\n" + "=" * 70)
    print("IMPORT COMPLETE")
    print("=" * 70)
    print("\nNext step: python scripts/raw_to_stg.py")

    return failed_count == 0


if __name__ == "__main__":
    import sys

    use_sample = '--sample' in sys.argv

    if use_sample:
        print("\n*** SAMPLE MODE - Using data/sample/ folder ***\n")

    main(use_sample=use_sample)
