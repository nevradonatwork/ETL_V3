"""
ETL Pipeline Runner
===================
Runs the full ETL pipeline in order:
  1. csv_importer.py  — Import raw CSV data
  2. raw_to_stg.py    — Transform raw to staging
  3. stg_to_rpt.py    — Create report tables

Usage:
    python scripts/run_pipeline.py              # Process incoming folder
    python scripts/run_pipeline.py --sample     # Process sample folder (demo)

Author: Nevra Donat
"""

import subprocess
import sys
import os


def get_project_root():
    """Get the project root directory."""
    return os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))


def run_step(step_number, description, script, args=None):
    """Run a single pipeline step and return True if successful."""
    print(f"\n{'=' * 70}")
    print(f"STEP {step_number}: {description}")
    print(f"{'=' * 70}\n")

    cmd = [sys.executable, script]
    if args:
        cmd.extend(args)

    result = subprocess.run(cmd, cwd=get_project_root())

    if result.returncode != 0:
        print(f"\n>>> STEP {step_number} FAILED (exit code {result.returncode})")
        return False

    print(f"\n>>> STEP {step_number} COMPLETED")
    return True


def main():
    root = get_project_root()

    # Pass --sample flag if provided
    importer_args = ['--sample'] if '--sample' in sys.argv else []

    steps = [
        (1, "Import CSV Data", os.path.join(root, 'scripts', 'csv_importer.py'), importer_args),
        (2, "Raw to Staging",  os.path.join(root, 'scripts', 'raw_to_stg.py'),   None),
        (3, "Staging to Report", os.path.join(root, 'scripts', 'stg_to_rpt.py'), None),
    ]

    print("=" * 70)
    print("ETL PIPELINE")
    print("Banking ETL System")
    print("=" * 70)

    for step_number, description, script, args in steps:
        if not run_step(step_number, description, script, args):
            print(f"\n{'=' * 70}")
            print(f"PIPELINE STOPPED at Step {step_number}")
            print(f"{'=' * 70}")
            sys.exit(1)

    print(f"\n{'=' * 70}")
    print("PIPELINE COMPLETE - All 3 steps finished successfully")
    print(f"{'=' * 70}")
    print("\nDashboard ready! Run: streamlit run dashboard/app.py")


if __name__ == "__main__":
    main()
