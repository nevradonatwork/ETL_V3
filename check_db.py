import sqlite3
import os

db_path = os.path.join(os.path.dirname(__file__), 'database', 'banking.db')
print(f"Database: {db_path}")
print(f"Exists: {os.path.exists(db_path)}")

if not os.path.exists(db_path):
    print("Database not found!")
    exit()

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = cursor.fetchall()

print(f"\nTotal tables: {len(tables)}\n")
print(f"{'Table Name':<40} {'Rows':>10}")
print("-" * 52)

for t in tables:
    name = t[0]
    cursor.execute(f"SELECT COUNT(*) FROM [{name}]")
    count = cursor.fetchone()[0]
    print(f"{name:<40} {count:>10}")

conn.close()
