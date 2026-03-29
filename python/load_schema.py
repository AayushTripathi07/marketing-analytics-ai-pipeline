"""
load_schema.py
─────────────────────────────────────────────────────────────────────────────
Marketing Analytics Pipeline — Task 3: Schema Loader
─────────────────────────────────────────────────────────────────────────────
Creates (or recreates) the star schema database at:
    data/final/analytics_warehouse.db

Attaches both cleaned source databases, executes schema.sql, then verifies
the row counts of every table and view created.

Usage:
    cd python && python3 load_schema.py
    (or run from repository root: python3 python/load_schema.py)
"""

import sqlite3
import os
import sys

# ─── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR     = os.path.dirname(BASE_DIR)
DATA_DIR     = os.path.join(ROOT_DIR, 'data', 'final')
SQL_DIR      = os.path.join(ROOT_DIR, 'sql')

DB_CAMPAIGNS = os.path.join(DATA_DIR, 'cleaned_campaigns.db')
DB_SHOPIFY   = os.path.join(DATA_DIR, 'cleaned_shopify.db')
DB_WAREHOUSE = os.path.join(DATA_DIR, 'analytics_warehouse.db')
SCHEMA_SQL   = os.path.join(SQL_DIR,  'schema.sql')

# ─── Sanity checks ─────────────────────────────────────────────────────────────
for path, label in [(DB_CAMPAIGNS, 'cleaned_campaigns.db'),
                    (DB_SHOPIFY,   'cleaned_shopify.db'),
                    (SCHEMA_SQL,   'schema.sql')]:
    if not os.path.exists(path):
        print(f"❌ Missing: {label} at {path}")
        print("   Run python/clean_data.py first.")
        sys.exit(1)

# Remove stale warehouse so schema runs clean
if os.path.exists(DB_WAREHOUSE):
    os.remove(DB_WAREHOUSE)
    print("🗑  Removed stale analytics_warehouse.db")

# ─── Connect & attach ──────────────────────────────────────────────────────────
conn = sqlite3.connect(DB_WAREHOUSE)
conn.execute("PRAGMA journal_mode = WAL")
conn.execute("PRAGMA foreign_keys = ON")

# Attach cleaned source databases under alias names used in schema.sql
conn.execute(f"ATTACH DATABASE '{DB_CAMPAIGNS}' AS src_camp")
conn.execute(f"ATTACH DATABASE '{DB_SHOPIFY}'   AS src_shop")

# Copy source tables into main warehouse DB (SQLite can't cross-reference
# attached DBs in views, so we materialise them here first)
conn.execute("""
    CREATE TABLE IF NOT EXISTS campaigns_clean AS
    SELECT * FROM src_camp.campaigns_clean
""")
conn.execute("""
    CREATE TABLE IF NOT EXISTS shopify_clean AS
    SELECT * FROM src_shop.shopify_clean
""")
conn.commit()
print("✅ Source databases attached and tables copied")


# ─── Execute schema.sql ────────────────────────────────────────────────────────
with open(SCHEMA_SQL, 'r') as f:
    sql = f.read()

# executescript auto-commits; use it for DDL
try:
    conn.executescript(sql)
    print("✅ schema.sql executed successfully")
except sqlite3.OperationalError as e:
    print(f"❌ Schema execution failed: {e}")
    conn.close()
    sys.exit(1)

# ─── Verification ──────────────────────────────────────────────────────────────
print()
print("─── Row counts ─────────────────────────────────────────────────────────")

checks = [
    ("dim_date",                      "SELECT COUNT(*) FROM dim_date"),
    ("dim_campaign",                  "SELECT COUNT(*) FROM dim_campaign"),
    ("dim_channel",                   "SELECT COUNT(*) FROM dim_channel"),
    ("fact_campaign_performance",     "SELECT COUNT(*) FROM fact_campaign_performance"),
    ("fact_sales",                    "SELECT COUNT(*) FROM fact_sales"),
    ("vw_powerbi_monthly_performance","SELECT COUNT(*) FROM vw_powerbi_monthly_performance"),
    ("vw_powerbi_campaign_performance","SELECT COUNT(*) FROM vw_powerbi_campaign_performance"),
    ("vw_powerbi_region_performance", "SELECT COUNT(*) FROM vw_powerbi_region_performance"),
    ("vw_powerbi_channel_performance","SELECT COUNT(*) FROM vw_powerbi_channel_performance"),
    ("vw_ai_flexible_performance",    "SELECT COUNT(*) FROM vw_ai_flexible_performance"),
    ("vw_ai_kpi_summary",             "SELECT COUNT(*) FROM vw_ai_kpi_summary"),
]

all_ok = True
for label, query in checks:
    try:
        count = conn.execute(query).fetchone()[0]
        status = "✅" if count > 0 else "⚠️ "
        if count == 0:
            all_ok = False
        print(f"  {status}  {label:<40} {count:>8,} rows")
    except sqlite3.OperationalError as e:
        print(f"  ❌  {label:<40} ERROR: {e}")
        all_ok = False

# ─── Null checks on fact tables ───────────────────────────────────────────────
print()
print("─── Null checks on fact tables ─────────────────────────────────────────")
for table, col in [
    ("fact_campaign_performance", "date_id"),
    ("fact_campaign_performance", "campaign_id"),
    ("fact_campaign_performance", "spend_inr"),
    ("fact_sales",                "date_id"),
    ("fact_sales",                "channel_id"),
]:
    nulls = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
    ).fetchone()[0]
    icon = "✅" if nulls == 0 else "❌"
    print(f"  {icon}  {table}.{col} nulls: {nulls}")

conn.close()

print()
if all_ok:
    print(f"✅ analytics_warehouse.db ready at:")
    print(f"   {DB_WAREHOUSE}")
else:
    print("⚠️  Some checks failed — review output above.")
